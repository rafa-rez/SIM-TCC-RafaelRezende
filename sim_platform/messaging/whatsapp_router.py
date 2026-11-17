"""
Roteador Multi-Cidade para WhatsApp / WAHA via comandos `/nomedacidade` (`WhatsAppTenantRouter`).

Regras de Negócio (SIM v4.5):
1. Por padrão, cidadãos que enviam mensagens sem vínculo prévio utilizam a cidade padrão ativa no Console (`default_tenant_id`).
2. Comandos `/nomedacidade` (case-insensitive, insensíveis a acentos e hífens — ex.: `/lagoa`, `/lagoa_da_prata`, `/lagoadaprata`,
   `/lagoa-da-prata`, `/caete`, `/caeté`, `/3137205`, `/3110004`, `/cidade lagoa da prata`, `/padrao`):
   - Resolvem dinamicamente contra todos os municípios configurados em `tenants/*.json` (`tenant_id`, `ibge_code`, slug de
     `municipality_name` e primeiro token único como `lagoa` -> `lagoa_da_prata_mg_3137205`).
   - **Modo inline com pergunta** (ex.: `"/lagoa qual a receita arrecadada em 2026?"`): redireciona imediatamente para o `tenant_id`
     resolvido, registra o vínculo do `chat_id` no SQLite e retorna `(resolved_tenant_id, stripped_question, None)`.
   - **Modo comando puro** (ex.: `"/lagoa"`, `"/caete"` ou `"/padrao"`): grava (ou limpa em `/padrao`) o vínculo permanente
     do `chat_id` em `avaliacao/dados/governance/whatsapp_bindings.sqlite` e retorna `command_only_reply` amigável confirmando a mudança.
   - Compatível com o prefixo de auto-teste `/sim` (`fromMe=True`), tratando `/sim /lagoa qual a receita?` sem considerar `/sim` como cidade.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Tuple

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BINDINGS_DB_PATH = WORKSPACE_ROOT / "avaliacao" / "dados" / "governance" / "whatsapp_bindings.sqlite"

_STOPWORDS_PT = {"da", "de", "do", "das", "dos", "e"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _strip_accents_lower(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").strip().lower())
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _to_slug(text: str) -> str:
    clean = _strip_accents_lower(text)
    return re.sub(r"[^a-z0-9]+", "_", clean).strip("_")


def _to_compact(text: str) -> str:
    clean = _strip_accents_lower(text)
    return re.sub(r"[^a-z0-9]+", "", clean)


class WhatsAppRouteResult(NamedTuple):
    """
    Resultado compatível simultaneamente com desempacotamento de tupla de 3 elementos
    `(resolved_tenant_id, stripped_question, command_only_reply)` e acesso por atributos nomeados.
    """

    resolved_tenant_id: str
    stripped_question: str
    command_only_reply: Optional[str] = None

    @property
    def tenant_id(self) -> str:
        return self.resolved_tenant_id

    @property
    def question(self) -> str:
        return self.stripped_question

    @property
    def is_command_only(self) -> bool:
        return self.command_only_reply is not None and not self.stripped_question

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resolved_tenant_id": self.resolved_tenant_id,
            "stripped_question": self.stripped_question,
            "command_only_reply": self.command_only_reply,
            "is_command_only": self.is_command_only,
        }


class WhatsAppTenantRouter:
    """
    Roteador determinístico e persistente (SQLite WAL) de municípios no WhatsApp por `chat_id` e comando `/nomedacidade`.
    """

    def __init__(
        self,
        workspace_root: Optional[Path] = None,
        db_path: Optional[Path] = None,
        tenants_dir: Optional[Path] = None,
    ) -> None:
        self.workspace_root = Path(workspace_root).resolve() if workspace_root is not None else WORKSPACE_ROOT
        self.tenants_dir = (
            Path(tenants_dir).resolve()
            if tenants_dir is not None
            else (self.workspace_root / "tenants").resolve()
        )
        if db_path is not None:
            self.db_path = Path(db_path).resolve()
        elif workspace_root is not None:
            self.db_path = (self.workspace_root / "avaliacao" / "dados" / "governance" / "whatsapp_bindings.sqlite").resolve()
        else:
            self.db_path = DEFAULT_BINDINGS_DB_PATH

        self._lock = threading.RLock()
        self._cached_sig: Optional[Tuple[Tuple[str, int], ...]] = None
        self._tenants_by_id: Dict[str, Dict[str, str]] = {}
        self._alias_to_tid: Dict[str, str] = {}
        self._multiword_phrases: List[Tuple[str, str]] = []

        self._init_sqlite()
        self._refresh_tenants_index()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(str(self.db_path), timeout=5.0)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            with con:
                yield con
        finally:
            con.close()

    def _init_sqlite(self) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS whatsapp_chat_bindings (
                    chat_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    source_command TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def _compute_tenants_dir_signature(self) -> Tuple[Tuple[str, int], ...]:
        if not self.tenants_dir.exists():
            return ()
        items: List[Tuple[str, int]] = []
        for fp in sorted(self.tenants_dir.glob("*.json")):
            if fp.name.startswith("_"):
                continue
            try:
                items.append((fp.name, int(fp.stat().st_mtime_ns)))
            except Exception:
                items.append((fp.name, 0))
        return tuple(items)

    def _refresh_tenants_index(self) -> None:
        sig = self._compute_tenants_dir_signature()
        with self._lock:
            if self._cached_sig is not None and sig == self._cached_sig and self._tenants_by_id:
                return

            tenants_by_id: Dict[str, Dict[str, str]] = {}
            if self.tenants_dir.exists():
                for fp in sorted(self.tenants_dir.glob("*.json")):
                    if fp.name.startswith("_"):
                        continue
                    try:
                        data = json.loads(fp.read_text(encoding="utf-8"))
                    except Exception:
                        continue
                    if not isinstance(data, dict):
                        continue
                    tid = str(data.get("tenant_id") or fp.stem).strip()
                    if not tid:
                        continue
                    m_name = str(data.get("municipality_name") or tid).strip()
                    uf = str(data.get("state_uf") or "MG").strip().upper()
                    ibge = str(data.get("ibge_code") or "").strip()
                    city_slug = _to_slug(m_name)
                    sig_toks = [t for t in city_slug.split("_") if t and t not in _STOPWORDS_PT]
                    if len(sig_toks) >= 2 and sig_toks[0] in ("belo", "governador", "sao", "santa", "santo", "ouro", "pouso", "pocos"):
                        prim_cmd = f"/{''.join(t[0] for t in sig_toks)}" if len(sig_toks) == 2 and sig_toks[0] in ("belo", "governador") else f"/{city_slug}"
                    else:
                        prim_cmd = f"/{city_slug.split('_')[0]}" if city_slug else f"/{tid}"
                    tenants_by_id[tid] = {
                        "tenant_id": tid,
                        "municipality_name": m_name,
                        "state_uf": uf,
                        "ibge_code": ibge,
                        "city_slug": city_slug,
                        "primary_command": prim_cmd,
                    }

            if "caete_mg_3110004" not in tenants_by_id and not tenants_by_id:
                tenants_by_id["caete_mg_3110004"] = {
                    "tenant_id": "caete_mg_3110004",
                    "municipality_name": "Caeté",
                    "state_uf": "MG",
                    "ibge_code": "3110004",
                    "city_slug": "caete",
                    "primary_command": "/caete",
                }

            alias_map: Dict[str, str] = {}
            first_token_candidates: Dict[str, List[str]] = {}
            nostop_slug_candidates: Dict[str, List[str]] = {}
            multiword: List[Tuple[str, str]] = []

            for tid, meta in tenants_by_id.items():
                city_slug = meta["city_slug"]
                city_compact = _to_compact(meta["municipality_name"])
                uf_low = meta["state_uf"].lower()
                ibge = meta["ibge_code"]
                tid_slug = _to_slug(tid)
                tid_compact = _to_compact(tid)

                exact_keys = {
                    tid.lower(),
                    tid_slug,
                    tid_compact,
                    city_slug,
                    city_compact,
                    f"{city_slug}_{uf_low}",
                    f"{city_compact}{uf_low}",
                }
                if ibge:
                    exact_keys.add(ibge.lower())

                for k in exact_keys:
                    if k:
                        alias_map[k] = tid

                tokens = [t for t in city_slug.split("_") if t]
                if tokens:
                    if len(tokens[0]) >= 3:
                        first_token_candidates.setdefault(tokens[0], []).append(tid)
                    if len(tokens) >= 2 and len(tokens[-1]) >= 4 and tokens[-1] != tokens[0]:
                        first_token_candidates.setdefault(tokens[-1], []).append(tid)
                    sig_tokens = [t for t in tokens if t not in _STOPWORDS_PT]
                    if len(sig_tokens) >= 2:
                        ns_slug = "_".join(sig_tokens)
                        ns_compact = "".join(sig_tokens)
                        initials = "".join(t[0] for t in sig_tokens if t)
                        nostop_slug_candidates.setdefault(ns_slug, []).append(tid)
                        nostop_slug_candidates.setdefault(ns_compact, []).append(tid)
                        if len(initials) >= 2:
                            nostop_slug_candidates.setdefault(initials, []).append(tid)

                space_name = city_slug.replace("_", " ")
                if space_name:
                    multiword.append((f"{space_name} {uf_low}", tid))
                    multiword.append((space_name, tid))
                multiword.append((tid_slug.replace("_", " "), tid))
                if ibge:
                    multiword.append((ibge.lower(), tid))

            # Registra primeiro token único (ex.: 'lagoa' -> 'lagoa_da_prata_mg_3137205') sem colisão ambígua
            for tok, tids in first_token_candidates.items():
                unique_tids = list(dict.fromkeys(tids))
                if len(unique_tids) == 1 and tok not in alias_map:
                    alias_map[tok] = unique_tids[0]
                    multiword.append((tok, unique_tids[0]))
                elif len(unique_tids) > 1:
                    # Se houver colisão de 1º token, ajusta primary_command dos conflitantes para city_slug completo
                    for c_tid in unique_tids:
                        tenants_by_id[c_tid]["primary_command"] = f"/{tenants_by_id[c_tid]['city_slug']}"

            for ns_key, tids in nostop_slug_candidates.items():
                unique_tids = list(dict.fromkeys(tids))
                if len(unique_tids) == 1 and ns_key not in alias_map:
                    alias_map[ns_key] = unique_tids[0]

            # Ordena frases multi-palavra da mais longa para a mais curta para `/cidade lagoa da prata <pergunta>`
            dedup_mw: Dict[str, str] = {}
            for phrase, tid in sorted(multiword, key=lambda kv: len(kv[0]), reverse=True):
                if phrase and phrase not in dedup_mw:
                    dedup_mw[phrase] = tid

            self._tenants_by_id = tenants_by_id
            self._alias_to_tid = alias_map
            self._multiword_phrases = list(dedup_mw.items())
            self._cached_sig = sig

    def list_available_tenants(self) -> List[Dict[str, str]]:
        self._refresh_tenants_index()
        with self._lock:
            return [dict(v) for v in self._tenants_by_id.values()]

    def resolve_tenant_alias(self, alias: str) -> Optional[str]:
        """
        Resolve um identificador, código IBGE, nome de cidade ou comando (ex.: `'lagoa'`, `'/caeté'`, `'lagoa-da-prata'`, `'3137205'`)
        para o `tenant_id` oficial correspondente, ou `None` se não encontrado.
        """
        self._refresh_tenants_index()
        raw = (alias or "").strip().lstrip("/").strip()
        if not raw:
            return None
        slug = _to_slug(raw)
        compact = _to_compact(raw)
        if slug in ("sim", "padrao", "default", "reset", "cidade", "municipio", "cidades", "municipios"):
            return None
        with self._lock:
            if raw.lower() in self._alias_to_tid:
                return self._alias_to_tid[raw.lower()]
            if slug in self._alias_to_tid:
                return self._alias_to_tid[slug]
            if compact in self._alias_to_tid:
                return self._alias_to_tid[compact]
        return None

    def get_binding(self, chat_id: str) -> Optional[str]:
        cid = (chat_id or "").strip()
        if not cid:
            return None
        self._refresh_tenants_index()
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT tenant_id FROM whatsapp_chat_bindings WHERE chat_id = ?",
                (cid,),
            ).fetchone()
            if not row:
                return None
            tid = str(row["tenant_id"] or "").strip()
            if tid and (not self._tenants_by_id or tid in self._tenants_by_id):
                return tid
            return None

    def set_binding(self, chat_id: str, tenant_id: str, source_command: str = "/cidade") -> None:
        cid = (chat_id or "").strip()
        tid = (tenant_id or "").strip()
        if not cid or not tid:
            return
        with self._lock, self._connect() as con:
            con.execute(
                """
                INSERT INTO whatsapp_chat_bindings (chat_id, tenant_id, source_command, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    source_command = excluded.source_command,
                    updated_at = excluded.updated_at
                """,
                (cid, tid, source_command, _now_iso()),
            )

    def clear_binding(self, chat_id: str) -> bool:
        cid = (chat_id or "").strip()
        if not cid:
            return False
        with self._lock, self._connect() as con:
            cur = con.execute("DELETE FROM whatsapp_chat_bindings WHERE chat_id = ?", (cid,))
            return int(cur.rowcount or 0) > 0

    def list_bindings(self) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            rows = con.execute(
                "SELECT chat_id, tenant_id, source_command, updated_at FROM whatsapp_chat_bindings ORDER BY updated_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _strip_leading_sim_and_punct(text: str) -> str:
        out = (text or "").lstrip(" :-–—,").strip()
        while re.match(r"^/sim(?:\s+|$)", out, flags=re.IGNORECASE):
            out = re.sub(r"^/sim(?:\s+|$)", "", out, count=1, flags=re.IGNORECASE).lstrip(" :-–—,").strip()
        return out

    def _format_available_commands_summary(self) -> str:
        parts: List[str] = []
        for meta in self._tenants_by_id.values():
            ibge_str = f", IBGE {meta['ibge_code']}" if meta.get("ibge_code") else ""
            parts.append(f"• *{meta['municipality_name']}/{meta['state_uf']}*: `{meta['primary_command']}`{ibge_str}")
        parts.append("• *Seguir cidade padrão do Console*: `/padrao`")
        return "\n".join(parts)

    def _build_switch_reply(self, tenant_id: str) -> str:
        meta = self._tenants_by_id.get(tenant_id) or {
            "municipality_name": tenant_id,
            "state_uf": "MG",
            "ibge_code": "",
            "primary_command": f"/{_to_slug(tenant_id)}",
        }
        ibge_part = f" (IBGE `{meta['ibge_code']}`)" if meta.get("ibge_code") else ""
        return (
            f"*Município selecionado para este chat:* *{meta['municipality_name']}/{meta['state_uf']}*{ibge_part}.\n\n"
            f"Suas próximas perguntas neste WhatsApp consultarão exclusivamente as bases oficiais de "
            f"*{meta['municipality_name']}/{meta['state_uf']}*. "
            f"Para voltar a seguir automaticamente a cidade padrão do Console, envie `/padrao`."
        )

    def _build_reset_reply(self, default_tenant_id: str) -> str:
        meta = self._tenants_by_id.get(default_tenant_id) or {
            "municipality_name": default_tenant_id,
            "state_uf": "MG",
            "ibge_code": "",
        }
        ibge_part = f" (IBGE `{meta['ibge_code']}`)" if meta.get("ibge_code") else ""
        return (
            f"*Vínculo fixo removido.* Seu chat voltou a seguir automaticamente o município padrão selecionado "
            f"na plataforma: *{meta['municipality_name']}/{meta['state_uf']}*{ibge_part}.\n\n"
            f"Cidades disponíveis por comando:\n{self._format_available_commands_summary()}"
        )

    def _build_unknown_city_reply(self, attempted_cmd: str, active_tid: str) -> str:
        cur_meta = self._tenants_by_id.get(active_tid) or {"municipality_name": active_tid, "state_uf": "MG"}
        clean_cmd = attempted_cmd.strip().lstrip("/")
        return (
            f"Não encontrei o município `/{clean_cmd}` configurado nesta instância do SIM "
            f"(cidade ativa atual para sua conversa: *{cur_meta['municipality_name']}/{cur_meta['state_uf']}*).\n\n"
            f"Comandos de municípios disponíveis:\n{self._format_available_commands_summary()}"
        )

    def _match_cidade_subcommand(self, rest_text: str) -> Tuple[Optional[str], str]:
        """
        Extrai `(tenant_id, pergunta_restante)` de `/cidade <nome multi-palavra> [pergunta]`.
        """
        raw_words = (rest_text or "").strip().split()
        if not raw_words:
            return None, ""

        # Tenta casar do prefixo de 5 palavras até 1 palavra contra os aliases configurados
        max_k = min(5, len(raw_words))
        for k in range(max_k, 0, -1):
            candidate_phrase = " ".join(raw_words[:k])
            tid = self.resolve_tenant_alias(candidate_phrase)
            if tid:
                # Preserva o texto original após as `k` palavras consumidas
                pattern = r"^\s*" + r"\s+".join(re.escape(w) for w in raw_words[:k]) + r"(?:\s+(.*))?$"
                m = re.match(pattern, rest_text.strip(), flags=re.DOTALL)
                remainder = (m.group(1) if m and m.group(1) else "").strip()
                return tid, remainder

        # Fallback: busca prefixo normalizado contra _multiword_phrases
        norm_full = _to_slug(rest_text).replace("_", " ")
        for phrase, tid in self._multiword_phrases:
            if norm_full == phrase:
                return tid, ""
            if norm_full.startswith(phrase + " "):
                k_words = len(phrase.split())
                remainder = " ".join(raw_words[k_words:]).strip()
                return tid, remainder

        return None, rest_text.strip()

    def route_message(
        self,
        chat_id: str,
        message_text: str,
        default_tenant_id: str = "caete_mg_3110004",
        *,
        persist_inline: bool = True,
    ) -> WhatsAppRouteResult:
        """
        Resolve o município de destino e higieniza o comando `/nomedacidade` da mensagem do WhatsApp.
        Retorna `WhatsAppRouteResult(resolved_tenant_id, stripped_question, command_only_reply)`.
        """
        self._refresh_tenants_index()
        def_tid = (default_tenant_id or "caete_mg_3110004").strip()
        bound_tid = self.get_binding(chat_id)
        effective_tid = bound_tid or def_tid

        work = (message_text or "").strip()
        if not work:
            return WhatsAppRouteResult(effective_tid, "", None)

        # Ignora prefixo de auto-teste `/sim` (`fromMe=True`) sem jamais tratá-lo como nome de cidade
        while re.match(r"^/sim(?:\s+|$)", work, flags=re.IGNORECASE):
            work = re.sub(r"^/sim(?:\s+|$)", "", work, count=1, flags=re.IGNORECASE).strip()

        if not work:
            return WhatsAppRouteResult(effective_tid, "", None)

        # Detecta comando `/token` inicial (ignorando paths de sistema com múltiplos '/', como '/etc/passwd')
        cmd_match = re.match(r"^/([^\s/]+)(?:\s+(.*))?$", work, flags=re.DOTALL)
        if cmd_match:
            raw_token = cmd_match.group(1).strip()
            raw_rest = (cmd_match.group(2) or "").strip()
            cmd_slug = _to_slug(raw_token)

            # 1. Comando `/padrao` (volta a seguir a cidade padrão do Console)
            if cmd_slug in ("padrao", "default", "reset", "console", "desvincular"):
                self.clear_binding(chat_id)
                clean_q = self._strip_leading_sim_and_punct(raw_rest)
                if clean_q:
                    return WhatsAppRouteResult(def_tid, clean_q, None)
                return WhatsAppRouteResult(def_tid, "", self._build_reset_reply(def_tid))

            # 2. Comandos `/cidades`, `/ajuda`, `/menu`, `/oi`, `/start` (menu institucional e lista de cidades disponíveis)
            if cmd_slug in ("cidades", "municipios", "ajuda_cidades", "listar_cidades", "ajuda", "menu", "help", "comandos", "oi", "ola", "start", "inicio"):
                cur_meta = self._tenants_by_id.get(effective_tid) or {"municipality_name": effective_tid, "state_uf": "MG"}
                reply = (
                    f"*SIM — Sistema de Inteligência Municipal*\n"
                    f"*Município ativo nesta conversa:* *{cur_meta['municipality_name']}/{cur_meta['state_uf']}*.\n\n"
                    f"Envie sua pergunta em texto ou áudio sobre orçamento, receitas, despesas, folha, contratos, frota, saúde, educação ou Diário Oficial.\n\n"
                    f"Comandos para alternar de município:\n{self._format_available_commands_summary()}"
                )
                return WhatsAppRouteResult(effective_tid, "", reply)

            # 3. Comando `/cidade <nome da cidade> [pergunta]`
            if cmd_slug in ("cidade", "municipio"):
                if not raw_rest:
                    cur_meta = self._tenants_by_id.get(effective_tid) or {"municipality_name": effective_tid, "state_uf": "MG"}
                    reply = (
                        f"*Município atual desta conversa:* *{cur_meta['municipality_name']}/{cur_meta['state_uf']}*.\n\n"
                        f"Para trocar de cidade, envie o comando correspondente:\n{self._format_available_commands_summary()}"
                    )
                    return WhatsAppRouteResult(effective_tid, "", reply)

                first_rest_slug = _to_slug(raw_rest.split()[0])
                if first_rest_slug in ("padrao", "default", "reset", "console"):
                    self.clear_binding(chat_id)
                    after_padrao = re.sub(r"^\S+\s*", "", raw_rest, count=1).strip()
                    clean_q = self._strip_leading_sim_and_punct(after_padrao)
                    if clean_q:
                        return WhatsAppRouteResult(def_tid, clean_q, None)
                    return WhatsAppRouteResult(def_tid, "", self._build_reset_reply(def_tid))

                matched_tid, remainder = self._match_cidade_subcommand(raw_rest)
                if matched_tid:
                    clean_q = self._strip_leading_sim_and_punct(remainder)
                    if clean_q:
                        if persist_inline:
                            self.set_binding(chat_id, matched_tid, source_command="/cidade")
                        return WhatsAppRouteResult(matched_tid, clean_q, None)
                    self.set_binding(chat_id, matched_tid, source_command="/cidade")
                    return WhatsAppRouteResult(matched_tid, "", self._build_switch_reply(matched_tid))

                return WhatsAppRouteResult(
                    effective_tid,
                    "",
                    self._build_unknown_city_reply(raw_rest.split()[0], effective_tid),
                )

            # 4. Comando direto `/<nomedacidade_ou_ibge> [pergunta]`
            matched_tid = self.resolve_tenant_alias(raw_token)
            if matched_tid:
                clean_q = self._strip_leading_sim_and_punct(raw_rest)
                if clean_q:
                    if persist_inline:
                        self.set_binding(chat_id, matched_tid, source_command=f"/{cmd_slug}")
                    return WhatsAppRouteResult(matched_tid, clean_q, None)
                self.set_binding(chat_id, matched_tid, source_command=f"/{cmd_slug}")
                return WhatsAppRouteResult(matched_tid, "", self._build_switch_reply(matched_tid))

            return WhatsAppRouteResult(
                effective_tid,
                "",
                self._build_unknown_city_reply(raw_token, effective_tid),
            )

        return WhatsAppRouteResult(effective_tid, work, None)

    def resolve(
        self,
        chat_id: str,
        message_text: str,
        default_tenant_id: str = "caete_mg_3110004",
        *,
        persist_inline: bool = True,
    ) -> WhatsAppRouteResult:
        """Alias para `route_message(...)`."""
        return self.route_message(
            chat_id=chat_id,
            message_text=message_text,
            default_tenant_id=default_tenant_id,
            persist_inline=persist_inline,
        )
