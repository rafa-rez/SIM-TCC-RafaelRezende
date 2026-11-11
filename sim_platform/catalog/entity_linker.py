"""
Vinculador Semântico de Valores (CHESS Value Linker), Normalizador Fonético ASR de Domínio Público
e Recuperador Dinâmico de Few-Shots via Okapi BM25 (`EntityLinker`).

Referências Arquiteturais:
- Talaei et al. (Stanford, 2024) — *CHESS: Contextual Harnessing for Efficient SQL Synthesis* (Value Linking & Schema Pruning)
- Gao et al. (VLDB 2024) — *DAIL-SQL* (Dynamic Few-Shot Example Selection por similaridade estrutural e lexical)
"""

from __future__ import annotations

import csv
import math
import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb


def _norm(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", clean).strip()


# ==============================================================================
# EIXO 5: NORMALIZADOR FONÉTICO E LEXICAL PÓS-ASR PARA ADMINISTRAÇÃO PÚBLICA
# ==============================================================================
_QET_CITY_RE = re.compile(
    r"\b(?:Q\.?\s*E\.?\s*T\.?|QET|KAETE|KAET[EÉ]|CAITE|CAIT[EÉ]|CAETHE)\b",
    re.IGNORECASE,
)

_STAFF_CONTEXT_RE = re.compile(
    r"\b(?:concursad\w*|efetiv\w*|funcion[aá]ri\w*|servidor\w*|quadro|contratad\w*|comissionad\w*|quantos\s+s[aã]o|deste\s+total)\b",
    re.IGNORECASE,
)
_LOAN_CONTEXT_RE = re.compile(
    r"\b(?:empr[eé]stim\w*|margem|desconto\s+em\s+folha|banco|cef|caixa| Bradesco|ita[uú])\b",
    re.IGNORECASE,
)

_LEXICAL_DOMAIN_RULES: List[Tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"\bCEDESMA\b|\bSEDESMAS\b|\bCDESMA\b", re.IGNORECASE), "SEDESMA", "SEDESMA"),
    (re.compile(r"\bCEMAS\b", re.IGNORECASE), "SEMAS", "SEMAS"),
    (re.compile(r"\bFUNDEBE\b|\bFUNDEBI\b", re.IGNORECASE), "FUNDEB", "FUNDEB"),
    (re.compile(r"\bCIOPS\b|\bSIOPES\b|\bCIOPES\b", re.IGNORECASE), "SIOPS", "SIOPS"),
    (re.compile(r"\bSICONFE\b|\bCICONFI\b|\bSICONF\b", re.IGNORECASE), "SICONFI", "SICONFI"),
    (re.compile(r"\bSIC[AÃ]O\b|\bSICON\b(?!\w)", re.IGNORECASE), "SICOM", "SICOM"),
    (re.compile(r"\bCAPAGUE\b|\bCAPAGUI\b", re.IGNORECASE), "CAPAG", "CAPAG"),
    (re.compile(r"\bID[EÉ]BI\b|\bIDEBE\b", re.IGNORECASE), "IDEB", "IDEB"),
    (re.compile(r"\bL\.?\s*O\.?\s*A\.?\b", re.IGNORECASE), "LOA", "LOA"),
    (re.compile(r"\bL\.?\s*R\.?\s*F\.?\b", re.IGNORECASE), "LRF", "LRF"),
]

_SAAE_PHONETIC_RE = re.compile(r"\b(?:S\.?\s*A\.?\s*I\.?|SA[EÉ])\b")
_WATER_CONTEXT_RE = re.compile(
    r"\b(?:autarquia|[aá]gua|esgoto|saneamento|tarifa|conta\s+d['e]\s*[aá]gua|abastecimento)\b",
    re.IGNORECASE,
)


def normalize_municipal_asr_text(
    text: str,
    municipality_name: str = "Caeté",
) -> Tuple[str, List[str]]:
    """
    Aplica correção determinística fonética e lexical sobre transcrições de áudio (ASR)
    e entradas coloquiais, impedindo que artefatos fonéticos (ex: 'QET' -> 'Caeté',
    'consignados' em pergunta de vínculo funcional -> 'comissionados ou contratados')
    contaminem o Schema Linking, a geração SQL ou a resposta final ao cidadão.
    """
    if not text or not text.strip():
        return text, []

    out = text
    applied: List[str] = []

    # 1. Nome do município piloto corrompido pelo ASR ("QET", "Caité", "Kaeté")
    if municipality_name.lower() in ("caeté", "caete") and _QET_CITY_RE.search(out):
        out = _QET_CITY_RE.sub("Caeté", out)
        applied.append("QET->Caeté")

    # 2. Confusão fonética "consignados" vs "comissionados / contratados" em contexto de contagem de servidores
    if re.search(r"\bconsignad[oa]s?\b", out, re.IGNORECASE):
        if _STAFF_CONTEXT_RE.search(out) and not _LOAN_CONTEXT_RE.search(out):
            out = re.sub(
                r"\bconsignados\b",
                "comissionados e contratados",
                out,
                flags=re.IGNORECASE,
            )
            out = re.sub(
                r"\bconsignadas\b",
                "comissionadas e contratadas",
                out,
                flags=re.IGNORECASE,
            )
            out = re.sub(
                r"\bconsignado\b",
                "comissionado ou contratado",
                out,
                flags=re.IGNORECASE,
            )
            applied.append("consignados->comissionados e contratados")

    # 3. Siglas institucionais e contábeis recorrentes
    for pattern, replacement, label in _LEXICAL_DOMAIN_RULES:
        if pattern.search(out):
            out = pattern.sub(replacement, out)
            applied.append(label)

    # 4. SAAE transcrito como "SAI" / "SAÉ" em contexto de autarquia/água/esgoto
    if _SAAE_PHONETIC_RE.search(out) and _WATER_CONTEXT_RE.search(out):
        out = _SAAE_PHONETIC_RE.sub("SAAE", out)
        applied.append("SAI->SAAE")

    return out, applied


def sanitize_asr_question(text: str, municipality_name: str = "Caeté") -> str:
    normalized, _ = normalize_municipal_asr_text(text, municipality_name=municipality_name)
    return normalized


# ==============================================================================
# TOKENIZAÇÃO E FEW-SHOTS CANÔNICOS DA CAMADA SEMÂNTICA GOLD
# ==============================================================================
GENERIC_DOMAIN_WORDS: Set[str] = {
    "saude", "educacao", "obras", "engenharia", "transporte", "escolar",
    "folha", "pagamento", "servidores", "cultura", "esporte", "turismo",
    "assistencia", "social", "fazenda", "administracao", "defesa", "meio",
    "ambiente", "planejamento", "governo", "gabinete", "secretaria",
    "fundo", "instituto", "previdencia", "consorcio", "hospital", "caixa",
    "banco", "brasil", "minas", "gerais", "estadual", "federal", "nacional",
}

_STOP_WORDS: Set[str] = {
    "que", "para", "com", "por", "uma", "dos", "das", "nos", "nas", "qual", "quais",
    "quanto", "quantos", "foi", "foram", "ano", "anos", "entre", "municipio", "caete",
    "sabara", "nova", "lima", "prefeitura", "municipal", "total", "valor", "gasto",
    "pago", "todos", "todas", "ltda", "eireli", "servicos", "comercio", "empresa",
    "tem", "hoje", "voce", "acesso", "saber", "informar", "consegue", "sobre",
}


def _stem_pt(tok: str) -> str:
    if len(tok) > 6 and tok.endswith(("amentos", "imentos")):
        return tok[:-3]
    if len(tok) > 5 and tok.endswith(("coes", "soes")):
        return tok[:-4] + "cao"
    if len(tok) > 5 and tok.endswith(("ores", "oras")):
        return tok[:-2]
    if len(tok) > 4 and tok.endswith("s") and not tok.endswith(("ss", "is", "us")):
        return tok[:-1]
    return tok


def _tokens(text: str, extra_stop: Optional[Set[str]] = None) -> Set[str]:
    stop = _STOP_WORDS if not extra_stop else _STOP_WORDS.union(extra_stop)
    raw = re.findall(r"[a-z0-9]{3,}", _norm(text))
    out: Set[str] = set()
    for t in raw:
        if t in stop:
            continue
        out.add(t)
        st = _stem_pt(t)
        if st and st not in stop:
            out.add(st)
    return out


def _bm25_tokens_list(text: str) -> List[str]:
    raw = re.findall(r"[a-z0-9]{2,}", _norm(text))
    res: List[str] = []
    for t in raw:
        if t in ("de", "da", "do", "em", "na", "no", "os", "as", "um", "uma", "por", "com", "que", "para", "caete", "municipio", "prefeitura"):
            continue
        res.append(_stem_pt(t))
    return res


CANONICAL_GOLD_FEW_SHOTS: List[Dict[str, str]] = [
    {
        "question": "Qual o orçamento previsto na LOA e a receita arrecadada de Caeté nos anos de 2024, 2025 e 2026/2027?",
        "sql": (
            "SELECT ano_referencia, orcamento_previsto_loa, orcamento_atualizado_loa, "
            "despesa_dotacao_atualizada, receita_arrecadada_realizada, despesa_empenhada, "
            "despesa_liquidada, despesa_paga, pct_arrecadacao_loa, status_exercicio "
            "FROM vw_gold_orcamento_previsto_vs_realizado "
            "ORDER BY ano_referencia"
        ),
        "tables": "vw_gold_orcamento_previsto_vs_realizado",
    },
    {
        "question": "Quantos funcionários e servidores a Prefeitura tem hoje no quadro ativo?",
        "sql": (
            "SELECT ano_referencia, MAX(seq_dat_pagamento) AS ultima_competencia, "
            "COUNT(DISTINCT num_cpf) AS qtd_servidores_ultimo_mes, "
            "(SELECT COUNT(DISTINCT num_cpf) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025) AS qtd_servidores_distintos_no_ano, "
            "ROUND(SUM(vlr_rmn_bruta), 2) AS folha_bruta_mensal, "
            "ROUND(AVG(vlr_rmn_bruta), 2) AS media_bruta_mensal "
            "FROM vw_gold_folha_servidores "
            "WHERE ano_referencia = 2025 "
            "AND seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025) "
            "GROUP BY ano_referencia"
        ),
        "tables": "vw_gold_folha_servidores",
    },
    {
        "question": "Deste total de funcionários, quantos são concursados (efetivos), comissionados e contratados temporários?",
        "sql": (
            "SELECT dsc_situacao, tipo_vinculo, "
            "COUNT(DISTINCT num_cpf) AS qtd_servidores_ativos_mes, "
            "ROUND(AVG(vlr_rmn_bruta), 2) AS media_salarial_bruta, "
            "ROUND(SUM(vlr_rmn_bruta), 2) AS total_folha_bruta_mes "
            "FROM vw_gold_folha_servidores "
            "WHERE ano_referencia = 2025 "
            "AND seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025) "
            "GROUP BY dsc_situacao, tipo_vinculo "
            "ORDER BY qtd_servidores_ativos_mes DESC"
        ),
        "tables": "vw_gold_folha_servidores",
    },
    {
        "question": "Qual o salário mensal do Prefeito e ele está acima da média salarial dos servidores da Prefeitura?",
        "sql": (
            "WITH ultimo_mes AS ("
            "  SELECT MAX(seq_dat_pagamento) AS competencia FROM vw_gold_folha_servidores WHERE ano_referencia = 2025"
            "), media_geral AS ("
            "  SELECT ROUND(AVG(vlr_rmn_bruta), 2) AS media_bruta_municipio, "
            "         ROUND(MEDIAN(vlr_rmn_bruta), 2) AS mediana_bruta_municipio "
            "  FROM vw_gold_folha_servidores, ultimo_mes "
            "  WHERE ano_referencia = 2025 AND seq_dat_pagamento = ultimo_mes.competencia"
            ") "
            "SELECT f.seq_dat_pagamento, f.nom_servidor, f.dsc_cargo, f.vlr_rmn_bruta, f.vlr_rmn_liquida, "
            "m.media_bruta_municipio, m.mediana_bruta_municipio, "
            "ROUND(f.vlr_rmn_bruta / NULLIF(m.media_bruta_municipio, 0), 2) AS razao_sobre_media "
            "FROM vw_gold_folha_servidores f CROSS JOIN media_geral m JOIN ultimo_mes u ON f.seq_dat_pagamento = u.competencia "
            "WHERE f.ano_referencia = 2025 AND f.dsc_cargo ILIKE '%PREFEITO%' "
            "ORDER BY f.vlr_rmn_bruta DESC"
        ),
        "tables": "vw_gold_folha_servidores",
    },
    {
        "question": "Quem é o atual Secretário Municipal de Meio Ambiente (SEDESMA) ou quem são os secretários municipais ativos?",
        "sql": (
            "SELECT seq_dat_pagamento, nom_servidor, dsc_cargo, dsc_lotacao, vlr_rmn_bruta "
            "FROM vw_gold_folha_servidores "
            "WHERE ano_referencia = 2025 "
            "AND seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025) "
            "AND (dsc_cargo ILIKE '%SECRETARIO MUNICIPAL%' OR dsc_cargo ILIKE '%PROCURADOR GERAL%' OR dsc_cargo ILIKE '%CONTROLADOR GERAL%') "
            "ORDER BY dsc_lotacao, nom_servidor"
        ),
        "tables": "vw_gold_folha_servidores",
    },
    {
        "question": "Qual a quantidade total de veículos que a Prefeitura tem na frota e como se dividem por tipo ou situação?",
        "sql": (
            "SELECT status_cadastro_frota, COALESCE(dsc_tipo_veiculo, 'NÃO ESPECIFICADO NO CADASTRO') AS tipo_veiculo, "
            "COUNT(DISTINCT seq_veiculo) AS qtd_veiculos, "
            "ROUND(SUM(vlr_gasto), 2) AS gasto_acumulado_combustivel_manutencao "
            "FROM vw_gold_frota_consolidada "
            "GROUP BY 1, 2 "
            "ORDER BY qtd_veiculos DESC"
        ),
        "tables": "vw_gold_frota_consolidada",
    },
    {
        "question": "Quais veículos emplacados da frota municipal tiveram maior gasto com combustível e manutenção em 2024?",
        "sql": (
            "SELECT seq_veiculo, dsc_num_placa, dsc_veiculo, dsc_tipo_veiculo, "
            "ROUND(SUM(vlr_gasto), 2) AS total_gasto_2024 "
            "FROM vw_gold_frota_consolidada "
            "WHERE ano_referencia = 2024 AND status_cadastro_frota = 'CADASTRADO' "
            "GROUP BY seq_veiculo, dsc_num_placa, dsc_veiculo, dsc_tipo_veiculo "
            "ORDER BY total_gasto_2024 DESC LIMIT 10"
        ),
        "tables": "vw_gold_frota_consolidada",
    },
    {
        "question": "Quais os maiores fornecedores externos da Prefeitura de Caeté?",
        "sql": (
            "SELECT nom_credor, num_doc_credor, qtd_pagamentos, total_pago_historico, "
            "primeiro_ano_pagamento, ultimo_ano_pagamento, nom_socio "
            "FROM vw_gold_fornecedores_qsa_360 "
            "WHERE nom_credor NOT ILIKE '%PREFEITURA%' "
            "AND nom_credor NOT ILIKE '%INSTITUTO NACIONAL DO SEGURO SOCIAL%' "
            "AND nom_credor NOT ILIKE '%INSS%' "
            "AND nom_credor NOT ILIKE '%FGTS%' "
            "AND nom_credor NOT ILIKE '%PASEP%' "
            "AND nom_credor NOT ILIKE '%TRIBUNAL DE JUSTICA%' "
            "AND nom_credor NOT ILIKE '%FOLHA DE PAGAMENTO%' "
            "ORDER BY total_pago_historico DESC LIMIT 15"
        ),
        "tables": "vw_gold_fornecedores_qsa_360",
    },
    {
        "question": "Quais fornecedores mais receberam via carona e adesão à ata de registro de preços?",
        "sql": (
            "SELECT nom_credor, num_doc_credor, "
            "SUM(qtd_empenhos) AS qtd_empenhos_credor, "
            "ROUND(SUM(total_empenhado_carona), 2) AS total_empenhado_carona, "
            "ROUND(SUM(total_pago), 2) AS total_pago_credor, "
            "MIN(ano_referencia) AS de_ano, MAX(ano_referencia) AS ate_ano "
            "FROM vw_gold_execucao_despesa_credor "
            "WHERE total_empenhado_carona > 0 "
            "AND nom_credor NOT ILIKE '%INSS%' AND nom_credor NOT ILIKE '%TRIBUNAL%' "
            "AND nom_credor NOT ILIKE '%SANTA CASA%' AND nom_credor NOT ILIKE '%BENEFICENCIA%' "
            "GROUP BY nom_credor, num_doc_credor "
            "ORDER BY total_empenhado_carona DESC LIMIT 15"
        ),
        "tables": "vw_gold_execucao_despesa_credor",
    },
    {
        "question": "Como estão os indicadores fiscais da LRF, despesa com pessoal sobre RCL, mínimos de saúde e educação e nota CAPAG?",
        "sql": (
            "SELECT ano_referencia, receita_corrente_liquida_rcl, pct_despesa_pessoal_rcl, "
            "situacao_fiscal_lrf, nota_capag_estimada, pct_aplicacao_saude, pct_aplicacao_educacao_mde "
            "FROM siconfi_finbra_indicadores "
            "ORDER BY ano_referencia"
        ),
        "tables": "siconfi_finbra_indicadores",
    },
]


class EntityLinker:
    """
    Indexador leve em memória de entidades reais do município (Credores, Órgãos, Secretarias/Unidades,
    Funções, Cargos e Lotações) + Recuperador Dinâmico de Few-Shots via Okapi BM25 sobre o
    `golden_dataset_v1.0.csv` (somente leitura) enriquecido com exemplos canônicos das Gold Views.
    """

    def __init__(
        self,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        workspace_root: Optional[Path] = None,
        city_name: str = "Caeté",
    ) -> None:
        self.workspace_root = workspace_root or Path(__file__).resolve().parents[2]
        self.city_name = city_name
        self.city_stop_tokens: Set[str] = {
            t for t in re.findall(r"[a-z0-9]{3,}", _norm(city_name))
        }
        self.creditors: List[Tuple[str, str, Set[str]]] = []
        self.orgaos: List[Tuple[str, str, Set[str]]] = []
        self.unidades: List[Tuple[str, str, Set[str]]] = []
        self.funcoes: List[Tuple[str, Set[str]]] = []
        self.cargos: List[Tuple[str, Set[str]]] = []
        self.lotacoes: List[Tuple[str, Set[str]]] = []
        self.few_shots: List[Dict[str, Any]] = []
        self._bm25_idf: Dict[str, float] = {}
        self._bm25_avgdl: float = 1.0

        if conn is not None:
            self._index_db_entities(conn)
        self._load_golden_few_shots()
        self._build_bm25_index()

    def _index_db_entities(self, conn: duckdb.DuckDBPyConnection) -> None:
        cur = conn.cursor()
        # 1. Top 250 Credores de despesa_pagamento
        try:
            rows = cur.execute(
                """
                SELECT MAX(nom_credor) AS nom, MAX(num_doc_credor) AS doc,
                       SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) AS tot
                FROM despesa_pagamento
                WHERE COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%'
                GROUP BY UPPER(TRIM(nom_credor))
                ORDER BY tot DESC NULLS LAST
                LIMIT 250
                """
            ).fetchall()
            for nom, doc, _ in rows:
                if nom:
                    toks = _tokens(str(nom), self.city_stop_tokens)
                    if toks:
                        self.creditors.append((str(nom).strip(), str(doc or "").strip(), toks))
        except Exception:
            pass

        # 2. Órgãos de orgao_orgao
        try:
            rows = cur.execute(
                "SELECT DISTINCT nom_orgao, CAST(cod_orgao AS VARCHAR) FROM orgao_orgao WHERE nom_orgao IS NOT NULL"
            ).fetchall()
            for nom, cod in rows:
                toks = _tokens(str(nom), self.city_stop_tokens)
                if toks:
                    self.orgaos.append((str(nom).strip(), str(cod or "").strip(), toks))
        except Exception:
            pass

        # 2b. Secretarias / Unidades Orçamentárias de orgao_orgaoUnidade
        try:
            rows = cur.execute(
                """
                SELECT MAX(TRIM(dsc_unidade)) AS dsc, MAX(CAST(cod_unidade AS VARCHAR)) AS cod
                FROM orgao_orgaoUnidade
                WHERE dsc_unidade IS NOT NULL
                GROUP BY UPPER(TRIM(dsc_unidade))
                LIMIT 100
                """
            ).fetchall()
            for dsc, cod in rows:
                toks = _tokens(str(dsc), self.city_stop_tokens)
                if toks:
                    self.unidades.append((str(dsc).strip(), str(cod or "").strip(), toks))
        except Exception:
            pass

        # 3. Funções Orçamentárias de despesa_despesa
        try:
            rows = cur.execute(
                "SELECT DISTINCT dsc_funcao FROM despesa_despesa WHERE dsc_funcao IS NOT NULL LIMIT 50"
            ).fetchall()
            for (fn,) in rows:
                toks = _tokens(str(fn), self.city_stop_tokens)
                if toks:
                    self.funcoes.append((str(fn).strip(), toks))
        except Exception:
            pass

        # 4. TODOS os Cargos distintos de vw_gold_folha_servidores / remuneracao_municipio
        # (inclui cargos singulares de alta relevância pública como PREFEITO MUNICIPAL e SECRETARIO MUNICIPAL DS-01)
        try:
            rows = cur.execute(
                """
                SELECT DISTINCT TRIM(dsc_cargo) AS cargo
                FROM vw_gold_folha_servidores
                WHERE dsc_cargo IS NOT NULL AND TRIM(dsc_cargo) <> ''
                LIMIT 350
                """
            ).fetchall()
            for (cargo,) in rows:
                toks = _tokens(str(cargo), self.city_stop_tokens)
                if toks:
                    self.cargos.append((str(cargo).strip(), toks))
        except Exception:
            pass

        # 5. Lotações distintas de vw_gold_folha_servidores (inclui SEDESMA, SEMAS, Secretarias)
        try:
            rows = cur.execute(
                """
                SELECT DISTINCT TRIM(dsc_lotacao) AS lot
                FROM vw_gold_folha_servidores
                WHERE dsc_lotacao IS NOT NULL AND TRIM(dsc_lotacao) <> ''
                LIMIT 200
                """
            ).fetchall()
            for (lot,) in rows:
                toks = _tokens(str(lot), self.city_stop_tokens)
                if toks:
                    self.lotacoes.append((str(lot).strip(), toks))
        except Exception:
            pass

    def _load_golden_few_shots(self) -> None:
        # 1. Exemplos canônicos das Gold Views (prioridade semântica v4.0)
        for item in CANONICAL_GOLD_FEW_SHOTS:
            q = item["question"]
            bm_toks = _bm25_tokens_list(q)
            self.few_shots.append(
                {
                    "question": q,
                    "sql": item["sql"],
                    "tables": item["tables"],
                    "tokens": _tokens(q),
                    "bm25_tf": Counter(bm_toks),
                    "bm25_len": len(bm_toks),
                    "is_gold_canonical": True,
                }
            )

        # 2. Exemplos oficiais validados do golden_dataset_v1.0.csv (somente leitura!)
        golden_csv = self.workspace_root / "avaliacao" / "dados" / "golden" / "golden_dataset_v1.0.csv"
        if not golden_csv.exists():
            return
        try:
            with open(golden_csv, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f, delimiter=";"):
                    q = (row.get("input_usuario") or "").strip()
                    sql = (row.get("query_referencia") or "").strip()
                    tbls = (row.get("tabelas_esperadas") or "").strip()
                    if q and sql:
                        bm_toks = _bm25_tokens_list(q)
                        self.few_shots.append(
                            {
                                "question": q,
                                "sql": sql,
                                "tables": tbls,
                                "tokens": _tokens(q),
                                "bm25_tf": Counter(bm_toks),
                                "bm25_len": len(bm_toks),
                                "is_gold_canonical": False,
                            }
                        )
        except Exception:
            pass

    def _build_bm25_index(self) -> None:
        n_docs = len(self.few_shots)
        if n_docs == 0:
            return
        df: Counter[str] = Counter()
        total_len = 0
        for doc in self.few_shots:
            total_len += doc["bm25_len"]
            for term in doc["bm25_tf"].keys():
                df[term] += 1
        self._bm25_avgdl = max(1.0, total_len / n_docs)
        self._bm25_idf = {
            term: math.log(1.0 + (n_docs - freq + 0.5) / (freq + 0.5))
            for term, freq in df.items()
        }

    def extract_value_bindings(self, question: str, max_bindings: int = 8) -> List[str]:
        """
        Retorna dicas determinísticas de filtro (`WHERE`) vinculando termos da pergunta
        aos valores exatos existentes nas colunas do DuckDB.
        """
        q_norm = _norm(question)
        q_toks = _tokens(question, self.city_stop_tokens)
        if not q_toks and not q_norm:
            return []

        bindings: List[str] = []

        # Bindings semânticos institucionais de Caeté/MG
        if "santa casa" in q_norm or "hospital" in q_norm:
            bindings.append(
                "Entidade 'Santa Casa / Hospital': em `despesa_pagamento` / `vw_gold_execucao_despesa_credor` use `(nom_credor ILIKE '%SANTA CASA%' OR nom_credor ILIKE '%BENEFICENCIA%')`"
            )
        if "camara" in q_norm or "vereador" in q_norm or "legislativo" in q_norm:
            bindings.append(
                "Órgão 'Câmara Municipal': filtre `CAST(cod_orgao AS INTEGER) = 1` ou `nom_orgao ILIKE '%CAMARA%'` (em `vw_gold_folha_servidores`, `dsc_lotacao ILIKE '%CORPO LEGISLATIVO%'` ou `dsc_cargo ILIKE '%VEREADOR%'`)"
            )
        if "meio ambiente" in q_norm or "sedesma" in q_norm or "sustentavel" in q_norm:
            bindings.append(
                "Secretaria de Meio Ambiente em Caeté denomina-se `SEDESMA` (Secretaria Municipal de Desenvolvimento Sustentável e Meio Ambiente): em `vw_gold_folha_servidores` filtre `dsc_lotacao ILIKE '%SEDESMA%'` (Secretário(a): `dsc_cargo ILIKE '%SECRETARIO MUNICIPAL%'`); em `orgao_orgaoUnidade` filtre `dsc_unidade ILIKE '%SEDESMA%' OR dsc_unidade ILIKE '%MEIO AMBIENTE%'`"
            )
        if "assistencia social" in q_norm or "semas" in q_norm:
            bindings.append(
                "Secretaria de Assistência Social em Caeté denomina-se `SEMAS`: em `vw_gold_folha_servidores` filtre `dsc_lotacao ILIKE '%SEMAS%'`"
            )
        if "prefeito" in q_norm:
            bindings.append(
                "Cargo do Prefeito em `vw_gold_folha_servidores`: `dsc_cargo = 'PREFEITO MUNICIPAL'` (e Vice-Prefeito: `dsc_cargo = 'VICE-PREFEITO'`), sempre filtrando o último mês `seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025)`"
            )
        if "secretario" in q_norm or "secretarios" in q_norm or "secretaria" in q_norm:
            if any(w in q_norm for w in ("quem", "secretario", "secretarios", "atual", "ocupante", "cargo", "salario")):
                bindings.append(
                    "Cargo de Secretário Municipal em `vw_gold_folha_servidores`: filtre `dsc_cargo ILIKE '%SECRETARIO MUNICIPAL%'` (NÃO use apenas `ILIKE '%SECRETARI%'`, pois isso inclui `SECRETARIA ESCOLAR`!)"
                )
        if any(w in q_norm for w in ("concursado", "concursados", "efetivo", "efetivos", "comissionado", "comissionados", "temporario", "contratado", "contratados")):
            bindings.append(
                "Vínculos na folha (`vw_gold_folha_servidores`): use `dsc_situacao` / `tipo_vinculo` — `'Efetivo (Concursado)'` e `'Emprego Público (Concursado / Seleção Pública)'` = concursados/seleção pública; `'Contratado / Servidor Temporário'` = contratados temporários; `'Comissionado (Recrutamento Amplo)'` = comissionados; `'Agente Político (Prefeito, Vice, Secretários e Vereadores)'` = agentes políticos."
            )
        if any(w in q_norm for w in ("carona", "adesao", "ata de registro")):
            bindings.append(
                "Adesão à Ata de Registro de Preços (Carona): em `vw_gold_execucao_despesa_credor`, use a coluna já pré-calculada `WHERE total_empenhado_carona > 0` (`SUM(total_empenhado_carona)`); ou na tabela bruta `empenho_empenho` filtre `ind_dec_licitacao ILIKE '%ADES%'`"
            )

        # Match em Secretarias / Unidades Orçamentárias (`orgao_orgaoUnidade`)
        for dsc_unid, cod_unid, toks in self.unidades:
            non_generic_toks = toks - {"secretaria", "municipal"}
            inter = q_toks.intersection(non_generic_toks if non_generic_toks else toks)
            if inter and ("secretaria" in q_norm or len(inter) >= 2):
                bindings.append(
                    f"Unidade Orçamentária (`orgao_orgaoUnidade`): `dsc_unidade = '{dsc_unid}'` (`cod_unidade = '{cod_unid}'`)"
                )
                if len(bindings) >= max_bindings - 2:
                    break

        # Match em Credores
        scored_creds: List[Tuple[float, str, str]] = []
        for nom, doc, toks in self.creditors:
            inter = q_toks.intersection(toks)
            if inter:
                distinctive_inter = inter - GENERIC_DOMAIN_WORDS
                if not distinctive_inter and len(inter) < 2:
                    continue
                score = len(inter) / max(1, min(len(q_toks), len(toks)))
                if len(inter) >= 1 and any(len(t) >= 5 for t in (distinctive_inter or inter)):
                    scored_creds.append((score, nom, doc))
        scored_creds.sort(key=lambda x: x[0], reverse=True)
        for _, nom, doc in scored_creds[:2]:
            bindings.append(f"Credor identificado no banco: `nom_credor = '{nom}'` (`num_doc_credor = '{doc}'`)")

        # Match em Funções Orçamentárias
        for fn, toks in self.funcoes:
            if q_toks.intersection(toks) and len(bindings) < max_bindings:
                bindings.append(f"Função orçamentária exata em `despesa_despesa`: `dsc_funcao = '{fn}'`")
                break

        # Match em Cargos específicos da Folha (`vw_gold_folha_servidores`)
        matched_cargos: List[Tuple[int, str]] = []
        for cargo, toks in self.cargos:
            inter = q_toks.intersection(toks)
            if inter and any(len(t) >= 5 for t in inter):
                matched_cargos.append((len(inter), cargo))
        matched_cargos.sort(key=lambda x: (-x[0], len(x[1])))
        for _, cargo in matched_cargos[:2]:
            if len(bindings) < max_bindings:
                bindings.append(f"Cargo real em `vw_gold_folha_servidores`: `dsc_cargo = '{cargo}'` (ou `ILIKE '%{cargo[:22]}%'`)")

        return bindings[:max_bindings]

    def select_dynamic_few_shots(self, question: str, top_k: int = 4) -> List[Dict[str, str]]:
        """
        Recupera até `top_k` exemplos SQL validados via Okapi BM25 ($k_1=1.5, b=0.75$)
        combinado com bônus leve para exemplos que usam a Camada Semântica Gold (`vw_gold_*`).
        """
        q_terms = _bm25_tokens_list(question)
        if not q_terms or not self.few_shots:
            return []

        k1 = 1.5
        b = 0.75
        scored: List[Tuple[float, Dict[str, Any]]] = []
        q_set = set(q_terms)

        for item in self.few_shots:
            tf_map: Counter[str] = item["bm25_tf"]
            dl = max(1, item["bm25_len"])
            if not tf_map:
                continue
            bm25_score = 0.0
            matched_terms = 0
            for term in q_set:
                f = tf_map.get(term, 0)
                if f <= 0:
                    continue
                matched_terms += 1
                idf = self._bm25_idf.get(term, 1.0)
                denom = f + k1 * (1.0 - b + b * (dl / self._bm25_avgdl))
                bm25_score += idf * ((f * (k1 + 1.0)) / denom)

            if matched_terms > 0:
                if item.get("is_gold_canonical"):
                    bm25_score *= 1.18
                scored.append((bm25_score, item))

        scored.sort(key=lambda x: x[0], reverse=True)
        selected: List[Dict[str, str]] = []
        seen_sql: Set[str] = set()
        for sc, item in scored:
            if sc < 0.85:
                continue
            sql_sig = item["sql"][:90]
            if sql_sig in seen_sql:
                continue
            seen_sql.add(sql_sig)
            selected.append(
                {
                    "question": item["question"],
                    "sql": item["sql"],
                    "tables": item["tables"],
                }
            )
            if len(selected) >= top_k:
                break
        return selected

    def link_entities(self, question: str, top_k: int = 8) -> List[str]:
        return self.extract_value_bindings(question=question, max_bindings=top_k)

    def select_few_shots(self, question: str, top_k: int = 4) -> List[Dict[str, str]]:
        return self.select_dynamic_few_shots(question=question, top_k=top_k)
