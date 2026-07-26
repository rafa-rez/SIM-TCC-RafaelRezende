#!/usr/bin/env python3
"""
Avaliação de recuperação documental (RAG) sobre o golden RAG.

Uso:
  python eval/run_rag_eval.py --experiment rag_baseline_v1
  python eval/run_rag_eval.py --experiment rag_baseline_v1 --dry-run
  python eval/run_rag_eval.py --experiment rag_baseline_v1 --import-checkpoint caminho.jsonl
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.config_loader import get_openai_api_key, load_config, resolve_path
from eval.lib.paths import EXPERIMENTOS_ROOT, GOLDEN_RAG_DATASET
from eval.lib.rag_compare import (
    aggregate_mrr,
    aggregate_recall,
    extract_doc_label,
    extract_match_text,
    hit_expected,
    recall_at_k,
    reciprocal_rank,
)

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None


def load_golden(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def probe_qdrant(url: str, collection: str | None = None) -> dict:
    """Lista collections e, se informada, metadados da collection alvo."""
    if requests is None:
        raise RuntimeError("Pacote requests não instalado.")

    base = url.rstrip("/")
    resp = requests.get(f"{base}/collections", timeout=10)
    resp.raise_for_status()
    info: dict = {"url": base, "collections": resp.json()}

    if collection:
        col_resp = requests.get(f"{base}/collections/{collection}", timeout=10)
        col_resp.raise_for_status()
        info["target"] = col_resp.json()
    return info


def embed_question(question: str, model: str) -> list[float]:
    from openai import OpenAI

    client = OpenAI(api_key=get_openai_api_key())
    response = client.embeddings.create(model=model, input=question)
    return list(response.data[0].embedding)


def search_qdrant(
    question: str,
    *,
    url: str,
    collection: str,
    top_k: int,
    vector: list[float] | None = None,
    embedding_model: str | None = None,
    qdrant_filter: dict | None = None,
) -> list[dict]:
    """Busca no Qdrant com vetor informado ou gerado via OpenAI."""
    if vector is None:
        if not embedding_model:
            raise RuntimeError(
                "Busca Qdrant requer vetor ou embedding_model. "
                "Use --import-checkpoint ou configure rag.embedding_model."
            )
        vector = embed_question(question, embedding_model)
    if requests is None:
        raise RuntimeError("Pacote requests não instalado.")

    endpoint = f"{url.rstrip('/')}/collections/{collection}/points/search"
    payload = {
        "vector": vector,
        "limit": top_k,
        "with_payload": True,
    }
    if qdrant_filter:
        payload["filter"] = qdrant_filter
    resp = requests.post(endpoint, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("result") or []


# Busca híbrida por número de ato (ex.: "decreto 013/2024"): a busca densa não
# codifica bem o número do ato; chunks cujo CABEÇALHO cita o ato pedido entram
# na frente dos resultados densos.
ACT_HEADER_RE = re.compile(
    r"\b(DECRETO|LEI COMPLEMENTAR|LEI|PORTARIA|EDITAL)\s+(?:MUNICIPAL\s+)?"
    r"N[ºO°\.]?\s*\.?\s*(\d{1,4})\s*/\s*(\d{4})",
    re.IGNORECASE,
)
ACT_QUESTION_NUM_RE = re.compile(r"(\d{1,4})\s*/\s*(\d{4})")
ACT_QUESTION_TIPO_RE = re.compile(
    r"\b(decreto|lei complementar|lei|portaria|edital)\b", re.IGNORECASE
)


def build_act_header_index(url: str, collection: str) -> dict[tuple, list[dict]]:
    """Varre a collection uma vez e agrupa pontos pelo ato citado no cabeçalho."""
    if requests is None:
        raise RuntimeError("Pacote requests não instalado.")
    index: dict[tuple, list[dict]] = {}
    offset = None
    while True:
        body = {"limit": 500, "with_payload": True, "with_vector": False}
        if offset:
            body["offset"] = offset
        resp = requests.post(
            f"{url.rstrip('/')}/collections/{collection}/points/scroll",
            json=body,
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()["result"]
        for point in result["points"]:
            content = (point.get("payload") or {}).get("page_content") or ""
            m = ACT_HEADER_RE.search(content[:200].upper())
            if not m:
                continue
            key = (m.group(1).upper(), int(m.group(2)), int(m.group(3)))
            index.setdefault(key, []).append(
                {"id": point.get("id"), "payload": point.get("payload"), "score": None}
            )
        offset = result.get("next_page_offset")
        if not offset or not result["points"]:
            break
    return index


def act_exact_hits(question: str, act_index: dict[tuple, list[dict]]) -> list[dict]:
    """Chunks cujo cabeçalho corresponde ao ato citado na pergunta (se houver)."""
    num_match = ACT_QUESTION_NUM_RE.search(question)
    if not num_match:
        return []
    numero, ano = int(num_match.group(1)), int(num_match.group(2))
    tipo_match = ACT_QUESTION_TIPO_RE.search(question)
    tipos = [tipo_match.group(1).upper()] if tipo_match else [
        "DECRETO", "LEI COMPLEMENTAR", "LEI", "PORTARIA", "EDITAL"
    ]
    hits: list[dict] = []
    for tipo in tipos:
        hits.extend(act_index.get((tipo, numero, ano), []))
    return hits


# Mapeia palavra-chave da pergunta -> valor de tipo_ato no payload (P1.4,
# espelha o filtro do workflow n8n).
TIPO_ATO_KEYWORDS = [
    ("decreto", "DECRETO"),
    ("lei complementar", "LEI"),
    ("lei ", "LEI"),
    ("portaria", "PORTARIA"),
    ("edital", "AVISO_LICITACAO"),
    ("resolução", "RESOLUCAO"),
    ("resolucao", "RESOLUCAO"),
]


def infer_tipo_ato(question: str) -> str | None:
    q = question.lower()
    for kw, tipo in TIPO_ATO_KEYWORDS:
        if kw in q:
            return tipo
    return None


def dedup_hits(hits: list[dict], top_k: int) -> list[dict]:
    """Remove hits com page_content idêntico, preservando a ordem por score."""
    seen: set[str] = set()
    out: list[dict] = []
    for hit in hits:
        text = ((hit.get("payload") or {}).get("page_content") or "").strip()
        key = hashlib.md5(text.encode("utf-8")).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        out.append(hit)
        if len(out) >= top_k:
            break
    return out


def process_item(
    row: dict,
    hits: list[dict],
    top_k: int,
) -> dict:
    expected = row["doc_id_esperado"]
    retrieved_labels = [extract_doc_label(h) for h in hits]
    match_texts = [extract_match_text(h) for h in hits]
    rank = None
    for i, text in enumerate(match_texts, start=1):
        if hit_expected(expected, text):
            rank = i
            break

    return {
        "id": row["id"],
        "pergunta": row["pergunta"],
        "doc_id_esperado": expected,
        "titulo_documento": row.get("titulo_documento", ""),
        "retrieved": [
            {
                "rank": i + 1,
                "label": label,
                "match_text_preview": match_texts[i][:120] if i < len(match_texts) else "",
                "score": hits[i].get("score") if i < len(hits) else None,
                "payload": hits[i].get("payload") if i < len(hits) else None,
            }
            for i, label in enumerate(retrieved_labels)
        ],
        "recall_at_k": recall_at_k(expected, match_texts, top_k),
        "reciprocal_rank": reciprocal_rank(expected, match_texts),
        "rank_hit": rank,
        "dificuldade": row.get("dificuldade", ""),
        "tipo": row.get("tipo", ""),
        "estrato": row.get("estrato", ""),
    }


def wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Intervalo de confiança de Wilson (95% por padrão) para proporções."""
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    margin = (z / denom) * ((p * (1 - p) / n + z**2 / (4 * n**2)) ** 0.5)
    return (max(0.0, center - margin) * 100, min(1.0, center + margin) * 100)


def _subset_metrics(results: list[dict], top_k: int) -> dict:
    recalls = [bool(r["recall_at_k"]) for r in results]
    rrs = [float(r["reciprocal_rank"]) for r in results]
    hits = sum(recalls)
    n = len(results)
    lo, hi = wilson_ci(hits, n)
    return {
        "n": n,
        "recall_at_k": round(aggregate_recall(recalls) * 100, 2),
        "mrr": round(aggregate_mrr(rrs) * 100, 2),
        "hits": hits,
        "wilson_95_recall_lo": round(lo, 2),
        "wilson_95_recall_hi": round(hi, 2),
    }


def write_summary(out_dir: Path, results: list[dict], top_k: int) -> dict:
    recalls = [bool(r["recall_at_k"]) for r in results]
    rrs = [float(r["reciprocal_rank"]) for r in results]
    hits = sum(recalls)
    n = len(results)
    lo, hi = wilson_ci(hits, n)
    summary = {
        "n": n,
        "top_k": top_k,
        "recall_at_k": round(aggregate_recall(recalls) * 100, 2),
        "mrr": round(aggregate_mrr(rrs) * 100, 2),
        "hits": hits,
        "wilson_95_recall_lo": round(lo, 2),
        "wilson_95_recall_hi": round(hi, 2),
        "generated_at": datetime.now().isoformat(),
        "por_dificuldade": {},
        "por_estrato": {},
    }
    for diff in ("fácil", "médio", "difícil"):
        subset = [r for r in results if r.get("dificuldade") == diff]
        if not subset:
            continue
        summary["por_dificuldade"][diff] = _subset_metrics(subset, top_k)

    estratos = sorted({r.get("estrato") or "sem_estrato" for r in results})
    for estrato in estratos:
        subset = [r for r in results if (r.get("estrato") or "sem_estrato") == estrato]
        if subset:
            summary["por_estrato"][estrato] = _subset_metrics(subset, top_k)

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "metrics_summary_rag.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def import_checkpoint(path: Path) -> dict[str, list[dict]]:
    by_id: dict[str, list[dict]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            by_id[str(rec["id"])] = rec.get("hits") or rec.get("retrieved_raw") or []
    return by_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Avaliação RAG — golden Caeté")
    parser.add_argument("--experiment", default="rag_baseline_v1")
    parser.add_argument("--dataset", default=None, help="CSV golden RAG")
    parser.add_argument("--collection", default=None, help="Collection Qdrant (default: config)")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument(
        "--dedup",
        action="store_true",
        help="Deduplica page_content idêntico no retrieval (busca fetch-k e mantém top-k únicos)",
    )
    parser.add_argument(
        "--fetch-k",
        type=int,
        default=None,
        help="Candidatos buscados antes do dedup (padrão: 4x top-k; ignorado sem --dedup)",
    )
    parser.add_argument(
        "--hybrid-act",
        action="store_true",
        help=(
            "Busca híbrida: chunks cujo cabeçalho cita o ato pedido (nº/ano na pergunta) "
            "entram antes dos resultados densos"
        ),
    )
    parser.add_argument(
        "--filtro-tipo-ato",
        action="store_true",
        help="Filtra payload tipo_ato no Qdrant conforme o tipo citado na pergunta (P1.4)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Lista itens sem consultar Qdrant")
    parser.add_argument(
        "--probe-qdrant",
        action="store_true",
        help="Testa conexão com Qdrant e lista collections",
    )
    parser.add_argument(
        "--import-checkpoint",
        default=None,
        help="JSONL com hits por id (exportados de produção)",
    )
    args = parser.parse_args()

    cfg = load_config()
    rag_cfg = cfg.get("rag") or {}
    qdrant_url = rag_cfg.get("qdrant_url", "http://localhost:6333")
    collection = args.collection or rag_cfg.get("collection", "jornais_caete")
    embedding_model = rag_cfg.get("embedding_model")

    if args.probe_qdrant:
        try:
            info = probe_qdrant(qdrant_url, collection)
            print(json.dumps(info, ensure_ascii=False, indent=2))
        except Exception as exc:
            print(f"Falha ao conectar em {qdrant_url}: {exc}", file=sys.stderr)
            sys.exit(1)
        return
    dataset = Path(args.dataset) if args.dataset else resolve_path(
        rag_cfg.get("dataset", "dados/golden_rag/golden_rag_v1.0.csv")
    )
    top_k = args.top_k or int(rag_cfg.get("top_k", 5))
    dedup_enabled = args.dedup or bool(rag_cfg.get("dedup", False))
    fetch_k = (args.fetch_k or int(rag_cfg.get("fetch_k", top_k * 4))) if dedup_enabled else top_k
    hybrid_act = args.hybrid_act or bool(rag_cfg.get("hybrid_act", False))
    filtro_tipo = args.filtro_tipo_ato or bool(rag_cfg.get("filtro_tipo_ato", False))

    if not dataset.exists():
        print(f"Dataset não encontrado: {dataset}", file=sys.stderr)
        sys.exit(1)

    rows = load_golden(dataset)
    out_dir = EXPERIMENTOS_ROOT / args.experiment
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = {
        "experiment": args.experiment,
        "dataset": str(dataset),
        "top_k": top_k,
        "qdrant_url": qdrant_url,
        "collection": collection,
        "embedding_model": embedding_model,
        "dedup": dedup_enabled,
        "fetch_k": fetch_k if dedup_enabled else None,
        "hybrid_act": hybrid_act,
        "filtro_tipo_ato": filtro_tipo,
        "started_at": datetime.now().isoformat(),
        "n": len(rows),
    }
    (out_dir / "experiment_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if args.dry_run:
        print(f"Golden RAG: {len(rows)} itens em {dataset}")
        for row in rows:
            print(f"  [{row['id']}] {row['pergunta'][:70]}... -> {row['doc_id_esperado']}")
        print(f"Saída prevista: {out_dir}/checkpoint_rag.jsonl")
        return

    imported: dict[str, list[dict]] = {}
    if args.import_checkpoint:
        imported = import_checkpoint(Path(args.import_checkpoint))

    act_index: dict[tuple, list[dict]] = {}
    if hybrid_act:
        act_index = build_act_header_index(qdrant_url, collection)
        print(f"Índice de cabeçalhos: {len(act_index)} atos distintos", file=sys.stderr)

    results: list[dict] = []
    checkpoint_path = out_dir / "checkpoint_rag.jsonl"

    with open(checkpoint_path, "w", encoding="utf-8") as ck:
        for row in rows:
            item_id = row["id"]
            if item_id in imported:
                hits = imported[item_id]
            else:
                qdrant_filter = None
                if filtro_tipo:
                    tipo = infer_tipo_ato(row["pergunta"])
                    if tipo:
                        qdrant_filter = {"must": [{"key": "tipo_ato", "match": {"value": tipo}}]}
                try:
                    hits = search_qdrant(
                        row["pergunta"],
                        url=qdrant_url,
                        collection=collection,
                        top_k=fetch_k,
                        embedding_model=embedding_model,
                        qdrant_filter=qdrant_filter,
                    )
                except Exception as exc:
                    print(f"[{item_id}] Erro na busca: {exc}", file=sys.stderr)
                    hits = []
            if hybrid_act:
                hits = act_exact_hits(row["pergunta"], act_index) + hits
            if dedup_enabled or hybrid_act:
                hits = dedup_hits(hits, top_k)

            record = process_item(row, hits, top_k)
            results.append(record)
            ck.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = write_summary(out_dir, results, top_k)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
