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
import json
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
    resp = requests.post(endpoint, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("result") or []


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
    }


def write_summary(out_dir: Path, results: list[dict], top_k: int) -> dict:
    recalls = [bool(r["recall_at_k"]) for r in results]
    rrs = [float(r["reciprocal_rank"]) for r in results]
    summary = {
        "n": len(results),
        "top_k": top_k,
        "recall_at_k": round(aggregate_recall(recalls) * 100, 2),
        "mrr": round(aggregate_mrr(rrs) * 100, 2),
        "hits": sum(recalls),
        "generated_at": datetime.now().isoformat(),
        "por_dificuldade": {},
    }
    for diff in ("fácil", "médio", "difícil"):
        subset = [r for r in results if r.get("dificuldade") == diff]
        if not subset:
            continue
        summary["por_dificuldade"][diff] = {
            "n": len(subset),
            "recall_at_k": round(
                aggregate_recall([bool(r["recall_at_k"]) for r in subset]) * 100, 2
            ),
        }

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
    parser.add_argument("--top-k", type=int, default=None)
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
    collection = rag_cfg.get("collection", "jornais_caete")
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

    results: list[dict] = []
    checkpoint_path = out_dir / "checkpoint_rag.jsonl"

    with open(checkpoint_path, "w", encoding="utf-8") as ck:
        for row in rows:
            item_id = row["id"]
            if item_id in imported:
                hits = imported[item_id]
            else:
                try:
                    hits = search_qdrant(
                        row["pergunta"],
                        url=qdrant_url,
                        collection=collection,
                        top_k=top_k,
                        embedding_model=embedding_model,
                    )
                except Exception as exc:
                    print(f"[{item_id}] Erro na busca: {exc}", file=sys.stderr)
                    hits = []

            record = process_item(row, hits, top_k)
            results.append(record)
            ck.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary = write_summary(out_dir, results, top_k)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
