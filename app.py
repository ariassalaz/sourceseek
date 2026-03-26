"""
SourceSeek - API Flask
Endpoints REST para búsqueda semántica de fútbol.
"""

import json
import logging
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from flask import Flask, request, jsonify
from flask_cors import CORS

import src.rag as _rag_module
from src.rag import get_rag_engine
from src.ingest import run_ingestion
from src.indexer import run_indexing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        if not Path("data/football.index").exists():
            log.info("Índice no encontrado, construyendo automáticamente...")
            docs = run_ingestion(use_live=False)
            run_indexing(docs)
        _engine = get_rag_engine()
    return _engine


# ════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Sirve el frontend principal."""
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()


@app.route("/api/search", methods=["POST"])
def search():
    """
    POST /api/search
    Body: { "query": "string", "top_k": int, "type_filter": "player|club|competition|..." }
    """
    data        = request.get_json(silent=True) or {}
    query       = data.get("query", "").strip()
    top_k       = min(int(data.get("top_k", 5)), 10)
    type_filter = data.get("type_filter", None) or None

    if not query:
        return jsonify({"error": "El campo 'query' es requerido"}), 400
    if len(query) > 500:
        return jsonify({"error": "La consulta no puede superar 500 caracteres"}), 400

    try:
        engine = get_engine()
        result = engine.search(query, top_k=top_k, type_filter=type_filter)

        clean_chunks = []
        for c in result["chunks"]:
            clean_chunks.append({
                "chunk_id":     c.get("chunk_id"),
                "text":         c.get("text", "")[:700],
                "url":          c.get("url", ""),
                "source":       c.get("source", ""),
                "type":         c.get("type", ""),
                "entity":       c.get("entity", ""),
                "score":        round(c.get("score", 0), 4),
                "vector_score": round(c.get("vector_score", 0), 4),
                "bm25_score":   round(c.get("bm25_score", 0), 4),
            })

        # Calcular distribución de fuentes
        source_dist = {}
        for c in clean_chunks:
            s = c.get("source", "unknown")
            source_dist[s] = source_dist.get(s, 0) + 1

        return jsonify({
            "query":        result["query"],
            "answer":       result["answer"],
            "chunks":       clean_chunks,
            "sources":      result["sources"],
            "used_llm":     result["used_llm"],
            "cc_live_used": result.get("cc_live_used", False),
            "search_ms":    result.get("search_ms", 0),
            "type_filter":  type_filter,
            "stats": {
                "total_chunks": len(clean_chunks),
                "top_score":    clean_chunks[0]["score"] if clean_chunks else 0,
                "source_dist":  source_dist,
            }
        })

    except Exception as e:
        log.error(f"Error en /api/search: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/suggest", methods=["GET"])
def suggest():
    """
    GET /api/suggest?q=messi
    Devuelve sugerencias de búsqueda basadas en entidades conocidas del índice.
    """
    q = request.args.get("q", "").strip().lower()
    if len(q) < 2:
        return jsonify({"suggestions": []})

    try:
        engine = get_engine()
        # Buscar entidades en chunks que coincidan con el prefijo
        seen = set()
        suggestions = []
        for chunk in engine.chunks:
            entity = chunk.get("entity", "").strip()
            if entity and entity.lower().startswith(q) and entity not in seen:
                seen.add(entity)
                suggestions.append({
                    "label": entity,
                    "type":  chunk.get("type", ""),
                })
                if len(suggestions) >= 8:
                    break
        return jsonify({"suggestions": suggestions})
    except Exception as e:
        log.error(f"Error en /api/suggest: {e}")
        return jsonify({"suggestions": []})


@app.route("/api/entity", methods=["GET"])
def entity_info():
    """
    GET /api/entity?name=Lionel+Messi&type=player
    Devuelve información de una entidad específica del índice.
    """
    name       = request.args.get("name", "").strip()
    type_hint  = request.args.get("type", "").strip()
    if not name:
        return jsonify({"error": "Parámetro 'name' requerido"}), 400

    try:
        engine = get_engine()
        name_lower = name.lower()
        matches = []
        seen_docs = set()
        for chunk in engine.chunks:
            entity = chunk.get("entity", "").lower()
            text   = chunk.get("text", "").lower()
            if (name_lower in entity or name_lower in text[:200]):
                doc_id = chunk.get("doc_id")
                if doc_id not in seen_docs:
                    seen_docs.add(doc_id)
                    if not type_hint or chunk.get("type") == type_hint:
                        matches.append({
                            "entity": chunk.get("entity", ""),
                            "type":   chunk.get("type", ""),
                            "source": chunk.get("source", ""),
                            "url":    chunk.get("url", ""),
                            "text":   chunk.get("text", "")[:500],
                        })
            if len(matches) >= 5:
                break
        return jsonify({"name": name, "results": matches, "count": len(matches)})
    except Exception as e:
        log.error(f"Error en /api/entity: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/status", methods=["GET"])
def status():
    """Verifica el estado del sistema."""
    index_exists  = Path("data/football.index").exists()
    chunks_exists = Path("data/chunks.pkl").exists()
    nquads_exists = Path("data/football_knowledge.nq").exists()

    engine_loaded = _engine is not None and _engine._loaded
    index_size    = 0
    chunk_count   = 0
    source_stats  = {}

    if engine_loaded:
        index_size  = _engine.index.ntotal
        chunk_count = len(_engine.chunks)
        for c in _engine.chunks:
            s = c.get("source", "unknown")
            source_stats[s] = source_stats.get(s, 0) + 1

    ollama_available = False
    try:
        import requests as req
        r = req.get("http://localhost:11434/api/tags", timeout=2)
        ollama_available = r.status_code == 200
    except Exception:
        pass

    cc_live_cache = {}
    try:
        from src.cc_live import cache_stats
        cc_live_cache = cache_stats()
    except Exception:
        pass

    return jsonify({
        "status":          "ready" if index_exists else "not_indexed",
        "index_exists":    index_exists,
        "chunks_file":     chunks_exists,
        "nquads_file":     nquads_exists,
        "engine_loaded":   engine_loaded,
        "index_size":      index_size,
        "chunk_count":     chunk_count,
        "source_stats":    source_stats,
        "ollama":          ollama_available,
        "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
        "llm_model":       "llama3 (via Ollama)",
        "search_mode":     "BM25 + Vector RRF + Cross-Encoder + CC Live",
        "cc_live_cache":   cc_live_cache,
    })


@app.route("/api/rebuild", methods=["POST"])
def rebuild():
    """Reconstruye el índice desde cero."""
    global _engine
    try:
        body     = request.get_json(silent=True, force=True) or {}
        use_live = body.get("use_live", False)
        use_cc   = body.get("use_common_crawl", False)
        log.info(f"Reconstruyendo índice (live={use_live}, cc={use_cc})...")
        docs = run_ingestion(use_live=use_live)
        run_indexing(docs)
        # Forzar recarga del motor RAG con el nuevo índice
        _rag_module._engine = None
        _engine = None
        _engine = get_rag_engine()
        return jsonify({
            "status":      "rebuilt",
            "doc_count":   len(docs),
            "chunk_count": len(_engine.chunks),
            "index_size":  _engine.index.ntotal,
        })
    except Exception as e:
        log.error(f"Error en /api/rebuild: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/live-search", methods=["POST"])
def live_search():
    """
    POST /api/live-search
    Body: { "entity": "Jordan Carrillo", "max_results": 5 }
    Búsqueda directa en Common Crawl en tiempo real para una entidad.
    Útil para entidades recientes no presentes en el índice.
    """
    data       = request.get_json(silent=True) or {}
    entity     = data.get("entity", "").strip()
    max_res    = min(int(data.get("max_results", 5)), 10)

    if not entity:
        return jsonify({"error": "El campo 'entity' es requerido"}), 400
    if len(entity) > 200:
        return jsonify({"error": "El nombre de entidad no puede superar 200 caracteres"}), 400

    try:
        from src.cc_live import live_search_cached, cache_stats
        chunks = live_search_cached(entity, max_results=max_res)
        clean  = [
            {
                "text":   c.get("text", "")[:700],
                "url":    c.get("url", ""),
                "source": c.get("source", ""),
                "type":   c.get("type", ""),
                "entity": c.get("entity", ""),
                "domain": c.get("domain", ""),
                "score":  c.get("score", 0),
            }
            for c in chunks
        ]
        return jsonify({
            "entity":  entity,
            "found":   len(clean) > 0,
            "chunks":  clean,
            "count":   len(clean),
            "cache":   cache_stats(),
        })
    except Exception as e:
        log.error(f"Error en /api/live-search: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/graph-stats", methods=["GET"])
def graph_stats():
    """Estadísticas del grafo RDF / N-Quads."""
    nq_path  = Path("data/football_knowledge.nq")
    ttl_path = Path("data/football_knowledge.ttl")

    stats = {
        "nquads_available": nq_path.exists(),
        "turtle_available": ttl_path.exists(),
        "nquads_size_kb":   round(nq_path.stat().st_size / 1024, 1) if nq_path.exists() else 0,
        "turtle_size_kb":   round(ttl_path.stat().st_size / 1024, 1) if ttl_path.exists() else 0,
    }

    if nq_path.exists():
        triple_count = 0
        type_counts = {}
        with open(nq_path, encoding="utf-8") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    triple_count += 1
                    # Contar tipos de predicados
                    if "rdf-syntax-ns#type" in line:
                        if "schema.org/Person" in line:
                            type_counts["players"] = type_counts.get("players", 0) + 1
                        elif "schema.org/SportsTeam" in line:
                            type_counts["teams"] = type_counts.get("teams", 0) + 1
                        elif "schema.org/SportsEvent" in line:
                            type_counts["events"] = type_counts.get("events", 0) + 1
        stats["triple_count"] = triple_count
        stats["entity_types"] = type_counts

    return jsonify(stats)


@app.route("/api/graph-sample", methods=["GET"])
def graph_sample():
    """Devuelve una muestra del grafo N-Quads."""
    nq_path = Path("data/football_knowledge.nq")
    if not nq_path.exists():
        return jsonify({"error": "N-Quads no generados aún"}), 404

    limit = min(int(request.args.get("limit", 20)), 100)
    predicate_filter = request.args.get("predicate", None)
    lines = []
    with open(nq_path, encoding="utf-8") as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                if predicate_filter and predicate_filter not in line:
                    continue
                lines.append(line.strip())
                if len(lines) >= limit:
                    break

    return jsonify({"triples": lines, "showing": len(lines)})


if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════╗")
    log.info("║   SourceSeek Football Search v2.0         ║")
    log.info("║   BM25 + Vector Hybrid Search             ║")
    log.info("║   http://localhost:5000                   ║")
    log.info("╚═══════════════════════════════════════════╝")
    get_engine()
    app.run(host="0.0.0.0", port=5000, debug=False)
