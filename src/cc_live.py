"""
SourceSeek - Búsqueda dinámica en Common Crawl con cache TTL
Módulo independiente para consultas en tiempo real durante inferencia.
"""
import logging
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

log = logging.getLogger(__name__)

# ── Cache TTL ────────────────────────────────────────────────────────────────
_CACHE_TTL = 3600  # 1 hora en segundos
_cache: dict[str, tuple[float, list[dict]]] = {}   # key → (timestamp, chunks)


# ════════════════════════════════════════════════════════════════════════════
# Conversión docs → chunks
# ════════════════════════════════════════════════════════════════════════════

def _docs_to_chunks(docs: list[dict], entity_name: str = "") -> list[dict]:
    """
    Convierte documentos de CC a formato chunk compatible con HybridRetriever.
    Los chunks de CC-live reciben score provisional alto porque son el resultado
    de una búsqueda directa por nombre de entidad.
    """
    chunks = []
    for i, doc in enumerate(docs):
        text = doc.get("text", "")
        if not text:
            continue
        chunk = {
            "doc_id":       f"cc_live_{i}",
            "chunk_id":     f"cc_live_{i}_0",
            "text":         text,
            "url":          doc.get("url", ""),
            "source":       doc.get("source", "cc_live"),
            "type":         doc.get("type", "concept"),
            "entity":       doc.get("entity", entity_name),
            "domain":       doc.get("domain", ""),
            # Score provisional alto — es un resultado directo por nombre
            "score":        0.85,
            "vector_score": 0.0,
            "bm25_score":   0.0,
        }
        chunks.append(chunk)
    return chunks


# ════════════════════════════════════════════════════════════════════════════
# API pública
# ════════════════════════════════════════════════════════════════════════════

def live_search(entity_name: str, max_results: int = 5) -> list[dict]:
    """
    Búsqueda en tiempo real en Common Crawl para una entidad específica.
    Retorna chunks listos para usar directamente en SourceSeekRAG.

    Importa search_entity_cc de ingest de forma lazy para no cargar
    todo el módulo de ingesta en el inicio del servidor.
    """
    try:
        from ingest import search_entity_cc  # importación lazy
        docs = search_entity_cc(entity_name, max_results=max_results)
        return _docs_to_chunks(docs, entity_name=entity_name)
    except Exception as e:
        log.warning(f"[CC-Live] Error en live_search('{entity_name}'): {e}")
        return []


def live_search_cached(entity_name: str, max_results: int = 5) -> list[dict]:
    """
    Búsqueda con cache TTL de 1 hora.
    Si la entidad fue buscada recientemente se devuelve el resultado cacheado
    sin hacer nuevas peticiones a Common Crawl.
    """
    key = entity_name.lower().strip()
    now = time.time()

    if key in _cache:
        ts, cached_chunks = _cache[key]
        if now - ts < _CACHE_TTL:
            log.info(
                f"[CC-Live] Cache HIT '{entity_name}' "
                f"({len(cached_chunks)} chunks, "
                f"{int((_CACHE_TTL - (now - ts)) / 60)} min restantes)"
            )
            return cached_chunks

    log.info(f"[CC-Live] Cache MISS '{entity_name}', buscando en CC...")
    chunks = live_search(entity_name, max_results=max_results)

    _cache[key] = (now, chunks)

    if chunks:
        log.info(f"[CC-Live] '{entity_name}' → {len(chunks)} chunks guardados en cache")
    else:
        log.info(f"[CC-Live] '{entity_name}' → sin resultados (cacheando vacío)")

    return chunks


def invalidate(entity_name: Optional[str] = None):
    """Invalida la cache para una entidad o completamente."""
    global _cache
    if entity_name:
        removed = _cache.pop(entity_name.lower().strip(), None)
        if removed:
            log.info(f"[CC-Live] Cache invalidada para '{entity_name}'")
    else:
        count = len(_cache)
        _cache = {}
        log.info(f"[CC-Live] Cache completa invalidada ({count} entradas)")


def cache_stats() -> dict:
    """Estadísticas de la cache para el endpoint /api/status."""
    now = time.time()
    entries = []
    for key, (ts, chunks) in _cache.items():
        age_s = int(now - ts)
        entries.append({
            "entity":    key,
            "chunks":    len(chunks),
            "age_s":     age_s,
            "valid":     age_s < _CACHE_TTL,
            "ttl_left":  max(0, _CACHE_TTL - age_s),
        })
    valid   = sum(1 for e in entries if e["valid"])
    expired = len(entries) - valid
    return {
        "total":   len(entries),
        "valid":   valid,
        "expired": expired,
        "entries": entries,
    }
