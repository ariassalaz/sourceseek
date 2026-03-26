"""
SourceSeek - Búsqueda en vivo multi-fuente
Orden de prioridad (más rápida y fiable primero):
  1. Wikipedia ES (API oficial, gratis, cobertura excelente de fútbol)
  2. Wikipedia EN
  3. Wikidata (datos estructurados)
  4. TheSportsDB (API gratuita de jugadores/equipos)
  5. Common Crawl (lento pero amplio, último recurso)
Cache TTL en memoria: 1 hora.
"""
import hashlib
import logging
import re
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# ── Cache TTL ────────────────────────────────────────────────────────────────
_CACHE_TTL = 3600
_cache: dict[str, tuple[float, list[dict]]] = {}

# ── Sesión HTTP reutilizable ─────────────────────────────────────────────────
_session: Optional[requests.Session] = None

def _sess() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503])
        _session.mount("https://", HTTPAdapter(max_retries=retry))
        _session.mount("http://",  HTTPAdapter(max_retries=retry))
        _session.headers.update({
            "User-Agent": "SourceSeek/1.0 (football research; contact: sourceseek@itl.edu.mx)"
        })
    return _session


# ════════════════════════════════════════════════════════════════════════════
# 1. Wikipedia API — búsqueda + extracto de artículo
# ════════════════════════════════════════════════════════════════════════════

def _wikipedia_search(entity_name: str, lang: str = "es",
                      min_len: int = 200) -> list[dict]:
    """
    Busca la entidad en Wikipedia y devuelve el extracto del mejor artículo.
    Funciona para cualquier jugador, equipo, estadio o competición.
    """
    base = f"https://{lang}.wikipedia.org/w/api.php"
    entity_lower = entity_name.lower()

    # ── Paso 1: buscar el título del artículo más relevante ──
    try:
        resp = _sess().get(base, params={
            "action": "query", "list": "search",
            "srsearch": f"{entity_name} fútbol" if lang == "es" else f"{entity_name} football",
            "srlimit": 3, "format": "json", "utf8": 1,
        }, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("query", {}).get("search", [])
    except Exception as e:
        log.debug(f"[Wiki-{lang}] Search error: {e}")
        return []

    if not results:
        return []

    # Elegir el resultado más relevante (que contenga el nombre en el título)
    best_title = None
    for r in results:
        title_lower = r.get("title", "").lower()
        if any(part in title_lower for part in entity_lower.split() if len(part) > 3):
            best_title = r["title"]
            break
    if not best_title:
        best_title = results[0]["title"]

    # ── Paso 2: obtener extracto completo del artículo ──
    try:
        resp2 = _sess().get(base, params={
            "action": "query", "titles": best_title,
            "prop": "extracts", "exintro": False,
            "explaintext": True, "exsectionformat": "plain",
            "exchars": 4000,
            "format": "json", "redirects": 1, "utf8": 1,
        }, timeout=12)
        resp2.raise_for_status()
        pages = resp2.json().get("query", {}).get("pages", {})
    except Exception as e:
        log.debug(f"[Wiki-{lang}] Extract error: {e}")
        return []

    for pid, page in pages.items():
        if pid == "-1":
            continue
        text = page.get("extract", "").strip()
        if not text or len(text) < min_len:
            continue

        # Verificar que el artículo menciona la entidad
        if not any(part in text.lower()
                   for part in entity_lower.split() if len(part) > 3):
            continue

        title = page.get("title", entity_name)
        url   = f"https://{lang}.wikipedia.org/wiki/{title.replace(' ', '_')}"
        return [{
            "doc_id":       f"wiki_{lang}_{hashlib.md5(url.encode()).hexdigest()[:8]}",
            "chunk_id":     f"wiki_{lang}_{hashlib.md5(url.encode()).hexdigest()[:8]}_0",
            "text":         text[:3000],
            "url":          url,
            "source":       f"wikipedia_{lang}",
            "type":         "player",   # se refina abajo
            "entity":       entity_name,
            "score":        0.90,
            "vector_score": 0.0,
            "bm25_score":   0.0,
        }]

    return []


# ════════════════════════════════════════════════════════════════════════════
# 2. Wikidata — datos estructurados del jugador/equipo
# ════════════════════════════════════════════════════════════════════════════

def _wikidata_search(entity_name: str) -> list[dict]:
    """
    Busca en Wikidata y retorna datos estructurados (posición, club, nacimiento…).
    """
    try:
        # Paso 1: encontrar el QID de la entidad
        resp = _sess().get("https://www.wikidata.org/w/api.php", params={
            "action": "wbsearchentities", "search": entity_name,
            "language": "es", "format": "json", "type": "item", "limit": 3,
        }, timeout=10)
        resp.raise_for_status()
        hits = resp.json().get("search", [])
    except Exception as e:
        log.debug(f"[Wikidata] Search error: {e}")
        return []

    entity_lower = entity_name.lower()
    qid = None
    for h in hits:
        label = h.get("label", "").lower()
        desc  = h.get("description", "").lower()
        if any(part in label for part in entity_lower.split() if len(part) > 3):
            if any(kw in desc for kw in ["fútbol", "football", "soccer",
                                          "futbol", "jugador", "player", "club"]):
                qid = h.get("id")
                break
    if not qid and hits:
        # Si ninguno tiene desc de fútbol, tomar el primero con match en label
        for h in hits:
            if any(part in h.get("label", "").lower()
                   for part in entity_lower.split() if len(part) > 3):
                qid = h.get("id")
                break

    if not qid:
        return []

    # Paso 2: obtener datos de la entidad
    try:
        resp2 = _sess().get(f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
                            timeout=10)
        resp2.raise_for_status()
        entity_data = resp2.json().get("entities", {}).get(qid, {})
    except Exception as e:
        log.debug(f"[Wikidata] Entity fetch error for {qid}: {e}")
        return []

    claims = entity_data.get("claims", {})
    labels = entity_data.get("labels", {})
    label  = (labels.get("es") or labels.get("en") or {}).get("value", entity_name)

    def _claim_val(prop: str) -> str:
        """Extrae el valor string de un claim de Wikidata."""
        for c in claims.get(prop, []):
            mv = c.get("mainsnak", {}).get("datavalue", {}).get("value", "")
            if isinstance(mv, str) and mv:
                return mv
            if isinstance(mv, dict):
                # Es un item-id → intentar obtener label
                entity_id = mv.get("id", "")
                if entity_id:
                    try:
                        lr = _sess().get("https://www.wikidata.org/w/api.php", params={
                            "action": "wbgetentities", "ids": entity_id,
                            "props": "labels", "languages": "es|en", "format": "json",
                        }, timeout=5)
                        lr.raise_for_status()
                        elabels = lr.json().get("entities", {}).get(entity_id, {}).get("labels", {})
                        return (elabels.get("es") or elabels.get("en") or {}).get("value", "")
                    except Exception:
                        pass
        return ""

    parts = [f"{label} es un futbolista profesional."]

    # P27=nationality, P413=position, P54=club, P569=birthdate, P19=birthplace
    nat   = _claim_val("P27")
    pos   = _claim_val("P413")
    club  = _claim_val("P54")
    born  = _claim_val("P569")

    if nat:  parts.append(f"Nacionalidad: {nat}.")
    if pos:  parts.append(f"Posición: {pos}.")
    if club: parts.append(f"Club: {club}.")
    if born and isinstance(born, dict):
        born_str = born.get("time", "").lstrip("+").split("T")[0]
        if born_str:
            parts.append(f"Nacimiento: {born_str}.")
    elif born and isinstance(born, str) and born:
        parts.append(f"Nacimiento: {born}.")

    text = " ".join(p for p in parts if p)
    if len(text) < 60:
        return []

    url = f"https://www.wikidata.org/wiki/{qid}"
    return [{
        "doc_id":       f"wikidata_{qid}",
        "chunk_id":     f"wikidata_{qid}_0",
        "text":         text,
        "url":          url,
        "source":       "wikidata",
        "type":         "player",
        "entity":       entity_name,
        "score":        0.85,
        "vector_score": 0.0,
        "bm25_score":   0.0,
    }]


# ════════════════════════════════════════════════════════════════════════════
# 3. TheSportsDB — API gratuita de estadísticas
# ════════════════════════════════════════════════════════════════════════════

_TSDB = "https://www.thesportsdb.com/api/v1/json/3"

def _thesportsdb_search(entity_name: str) -> list[dict]:
    """Busca jugador o equipo en TheSportsDB."""
    chunks = []

    # Intentar como jugador
    try:
        r = _sess().get(f"{_TSDB}/searchplayers.php",
                        params={"p": entity_name}, timeout=8)
        r.raise_for_status()
        players = r.json().get("player") or []
        for p in players[:2]:
            name = p.get("strPlayer", "")
            if not name:
                continue
            parts = [f"{name} es un futbolista profesional."]
            nat  = p.get("strNationality", "")
            pos  = p.get("strPosition", "")
            team = p.get("strTeam", "")
            born = p.get("dateBorn", "")
            desc = (p.get("strDescriptionES") or p.get("strDescriptionEN") or "").strip()
            if nat:  parts.append(f"Nacionalidad: {nat}.")
            if pos:  parts.append(f"Posición: {pos}.")
            if team: parts.append(f"Club: {team}.")
            if born: parts.append(f"Nacimiento: {born}.")
            if desc: parts.append(desc[:800])
            text = " ".join(parts)
            if len(text) < 60:
                continue
            pid = p.get("idPlayer", "")
            chunks.append({
                "doc_id":       f"tsdb_p_{pid}",
                "chunk_id":     f"tsdb_p_{pid}_0",
                "text":         text,
                "url":          f"https://www.thesportsdb.com/player.php?id={pid}",
                "source":       "thesportsdb",
                "type":         "player",
                "entity":       name,
                "score":        0.88,
                "vector_score": 0.0,
                "bm25_score":   0.0,
            })
    except Exception as e:
        log.debug(f"[TSDB] Player search error: {e}")

    # Intentar como equipo si no se encontró como jugador
    if not chunks:
        try:
            r = _sess().get(f"{_TSDB}/searchteams.php",
                            params={"t": entity_name}, timeout=8)
            r.raise_for_status()
            teams = r.json().get("teams") or []
            for t in teams[:1]:
                name = t.get("strTeam", "")
                if not name:
                    continue
                parts = [f"{name} es un club de fútbol profesional."]
                country = t.get("strCountry", "")
                league  = t.get("strLeague", "")
                founded = t.get("intFormedYear", "")
                stadium = t.get("strStadium", "")
                desc    = (t.get("strDescriptionES") or t.get("strDescriptionEN") or "").strip()
                if country:  parts.append(f"País: {country}.")
                if league:   parts.append(f"Liga: {league}.")
                if founded:  parts.append(f"Fundado en: {founded}.")
                if stadium:  parts.append(f"Estadio: {stadium}.")
                if desc:     parts.append(desc[:800])
                text = " ".join(parts)
                if len(text) < 60:
                    continue
                tid = t.get("idTeam", "")
                chunks.append({
                    "doc_id":       f"tsdb_t_{tid}",
                    "chunk_id":     f"tsdb_t_{tid}_0",
                    "text":         text,
                    "url":          f"https://www.thesportsdb.com/team.php?id={tid}",
                    "source":       "thesportsdb",
                    "type":         "club",
                    "entity":       name,
                    "score":        0.88,
                    "vector_score": 0.0,
                    "bm25_score":   0.0,
                })
        except Exception as e:
            log.debug(f"[TSDB] Team search error: {e}")

    return chunks


# ════════════════════════════════════════════════════════════════════════════
# 4. Common Crawl — último recurso (lento, ~20s)
# ════════════════════════════════════════════════════════════════════════════

def _cc_search(entity_name: str, max_results: int = 3) -> list[dict]:
    """Búsqueda en Common Crawl como último recurso."""
    try:
        from ingest import search_entity_cc
        docs = search_entity_cc(entity_name, max_results=max_results, timeout_secs=20)
        chunks = []
        for i, doc in enumerate(docs):
            text = doc.get("text", "")
            if not text:
                continue
            chunks.append({
                "doc_id":       f"cc_live_{i}",
                "chunk_id":     f"cc_live_{i}_0",
                "text":         text,
                "url":          doc.get("url", ""),
                "source":       "cc_live",
                "type":         doc.get("type", "concept"),
                "entity":       doc.get("entity", entity_name),
                "domain":       doc.get("domain", ""),
                "score":        0.80,
                "vector_score": 0.0,
                "bm25_score":   0.0,
            })
        return chunks
    except Exception as e:
        log.debug(f"[CC-Live] Error: {e}")
        return []


# ════════════════════════════════════════════════════════════════════════════
# API pública
# ════════════════════════════════════════════════════════════════════════════

def live_search(entity_name: str, max_results: int = 5) -> list[dict]:
    """
    Búsqueda multi-fuente en tiempo real.
    Prioridad: Wikipedia ES → Wikipedia EN → Wikidata → TheSportsDB → CC
    """
    chunks: list[dict] = []
    seen_urls: set[str] = set()

    def _add(new: list[dict]):
        for c in new:
            url = c.get("url", "")
            key = url or c.get("text", "")[:80]
            if key and key not in seen_urls:
                seen_urls.add(key)
                chunks.append(c)

    log.info(f"[Live] Buscando: '{entity_name}'")

    # 1. Wikipedia ES (más completa para jugadores latinoamericanos)
    _add(_wikipedia_search(entity_name, lang="es"))
    if len(chunks) >= max_results:
        return chunks[:max_results]

    # 2. Wikipedia EN
    _add(_wikipedia_search(entity_name, lang="en"))
    if len(chunks) >= max_results:
        return chunks[:max_results]

    # 3. Wikidata (datos estructurados)
    _add(_wikidata_search(entity_name))
    if len(chunks) >= max_results:
        return chunks[:max_results]

    # 4. TheSportsDB (API gratuita)
    _add(_thesportsdb_search(entity_name))
    if len(chunks) >= max_results:
        return chunks[:max_results]

    # 5. Common Crawl (lento — solo si las anteriores fallaron)
    if not chunks:
        log.info(f"[Live] Fallback a Common Crawl para '{entity_name}'...")
        _add(_cc_search(entity_name, max_results=max_results))

    log.info(f"[Live] '{entity_name}' → {len(chunks)} chunks "
             f"de fuentes: {list({c['source'] for c in chunks})}")
    return chunks[:max_results]


def live_search_cached(entity_name: str, max_results: int = 5) -> list[dict]:
    """Búsqueda con cache TTL de 1 hora."""
    key = entity_name.lower().strip()
    now = time.time()

    if key in _cache:
        ts, cached = _cache[key]
        if now - ts < _CACHE_TTL:
            remaining = int((_CACHE_TTL - (now - ts)) / 60)
            log.info(f"[Live-Cache] HIT '{entity_name}' "
                     f"({len(cached)} chunks, {remaining}min restantes)")
            return cached

    chunks = live_search(entity_name, max_results=max_results)
    _cache[key] = (now, chunks)
    return chunks


def invalidate(entity_name: Optional[str] = None):
    """Invalida la cache (total o para una entidad)."""
    global _cache
    if entity_name:
        _cache.pop(entity_name.lower().strip(), None)
    else:
        _cache = {}


def cache_stats() -> dict:
    now = time.time()
    entries = []
    for key, (ts, chunks) in _cache.items():
        age = int(now - ts)
        entries.append({
            "entity":   key,
            "chunks":   len(chunks),
            "age_s":    age,
            "valid":    age < _CACHE_TTL,
            "ttl_left": max(0, _CACHE_TTL - age),
        })
    valid = sum(1 for e in entries if e["valid"])
    return {"total": len(entries), "valid": valid,
            "expired": len(entries) - valid, "entries": entries}
