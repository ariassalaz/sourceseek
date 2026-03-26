"""
SourceSeek - Motor RAG Mejorado
Recuperación híbrida BM25 + vectores semánticos + RRF + cross-encoder + CC live.
"""

import logging
import re
import time
from pathlib import Path
from typing import Optional

import numpy as np
import requests
from rank_bm25 import BM25Okapi

from indexer import load_index, get_model

# Cross-encoder (carga lazy para no bloquear el arranque del servidor)
_cross_encoder = None

def _get_cross_encoder():
    global _cross_encoder
    if _cross_encoder is None:
        try:
            from sentence_transformers import CrossEncoder
            _cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2", max_length=512)
            log.info("Cross-encoder cargado: ms-marco-MiniLM-L-6-v2")
        except Exception as e:
            log.warning(f"Cross-encoder no disponible: {e}")
            _cross_encoder = False  # marcar como no disponible
    return _cross_encoder if _cross_encoder else None

log = logging.getLogger(__name__)

OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3"
TOP_K        = 7

# ── Expansión de consulta ────────────────────────────────────────────────────
QUERY_EXPANSIONS = {
    "messi":              ["lionel messi", "leo messi", "argentina", "inter miami", "balón de oro", "fcb"],
    "ronaldo":            ["cristiano ronaldo", "cr7", "portugal", "real madrid", "juventus", "al nassr"],
    "mbappe":             ["kylian mbappé", "mbappé", "francia", "real madrid", "psg", "paris saint-germain"],
    "mbappé":             ["kylian mbappé", "mbappé", "francia", "real madrid", "psg"],
    "haaland":            ["erling haaland", "manchester city", "noruega", "premier league"],
    "vinicius":           ["vinícius júnior", "vinicius jr", "real madrid", "brasil", "extremo"],
    "bellingham":         ["jude bellingham", "real madrid", "inglaterra", "mediocampista"],
    "lamine yamal":       ["lamine yamal", "barcelona", "españa", "extremo", "eurocopa 2024"],
    "pedri":              ["pedri", "pedro gonzález", "barcelona", "españa", "mediocampista"],
    "rodri":              ["rodri", "rodrigo hernández", "manchester city", "españa", "balón de oro 2024"],
    "champions":          ["champions league", "liga de campeones", "copa de europa", "ucl", "uefa"],
    "mundial":            ["copa del mundo", "world cup", "fifa world cup", "selección nacional"],
    "premier":            ["premier league", "primera división inglesa", "england football"],
    "laliga":             ["laliga", "primera división", "liga española", "liga santander"],
    "bundesliga":         ["bundesliga", "liga alemana", "primera división alemana"],
    "var":                ["var", "video assistant referee", "videoarbitraje", "árbitro de video"],
    "offside":            ["offside", "fuera de juego", "posición adelantada"],
    "penalti":            ["penalti", "penalty", "penal", "tiro penal", "punto penal"],
    "balon de oro":       ["balón de oro", "ballon d'or", "mejor jugador del año"],
    "tiki":               ["tiki-taka", "posesión", "barcelona", "guardiola", "pase corto"],
    "guardiola":          ["pep guardiola", "manchester city", "barcelona", "tiki-taka"],
    "copa america":       ["copa america", "conmebol", "argentina", "brasil", "selecciones sudamericanas"],
    "liga mx":            ["liga mx", "futbol mexicano", "torneo apertura", "torneo clausura"],
    "real madrid":        ["real madrid", "merengues", "bernabéu", "hala madrid"],
    "barcelona":          ["fc barcelona", "barça", "blaugrana", "camp nou", "la masia"],
    # Jugadores mexicanos
    "jordan carrillo":    ["jordan carrillo", "pumas unam", "unam", "mediocampista", "mexico", "liga mx"],
    "santiago gimenez":   ["santiago giménez", "feyenoord", "delantero", "mexico", "seleccion mexicana"],
    "giménez":            ["santiago giménez", "feyenoord", "delantero", "mexico"],
    "edson alvarez":      ["edson álvarez", "west ham", "mediocampista", "mexico", "seleccion mexicana"],
    "memo ochoa":         ["guillermo ochoa", "portero", "mexico", "seleccion mexicana"],
    "chucky lozano":      ["hirving lozano", "chucky", "mexico", "extremo", "psv napoli"],
    "chicharito":         ["javier hernández", "chicharito", "delantero", "mexico", "real madrid man utd"],
    "raul jimenez":       ["raúl jiménez", "wolverhampton", "delantero", "mexico"],
    "carlos vela":        ["carlos vela", "lafc", "extremo", "mexico", "mls"],
    "henry martin":       ["henry martín", "club america", "delantero", "mexico", "liga mx"],
    "roberto alvarado":   ["roberto alvarado", "guadalajara chivas", "mediocampista", "mexico"],
    "cesar montes":       ["césar montes", "espanyol", "defensa", "mexico", "monterrey"],
    "pumas":              ["pumas unam", "club universidad nacional", "liga mx", "mexico", "pedregal"],
    "unam":               ["pumas unam", "club universidad nacional", "liga mx", "mexico"],
    "tigres":             ["tigres uanl", "nuevo leon", "liga mx", "mexico", "monterrey"],
    "pachuca":            ["club pachuca", "liga mx", "hidalgo", "mexico", "tuzos"],
    "atlas":              ["atlas fc", "guadalajara", "jalisco", "liga mx", "zorros"],
    "toluca":             ["deportivo toluca", "liga mx", "estado de mexico", "diablos rojos"],
}

def expand_query(query: str) -> str:
    """Expande la consulta con términos relacionados."""
    q_lower = query.lower()
    for key, terms in QUERY_EXPANSIONS.items():
        if key in q_lower:
            return query + " " + " ".join(terms)
    return query


# ── Detección de entidad específica ─────────────────────────────────────────
_ENTITY_STOPWORDS = {
    'quien', 'quién', 'qué', 'que', 'cómo', 'como', 'cuándo', 'cuando',
    'dónde', 'donde', 'cuál', 'cual', 'cuánto', 'cuanto', 'es', 'fue',
    'son', 'era', 'será', 'hay', 'tiene', 'tenía', 'hace', 'hizo',
    'juega', 'jugó', 'ganó', 'gana', 'marcó', 'marca', 'juegan',
    'información', 'info', 'datos', 'sobre', 'acerca', 'del', 'me',
    'puedes', 'dime', 'habla', 'cuéntame', 'explica', 'busco',
    'mejor', 'mejores', 'quiero', 'saber', 'conocer', 'perfil',
    'historia', 'un', 'una', 'el', 'la', 'los', 'las',
    'the', 'who', 'what', 'how', 'when', 'where', 'which', 'tell',
    'know', 'about', 'and', 'or', 'is', 'was', 'are', 'were',
}


def _extract_entity_name(query: str) -> Optional[str]:
    """
    Extrae la entidad principal de la consulta.
    Retorna el nombre si detecta una consulta de entidad específica,
    o None si es una consulta genérica.
    """
    q = query.strip().rstrip('?.!')

    # Patrones directos de pregunta sobre entidad
    for pattern in [
        r'(?:quién es|quien es|quién fue|quien fue|información sobre|info sobre|'
        r'info de|habla(?:me)? de|cuéntame? sobre|dime sobre|todo sobre|'
        r'qué sabes de|que sabes de|datos de|perfil de|historia de|'
        r'búscame|busca|búsqueda de)\s+(.+)',
    ]:
        m = re.search(pattern, q, re.IGNORECASE)
        if m:
            entity = m.group(1).strip()
            entity = re.sub(
                r'\s+(es|fue|era|son|está|juega|jugó)\s*$', '',
                entity, flags=re.IGNORECASE
            ).strip()
            if len(entity) >= 3:
                return entity

    # Consulta corta (1-4 palabras): verificar si todas son palabras de contenido
    words = q.split()
    content_words = [w for w in words if w.lower() not in _ENTITY_STOPWORDS and len(w) >= 2]
    if 1 <= len(content_words) <= 4 and len(content_words) == len(words):
        entity = ' '.join(content_words)
        if len(entity) >= 3:
            return entity

    return None


# ── Tokenización para BM25 ───────────────────────────────────────────────────
_STOPWORDS = {
    "el","la","los","las","un","una","unos","unas","de","del","al","en","a",
    "y","o","que","se","es","su","por","con","para","no","lo","le","más",
    "pero","como","si","este","esta","estos","estas","fue","era","son","son",
    "the","a","an","in","of","to","and","for","with","on","at","from","by",
    "this","that","are","was","were","be","has","have","is","he","she","it",
}

def tokenize(text: str) -> list[str]:
    tokens = re.findall(r'\b[a-záéíóúüñàèìòùâêîôûçäëïöü]{2,}\b', text.lower())
    return [t for t in tokens if t not in _STOPWORDS]


# ════════════════════════════════════════════════════════════════════════════
# 1. RECUPERACIÓN HÍBRIDA BM25 + VECTORIAL
# ════════════════════════════════════════════════════════════════════════════

class HybridRetriever:
    """Combina BM25 y búsqueda vectorial FAISS para mayor precisión."""

    def __init__(self, index, chunks: list[dict]):
        self.index  = index
        self.chunks = chunks
        self._bm25  = None
        self._build_bm25()

    def _build_bm25(self):
        corpus = [tokenize(c["text"]) for c in self.chunks]
        self._bm25 = BM25Okapi(corpus)
        log.info(f"BM25 construido: {len(self.chunks)} chunks")

    def retrieve(self, query: str, top_k: int = TOP_K,
                 min_score: float = 0.18,
                 type_filter: Optional[str] = None) -> list[dict]:
        """
        Recuperación híbrida con:
          - Reciprocal Rank Fusion (RRF) para combinar BM25 y vectores
          - Re-ranking por entidad específica (boost/penalización)
          - Cross-encoder reranking opcional sobre los mejores candidatos
        """
        expanded = expand_query(query)
        entity = _extract_entity_name(query)

        # ── Scores vectoriales ──
        model = get_model()
        q_vec = model.encode([expanded], normalize_embeddings=True).astype("float32")
        n = min(top_k * 8, self.index.ntotal)
        vec_scores, vec_idxs = self.index.search(q_vec, n)

        # Ranking vectorial: idx → rank (1-based)
        vec_rank: dict[int, int] = {}
        for rank, (score, idx) in enumerate(zip(vec_scores[0], vec_idxs[0]), 1):
            if idx >= 0:
                vec_rank[int(idx)] = rank
        vec_score_map: dict[int, float] = {int(i): float(s)
                                            for s, i in zip(vec_scores[0], vec_idxs[0])
                                            if int(i) >= 0}

        # ── Scores BM25 ──
        tokens = tokenize(query)
        if tokens:
            bm25_raw = self._bm25.get_scores(tokens)
            bm25_max = bm25_raw.max() if bm25_raw.max() > 0 else 1.0
            bm25_norm = bm25_raw / bm25_max
        else:
            bm25_norm = np.zeros(len(self.chunks))

        # Ranking BM25: idx → rank (1-based, mejores primero)
        top_bm25_idxs = np.argsort(bm25_norm)[::-1][: top_k * 4].tolist()
        bm25_rank: dict[int, int] = {int(idx): rank + 1
                                      for rank, idx in enumerate(top_bm25_idxs)}

        # ── Reciprocal Rank Fusion ──
        RRF_K = 60
        candidates = set(vec_rank.keys()) | set(bm25_rank.keys())
        scored: dict[int, tuple] = {}
        for idx in candidates:
            r_v = vec_rank.get(idx, len(self.chunks) + 1)
            r_b = bm25_rank.get(idx, len(self.chunks) + 1)
            rrf  = 1.0 / (RRF_K + r_v) + 1.0 / (RRF_K + r_b)
            v    = vec_score_map.get(idx, 0.0)
            b    = float(bm25_norm[idx]) if idx < len(bm25_norm) else 0.0
            scored[idx] = (rrf, v, b)

        # ── Re-ranking por entidad específica ──────────────────────────────
        if entity:
            entity_lower = entity.lower()
            entity_parts = [p for p in entity_lower.split() if len(p) > 3]
            boosted: dict[int, tuple] = {}
            for idx, (rrf, v, b) in scored.items():
                if idx < 0 or idx >= len(self.chunks):
                    continue
                chunk_text   = self.chunks[idx].get("text", "").lower()
                chunk_entity = self.chunks[idx].get("entity", "").lower()

                if entity_lower in chunk_text or entity_lower in chunk_entity:
                    factor = 1.6   # coincidencia exacta
                elif entity_parts and (
                    any(p in chunk_text   for p in entity_parts) or
                    any(p in chunk_entity for p in entity_parts)
                ):
                    factor = 1.25  # coincidencia parcial (apellido)
                else:
                    factor = 0.55  # sin relación: penalización
                boosted[idx] = (rrf * factor, v, b)
            scored = boosted

        # ── Filtrado y selección preliminar ──
        pre_results = []
        seen_docs: dict[int, int] = {}
        for idx in sorted(scored, key=lambda i: scored[i][0], reverse=True):
            if idx < 0 or idx >= len(self.chunks):
                continue
            rrf_score, v_score, b_score = scored[idx]
            if rrf_score < min_score / 30 and v_score < min_score:
                continue  # umbral adaptado a escala RRF

            chunk = self.chunks[idx].copy()
            chunk["score"]        = round(rrf_score, 6)
            chunk["vector_score"] = round(v_score, 4)
            chunk["bm25_score"]   = round(b_score, 4)

            if type_filter and chunk.get("type") != type_filter:
                continue

            doc_id = chunk["doc_id"]
            if seen_docs.get(doc_id, 0) >= 2:
                continue
            seen_docs[doc_id] = seen_docs.get(doc_id, 0) + 1

            pre_results.append(chunk)
            if len(pre_results) >= top_k * 3:   # pool amplio para cross-encoder
                break

        # ── Cross-encoder reranking (solo cuando hay entidad detectada) ──
        if entity and len(pre_results) > top_k:
            ce = _get_cross_encoder()
            if ce is not None:
                try:
                    pairs = [(query, c["text"][:400]) for c in pre_results]
                    ce_scores = ce.predict(pairs, show_progress_bar=False)
                    for chunk, ce_score in zip(pre_results, ce_scores):
                        chunk["score"] = round(float(ce_score), 4)
                    pre_results.sort(key=lambda c: c["score"], reverse=True)
                    log.info(f"Cross-encoder aplicado: {len(pre_results)} candidatos reordenados")
                except Exception as e:
                    log.warning(f"Cross-encoder falló, usando RRF: {e}")

        results = pre_results[:top_k]
        log.info(
            f"Híbrido+RRF: {len(results)} chunks para: '{query}' "
            f"(entidad: '{entity}', expandido: '{expanded[:50]}')"
        )
        return results


# ════════════════════════════════════════════════════════════════════════════
# 2. GENERACIÓN
# ════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """Eres SourceSeek, un asistente experto en fútbol soccer mundial.
Responde SIEMPRE en base al contexto proporcionado.
- Responde en el mismo idioma de la pregunta (español o inglés).
- Sé informativo, preciso y detallado. Incluye estadísticas y fechas cuando estén disponibles.
- Si el contexto menciona estadísticas, fechas, nombres o datos relevantes, inclúyelos en la respuesta.
- Si no hay suficiente información en el contexto, indícalo claramente.
- Estructura la respuesta en párrafos claros y fluidos.
- No inventes información que no esté en el contexto.
"""

def build_prompt(query: str, chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        src = c.get("url", "?")
        source_type = c.get("source", "")
        parts.append(f"[Fuente {i} | {source_type} | {src}]\n{c['text']}")
    ctx = "\n\n---\n\n".join(parts)
    return f"""{SYSTEM_PROMPT}

### Contexto recuperado:
{ctx}

### Pregunta del usuario:
{query}

### Respuesta:"""


def generate_with_ollama(prompt: str) -> Optional[str]:
    try:
        resp = requests.post(OLLAMA_URL, json={
            "model":   OLLAMA_MODEL,
            "prompt":  prompt,
            "stream":  False,
            "options": {"temperature": 0.3, "top_p": 0.9, "num_predict": 800},
        }, timeout=120)
        resp.raise_for_status()
        return resp.json().get("response", "").strip() or None
    except Exception:
        return None


def generate_direct(query: str, chunks: list[dict]) -> str:
    """
    Respuesta directa desde los chunks cuando no hay LLM.
    Filtra por entidad específica para evitar deriva temática.
    """
    if not chunks:
        return "No se encontró información relevante sobre tu consulta."

    entity = _extract_entity_name(query)
    selected = []

    if entity:
        entity_lower = entity.lower()
        entity_parts = [p for p in entity_lower.split() if len(p) > 3]

        # Filtrar chunks que mencionan explícitamente la entidad buscada
        primary = [c for c in chunks if entity_lower in c["text"].lower()]
        if not primary and entity_parts:
            primary = [c for c in chunks
                       if any(p in c["text"].lower() for p in entity_parts)]

        if primary:
            selected = primary[:3]
        else:
            # La entidad no aparece en ningún chunk recuperado
            return (f"No se encontró información específica sobre '{entity}' "
                    f"en la base de conocimiento de SourceSeek. "
                    f"Prueba reindexar con /api/rebuild para incluir más fuentes.")
    else:
        selected = chunks[:3]

    parts = []
    for c in selected:
        text = c["text"]
        sentences = re.split(r'(?<=[.!?])\s+', text)
        # Tomar oraciones informativas (>5 palabras)
        relevant = [s for s in sentences[:8] if len(s.split()) > 5]
        snippet = " ".join(relevant[:6])
        if snippet and len(snippet) > 60:
            if not parts:
                snippet = re.sub(r'^[^:]+:\s+', '', snippet, count=1)
            parts.append(snippet)

    combined = "\n\n".join(parts)
    return combined if combined else chunks[0]["text"][:1000]


# ════════════════════════════════════════════════════════════════════════════
# 3. MOTOR RAG
# ════════════════════════════════════════════════════════════════════════════

class SourceSeekRAG:
    def __init__(self):
        self.index     = None
        self.chunks    = None
        self.retriever = None
        self._loaded   = False

    def load(self):
        if not self._loaded:
            self.index, self.chunks = load_index()
            self.retriever = HybridRetriever(self.index, self.chunks)
            self._loaded = True

    def search(self, query: str, top_k: int = TOP_K,
               use_llm: bool = True,
               type_filter: Optional[str] = None) -> dict:
        self.load()
        t0 = time.time()
        cc_live_used = False

        retrieved = self.retriever.retrieve(query, top_k=top_k,
                                            type_filter=type_filter)

        # ── Verificar entidad y activar fallback CC live si no hay match ──
        entity = _extract_entity_name(query)
        if entity:
            entity_lower = entity.lower()
            entity_parts = [p for p in entity_lower.split() if len(p) > 3]

            def _has_entity_match(chunks: list[dict]) -> bool:
                return any(
                    entity_lower in c["text"].lower() or
                    entity_lower in c.get("entity", "").lower() or
                    (entity_parts and any(p in c["text"].lower() for p in entity_parts))
                    for c in chunks
                )

            if not retrieved or not _has_entity_match(retrieved):
                # Entidad no en índice → búsqueda en vivo CC
                log.info(f"Entidad '{entity}' no en índice, activando CC live...")
                try:
                    from cc_live import live_search_cached
                    live_chunks = live_search_cached(entity, max_results=5)
                    if live_chunks:
                        retrieved = live_chunks
                        cc_live_used = True
                        log.info(f"CC live: {len(live_chunks)} chunks para '{entity}'")
                except Exception as e:
                    log.warning(f"CC live fallback falló: {e}")

            # Si aún no hay resultados con match, devolver mensaje claro
            if not retrieved or not _has_entity_match(retrieved):
                return {
                    "query":        query,
                    "answer":       (f"No se encontró información específica sobre '{entity}'. "
                                     f"Prueba reindexar con /api/rebuild para ampliar las fuentes."),
                    "chunks":       [],
                    "sources":      [],
                    "used_llm":     False,
                    "cc_live_used": cc_live_used,
                    "search_ms":    round((time.time() - t0) * 1000),
                }

            # Filtrar chunks para el LLM: solo los que mencionan la entidad
            if use_llm:
                llm_chunks = [c for c in retrieved
                              if entity_lower in c["text"].lower() or
                              (entity_parts and any(p in c["text"].lower() for p in entity_parts))]
                retrieved_for_llm = llm_chunks if llm_chunks else retrieved
            else:
                retrieved_for_llm = retrieved
        else:
            if not retrieved:
                return {
                    "query":        query,
                    "answer":       "No se encontró información relevante.",
                    "chunks":       [],
                    "sources":      [],
                    "used_llm":     False,
                    "cc_live_used": False,
                    "search_ms":    round((time.time() - t0) * 1000),
                }
            retrieved_for_llm = retrieved

        answer   = None
        used_llm = False
        if use_llm:
            answer = generate_with_ollama(build_prompt(query, retrieved_for_llm))
            if answer:
                used_llm = True
        if not answer:
            answer = generate_direct(query, retrieved)

        sources = list(dict.fromkeys(
            c["url"] for c in retrieved if c.get("url", "").startswith("http")
        ))

        return {
            "query":        query,
            "answer":       answer,
            "chunks":       retrieved,
            "sources":      sources,
            "used_llm":     used_llm,
            "cc_live_used": cc_live_used,
            "search_ms":    round((time.time() - t0) * 1000),
        }


_engine: Optional[SourceSeekRAG] = None

def get_rag_engine() -> SourceSeekRAG:
    global _engine
    if _engine is None:
        _engine = SourceSeekRAG()
        _engine.load()
    return _engine


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    e = get_rag_engine()
    for q in ["¿Quién es Mbappé?", "Champions League títulos Real Madrid",
              "¿Qué es el tiki-taka?", "Goles de Haaland en Premier League"]:
        r = e.search(q)
        print(f"\n{'─'*60}\nQ: {q}\nA: {r['answer'][:400]}\n")
