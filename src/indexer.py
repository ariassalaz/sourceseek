"""
SourceSeek - Módulo de Indexación
Segmenta documentos en chunks, genera embeddings y construye índice FAISS.
También exporta el grafo de conocimiento en formato N-Quads.
"""

import os
import json
import logging
import pickle
import re
from pathlib import Path
from typing import Optional

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
from rdflib import ConjunctiveGraph, URIRef, Literal, Namespace, Graph
from rdflib.namespace import RDF, RDFS, DCTERMS, SKOS, XSD, OWL

log = logging.getLogger(__name__)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# ── Modelo de embeddings ─────────────────────────────────────────────────────
# paraphrase-multilingual-MiniLM-L12-v2 → soporta español e inglés, 384 dims
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIM   = 384

# ── Namespaces RDF ────────────────────────────────────────────────────────────
SS      = Namespace("http://sourceseek.io/ontology#")
SS_DATA = Namespace("http://sourceseek.io/data/")
SCHEMA  = Namespace("http://schema.org/")
PROV    = Namespace("http://www.w3.org/ns/prov#")


# ════════════════════════════════════════════════════════════════════════════
# 1. SEGMENTACIÓN EN CHUNKS
# ════════════════════════════════════════════════════════════════════════════

def split_into_chunks(text: str, chunk_size: int = 400, overlap: int = 80) -> list[str]:
    """
    Divide texto en fragmentos con solapamiento para preservar contexto.
    Prioriza divisiones en oraciones completas.
    """
    # Dividir en oraciones
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    chunks  = []
    current = []
    current_len = 0

    for sent in sentences:
        sent_len = len(sent.split())
        if current_len + sent_len > chunk_size and current:
            chunk_text = " ".join(current)
            if len(chunk_text.strip()) > 50:
                chunks.append(chunk_text.strip())
            # Mantener solapamiento
            overlap_sents = []
            overlap_len   = 0
            for s in reversed(current):
                w = len(s.split())
                if overlap_len + w <= overlap:
                    overlap_sents.insert(0, s)
                    overlap_len += w
                else:
                    break
            current     = overlap_sents + [sent]
            current_len = overlap_len + sent_len
        else:
            current.append(sent)
            current_len += sent_len

    if current:
        chunk_text = " ".join(current)
        if len(chunk_text.strip()) > 50:
            chunks.append(chunk_text.strip())

    return chunks if chunks else [text[:2000]]


def prepare_chunks(documents: list[dict]) -> list[dict]:
    """
    Convierte documentos en chunks etiquetados con metadatos.
    """
    all_chunks = []
    for doc_id, doc in enumerate(documents):
        text   = doc.get("text", "")
        url    = doc.get("url", f"doc_{doc_id}")
        source = doc.get("source", "unknown")
        doc_type = doc.get("type", "document")

        chunks = split_into_chunks(text)
        for chunk_id, chunk_text in enumerate(chunks):
            all_chunks.append({
                "chunk_id":  f"{doc_id}_{chunk_id}",
                "doc_id":    doc_id,
                "text":      chunk_text,
                "url":       url,
                "source":    source,
                "type":      doc_type,
                "entity":    doc.get("entity", ""),
            })

    log.info(f"Chunks generados: {len(all_chunks)} de {len(documents)} documentos")
    return all_chunks


# ════════════════════════════════════════════════════════════════════════════
# 2. GENERACIÓN DE EMBEDDINGS
# ════════════════════════════════════════════════════════════════════════════

_model_cache: Optional[SentenceTransformer] = None

def get_model() -> SentenceTransformer:
    global _model_cache
    if _model_cache is None:
        log.info(f"Cargando modelo de embeddings: {EMBEDDING_MODEL}")
        _model_cache = SentenceTransformer(EMBEDDING_MODEL)
    return _model_cache


def generate_embeddings(chunks: list[dict], batch_size: int = 64) -> np.ndarray:
    """Genera embeddings para todos los chunks en batches."""
    model  = get_model()
    texts  = [c["text"] for c in chunks]
    log.info(f"Generando embeddings para {len(texts)} chunks...")

    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,   # normalizar para cosine similarity
    )
    log.info(f"Embeddings generados: shape={embeddings.shape}")
    return embeddings.astype("float32")


# ════════════════════════════════════════════════════════════════════════════
# 3. ÍNDICE FAISS
# ════════════════════════════════════════════════════════════════════════════

def build_faiss_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    """
    Construye índice FAISS con Inner Product (equivalente a cosine similarity
    cuando los vectores están normalizados).
    """
    dim   = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)
    log.info(f"Índice FAISS construido: {index.ntotal} vectores, dim={dim}")
    return index


def save_index(index: faiss.IndexFlatIP, chunks: list[dict]) -> None:
    """Guarda el índice FAISS y los metadatos de chunks en disco."""
    faiss.write_index(index, str(DATA_DIR / "football.index"))
    with open(DATA_DIR / "chunks.pkl", "wb") as f:
        pickle.dump(chunks, f)
    log.info("Índice y chunks guardados en disco")


def load_index() -> tuple[faiss.IndexFlatIP, list[dict]]:
    """Carga el índice FAISS y los chunks desde disco."""
    index_path = DATA_DIR / "football.index"
    chunks_path = DATA_DIR / "chunks.pkl"

    if not index_path.exists() or not chunks_path.exists():
        raise FileNotFoundError("Índice no encontrado. Ejecuta primero: python src/indexer.py")

    index = faiss.read_index(str(index_path))
    with open(chunks_path, "rb") as f:
        chunks = pickle.load(f)
    log.info(f"Índice cargado: {index.ntotal} vectores, {len(chunks)} chunks")
    return index, chunks


# ════════════════════════════════════════════════════════════════════════════
# 4. EXPORTAR GRAFO DE CONOCIMIENTO EN N-QUADS
# ════════════════════════════════════════════════════════════════════════════

def build_knowledge_graph(documents: list[dict], chunks: list[dict]) -> ConjunctiveGraph:
    """
    Construye un grafo de conocimiento RDF con los documentos indexados.
    Cada documento y chunk se representan como nodos con sus relaciones.
    Exportable en N-Quads.
    """
    g = ConjunctiveGraph()

    # Registrar namespaces
    g.bind("ss",      SS)
    g.bind("ss-data", SS_DATA)
    g.bind("schema",  SCHEMA)
    g.bind("dcterms", DCTERMS)
    g.bind("prov",    PROV)
    g.bind("skos",    SKOS)
    g.bind("owl",     OWL)

    # Grafo nombrado para el corpus de fútbol
    corpus_graph = URIRef("http://sourceseek.io/graph/football-corpus")

    # ── Documentos ────────────────────────────────────────────────────────
    for i, doc in enumerate(documents):
        doc_uri = SS_DATA[f"document/{i}"]
        url     = doc.get("url", "")
        source  = doc.get("source", "unknown")
        doc_type = doc.get("type", "document")
        text    = doc.get("text", "")

        # Tipo RDF
        g.add((doc_uri, RDF.type, SS.Document, corpus_graph))

        # Tipo de entidad
        type_map = {
            "player":       SCHEMA.Person,
            "club":         SCHEMA.SportsTeam,
            "competition":  SCHEMA.SportsEvent,
            "rule":         SS.Rule,
            "concept":      SCHEMA.Thing,
            "tactic":       SS.Tactic,
            "technology":   SS.Technology,
            "manager":      SCHEMA.Person,
            "national_team": SCHEMA.SportsTeam,
            "stadium":      SCHEMA.CivicStructure,
            "world_cup":    SCHEMA.SportsEvent,
            "award":        SCHEMA.Award,
            "wikipedia":    SCHEMA.Article,
        }
        rdf_type = type_map.get(doc_type, SCHEMA.Thing)
        g.add((doc_uri, RDF.type, rdf_type, corpus_graph))

        # Propiedades
        if url:
            g.add((doc_uri, DCTERMS.source, URIRef(url) if url.startswith("http") else Literal(url), corpus_graph))
        g.add((doc_uri, DCTERMS.description, Literal(text[:500], lang="es"), corpus_graph))
        g.add((doc_uri, SS.dataSource, Literal(source), corpus_graph))

        # Extraer nombre de la entidad
        entity_name = doc.get("entity", "")
        if entity_name:
            g.add((doc_uri, RDFS.label, Literal(entity_name.replace("_", " ")), corpus_graph))
        elif url.startswith("http"):
            label = url.rstrip("/").split("/")[-1].replace("_", " ")
            g.add((doc_uri, RDFS.label, Literal(label), corpus_graph))

        # Provenance
        g.add((doc_uri, PROV.wasAttributedTo, SS_DATA["agent/sourceseek"], corpus_graph))

        # ── owl:sameAs cross-links ─────────────────────────────────────
        # Si la URL es de DBpedia, enlazar con Wikidata y Wikipedia equivalentes
        if url.startswith("http://dbpedia.org/resource/"):
            entity_name = url.replace("http://dbpedia.org/resource/", "")
            wiki_uri = URIRef(f"https://en.wikipedia.org/wiki/{entity_name}")
            g.add((doc_uri, OWL.sameAs, wiki_uri, corpus_graph))
        elif url.startswith("http://www.wikidata.org/entity/"):
            # Wikidata entidad
            g.add((doc_uri, DCTERMS.isPartOf, URIRef("http://www.wikidata.org/"), corpus_graph))
        elif "wikipedia.org/wiki/" in url:
            # Wikipedia → intentar enlazar con DBpedia
            entity_name = url.rstrip("/").split("/wiki/")[-1]
            dbpedia_uri = URIRef(f"http://dbpedia.org/resource/{entity_name}")
            g.add((doc_uri, OWL.sameAs, dbpedia_uri, corpus_graph))

        # ── Lenguaje del documento ──
        if source in ("wikipedia_es", "dbpedia_sparql") or "_es" in source:
            g.add((doc_uri, DCTERMS.language, Literal("es"), corpus_graph))
        elif source in ("wikipedia_en",) or "_en" in source:
            g.add((doc_uri, DCTERMS.language, Literal("en"), corpus_graph))

        # ── Provenance de Common Crawl ──
        if source == "common_crawl":
            cc_agent = SS_DATA["agent/common-crawl"]
            g.add((cc_agent, RDF.type, PROV.Organization, corpus_graph))
            g.add((cc_agent, RDFS.label, Literal("Common Crawl"), corpus_graph))
            g.add((cc_agent, SCHEMA.url, URIRef("https://commoncrawl.org"), corpus_graph))
            g.add((doc_uri, PROV.hadPrimarySource, cc_agent, corpus_graph))

    # ── Chunks (fragmentos indexados) ─────────────────────────────────────
    for chunk in chunks:
        chunk_uri = SS_DATA[f"chunk/{chunk['chunk_id']}"]
        doc_uri   = SS_DATA[f"document/{chunk['doc_id']}"]

        g.add((chunk_uri, RDF.type, SS.TextChunk, corpus_graph))
        g.add((chunk_uri, SS.partOf, doc_uri, corpus_graph))
        g.add((chunk_uri, SCHEMA.text, Literal(chunk["text"][:300]), corpus_graph))
        g.add((chunk_uri, SS.chunkIndex, Literal(chunk["chunk_id"]), corpus_graph))

    # ── Agente SourceSeek ──────────────────────────────────────────────────
    agent_uri = SS_DATA["agent/sourceseek"]
    g.add((agent_uri, RDF.type, PROV.SoftwareAgent, corpus_graph))
    g.add((agent_uri, RDFS.label, Literal("SourceSeek Football Search Engine"), corpus_graph))
    g.add((agent_uri, SCHEMA.url, URIRef("http://sourceseek.io"), corpus_graph))

    log.info(f"Grafo construido: {len(g)} tripletas en {len(list(g.contexts()))} grafos")
    return g


def export_nquads(g: ConjunctiveGraph, path: str = "data/football_knowledge.nq") -> None:
    """Exporta el grafo en formato N-Quads."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(g.serialize(format="nquads"))
    log.info(f"N-Quads exportado: {path}")


def export_turtle(g: ConjunctiveGraph, path: str = "data/football_knowledge.ttl") -> None:
    """Exporta el grafo en formato Turtle (más legible)."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(g.serialize(format="turtle"))
    log.info(f"Turtle exportado: {path}")


# ════════════════════════════════════════════════════════════════════════════
# 5. PIPELINE PRINCIPAL
# ════════════════════════════════════════════════════════════════════════════

def run_indexing(documents: list[dict] | None = None) -> tuple[faiss.IndexFlatIP, list[dict]]:
    """
    Pipeline completo: documentos → chunks → embeddings → FAISS index.
    También genera el grafo RDF en N-Quads.
    """
    if documents is None:
        doc_path = DATA_DIR / "documents.json"
        if not doc_path.exists():
            raise FileNotFoundError("Ejecuta primero: python src/ingest.py")
        with open(doc_path, encoding="utf-8") as f:
            documents = json.load(f)

    log.info(f"Indexando {len(documents)} documentos...")

    # 1. Chunking
    chunks = prepare_chunks(documents)

    # 2. Embeddings
    embeddings = generate_embeddings(chunks)

    # 3. FAISS
    index = build_faiss_index(embeddings)
    save_index(index, chunks)

    # 4. Grafo RDF → N-Quads
    log.info("Construyendo grafo de conocimiento RDF...")
    g = build_knowledge_graph(documents, chunks)
    export_nquads(g)
    export_turtle(g)

    # 5. Métricas
    log.info("─── Métricas de indexación ───────────────────────")
    log.info(f"  Documentos totales : {len(documents)}")
    log.info(f"  Chunks generados   : {len(chunks)}")
    log.info(f"  Vectores en FAISS  : {index.ntotal}")
    log.info(f"  Tripletas RDF      : {len(g)}")
    log.info("──────────────────────────────────────────────────")

    return index, chunks


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_indexing()
    print("\n✓ Indexación completa")
