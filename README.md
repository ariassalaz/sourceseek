# SourceSeek ⚽ — Motor de Búsqueda Semántica de Fútbol

**Instituto Tecnológico de la Laguna**  
Ricardo Arias Salazar · Sergio Reyes Montelongo  
Materia: Ingeniería de Software

---

## ¿Qué es SourceSeek?

SourceSeek es un sistema de búsqueda semántica especializado en fútbol soccer que combina:

| Tecnología | Uso |
|---|---|
| **Common Crawl** | Fuente de documentos web sobre fútbol |
| **DBpedia / N-Quads** | Web Semántica: entidades RDF de jugadores, clubes, competencias |
| **sentence-transformers** | Embeddings multilingües (español + inglés) |
| **FAISS** | Índice vectorial para búsqueda top-k de alta velocidad |
| **RAG** | Generación de respuestas aumentada con recuperación |
| **Ollama (llama3)** | LLM local para generación (opcional) |
| **Flask** | API REST + interfaz web |

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│                     INGESTA (ingest.py)                  │
│  Common Crawl ──┐                                        │
│  DBpedia SPARQL ├──► Documentos JSON ──► data/           │
│  N-Triples     ─┘    (texto limpio)                      │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│                  INDEXACIÓN (indexer.py)                 │
│  Documentos ──► Chunks (400 words, 80 overlap)          │
│              ──► Embeddings (MiniLM-L12-v2, 384d)       │
│              ──► FAISS IndexFlatIP                       │
│              ──► Grafo RDF ──► N-Quads / Turtle         │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│                    RAG (rag.py)                          │
│  Query ──► Embedding ──► FAISS top-k ──► Chunks         │
│                                      ──► Prompt         │
│                                      ──► Ollama LLM     │
│                                      ──► Respuesta      │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│                   API REST (app.py)                      │
│  POST /api/search     ← búsqueda principal              │
│  GET  /api/status     ← estado del sistema              │
│  POST /api/rebuild    ← reconstruir índice              │
│  GET  /api/graph-stats ← estadísticas N-Quads           │
│  GET  /api/graph-sample ← muestra del grafo RDF         │
└─────────────────────────────────────────────────────────┘
```

---

## Instalación y ejecución rápida

### Requisitos
- Python 3.10+
- 4 GB RAM mínimo (para sentence-transformers)
- [Ollama](https://ollama.com/) (opcional, para generación LLM)

### Pasos

```bash
# 1. Clonar / descomprimir el proyecto
cd sourceseek

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. (Opcional) Instalar y configurar Ollama para LLM
curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3
ollama serve   # en otra terminal

# 4. Ejecutar setup completo (ingesta + indexación + servidor)
python setup_and_run.py

# O paso a paso:
python src/ingest.py       # ingestar datos
python src/indexer.py      # generar embeddings + FAISS + N-Quads
python app.py              # iniciar servidor

# 5. Abrir navegador
open http://localhost:5000
```

---

## Estructura del proyecto

```
sourceseek/
├── app.py                  # Servidor Flask + API REST
├── setup_and_run.py        # Script de arranque automático
├── requirements.txt        # Dependencias Python
├── README.md
│
├── src/
│   ├── ingest.py           # Ingesta: Common Crawl + DBpedia + N-Triples
│   ├── indexer.py          # Chunking + Embeddings + FAISS + N-Quads
│   └── rag.py              # Motor RAG: recuperación + generación
│
├── templates/
│   └── index.html          # Frontend: UI de búsqueda
│
├── static/                 # Assets estáticos (CSS/JS adicional)
│
└── data/                   # Generado automáticamente
    ├── documents.json       # Corpus de documentos ingestados
    ├── chunks.pkl           # Chunks serializados
    ├── football.index       # Índice FAISS binario
    ├── football_knowledge.nq  # Grafo RDF en N-Quads
    └── football_knowledge.ttl # Grafo RDF en Turtle (legible)
```

---

## API REST

### `POST /api/search`
Búsqueda semántica con RAG.

**Request:**
```json
{
  "query": "¿Quién es Lionel Messi?",
  "top_k": 5
}
```

**Response:**
```json
{
  "query": "¿Quién es Lionel Messi?",
  "answer": "Lionel Messi es un futbolista argentino...",
  "chunks": [
    {
      "chunk_id": "1_0",
      "text": "Lionel Andrés Messi, nacido...",
      "url": "https://dbpedia.org/resource/Lionel_Messi",
      "source": "fallback",
      "type": "player",
      "score": 0.8721
    }
  ],
  "sources": ["https://dbpedia.org/resource/Lionel_Messi"],
  "used_llm": false,
  "stats": { "total_chunks": 3, "top_score": 0.8721 }
}
```

### `GET /api/status`
Estado del sistema (índice, Ollama, N-Quads, etc.).

### `GET /api/graph-stats`
Estadísticas del grafo RDF (número de tripletas, tamaño del archivo).

### `GET /api/graph-sample?limit=20`
Muestra del grafo N-Quads.

---

## Web Semántica y N-Quads

El sistema genera automáticamente un grafo de conocimiento RDF con:

- **Clases**: `ss:Document`, `ss:TextChunk`, `schema:Person`, `schema:SportsTeam`, `schema:SportsEvent`, `ss:Rule`, `ss:Tactic`, `ss:Technology`
- **Propiedades**: `dcterms:source`, `dcterms:description`, `ss:partOf`, `prov:wasAttributedTo`
- **Namespaces**: `schema.org`, `dbpedia.org`, `prov`, `dcterms`, `skos`

El archivo `data/football_knowledge.nq` se puede cargar en cualquier triplestore compatible (Apache Jena, Virtuoso, GraphDB).

---

## Fuentes de datos

| Fuente | Tipo | Contenido |
|---|---|---|
| Common Crawl (CC-MAIN-2024) | HTML/texto | Páginas de fútbol (espn.com, goal.com, transfermarkt.com, etc.) |
| DBpedia SPARQL | RDF | Jugadores, clubes, competiciones |
| DBpedia N-Triples | RDF | Entidades clave (Messi, Champions, etc.) |
| Datos curados | JSON | 16 documentos de respaldo en español/inglés |

---

## Sprints del proyecto

| Sprint | Fechas | Tareas |
|---|---|---|
| Sprint 1 | 11/02 - 16/02 | Planeación y arquitectura |
| Sprint 2 | 11/02 - 03/03 | Búsqueda semántica (chunks, embeddings, FAISS) |
| Sprint 3 | 11/02 - 24/02 | Casos de uso, diagramas de actividad, requisitos |
| General 4 | 03/04 - 15/04 | Recuperación de información |
| General 5 | 16/04 - 04/05 | Generación RAG |
| General 6 | 05/05 - 14/05 | Interfaz y API |
| General 7 | 15/05 - 25/05 | Evaluación y optimización |
| General 8 | 26/05 - 01/06 | Documentación y demo |
