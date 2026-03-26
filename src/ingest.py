"""
SourceSeek - Módulo de Ingesta EXPANDIDO
Recolecta miles de documentos de fútbol desde:
  - DBpedia SPARQL (jugadores, clubes, competiciones, estadios, entrenadores)
  - Wikipedia API (artículos completos en español e inglés)
  - Wikidata (datos estructurados)
  - Datos curados de respaldo
"""

import os, re, json, hashlib, logging, time, unicodedata
from pathlib import Path
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# ── HTTP session con reintentos ──────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "SourceSeek/1.0 (ITL research project; football search)"})
    return s

SESSION = make_session()

# ════════════════════════════════════════════════════════════════════════════
# 1. DBpedia SPARQL — consultas masivas de fútbol
# ════════════════════════════════════════════════════════════════════════════

DBPEDIA_SPARQL = "https://dbpedia.org/sparql"

def sparql_query(query: str, timeout: int = 45) -> list[dict]:
    """Ejecuta consulta SPARQL en DBpedia."""
    try:
        resp = SESSION.get(
            DBPEDIA_SPARQL,
            params={"query": query, "format": "application/sparql-results+json"},
            headers={"Accept": "application/sparql-results+json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])
    except Exception as e:
        log.warning(f"SPARQL error: {e}")
        return []


def bindings_to_doc(b: dict, label_key="label", abstract_key="abstract",
                    uri_key="subject", doc_type="document") -> Optional[dict]:
    """Convierte un binding SPARQL en documento."""
    uri      = b.get(uri_key, {}).get("value", "")
    label    = b.get(label_key, {}).get("value", "")
    abstract = b.get(abstract_key, {}).get("value", "")
    if not abstract or len(abstract) < 80:
        return None
    return {
        "url":    uri,
        "text":   f"{label}: {abstract}",
        "source": "dbpedia_sparql",
        "type":   doc_type,
    }


# ── Jugadores ────────────────────────────────────────────────────────────────
QUERIES_PLAYERS = [
    # Jugadores con abstract en español
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:SoccerPlayer ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 800
    """,
    # Jugadores con abstract en inglés
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:SoccerPlayer ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 800
    """,
    # Atletas de fútbol (captura más jugadores)
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbp: <http://dbpedia.org/property/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject dbo:sport <http://dbpedia.org/resource/Association_football> ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 400
    """,
    # Jugadores mexicanos específicamente
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbr: <http://dbpedia.org/resource/>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:SoccerPlayer ;
               dbo:nationality dbr:Mexico ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?abstract)='es' || lang(?abstract)='en')
    } LIMIT 300
    """,
    # Jugadores de América Latina (Colombia, Argentina, Brasil, Uruguay, Chile)
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbr: <http://dbpedia.org/resource/>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:SoccerPlayer ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      ?subject dbo:nationality ?nat .
      FILTER (?nat IN (dbr:Colombia, dbr:Uruguay, dbr:Chile, dbr:Peru, dbr:Ecuador))
      FILTER (lang(?abstract)='es' || lang(?abstract)='en')
    } LIMIT 300
    """,
]

# ── Clubes ───────────────────────────────────────────────────────────────────
QUERIES_CLUBS = [
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?club ?label ?abstract WHERE {
      ?club a dbo:SoccerClub ;
            rdfs:label ?label ;
            dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 600
    """,
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?club ?label ?abstract WHERE {
      ?club a dbo:SoccerClub ;
            rdfs:label ?label ;
            dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 600
    """,
]

# ── Competiciones ─────────────────────────────────────────────────────────────
QUERIES_COMPETITIONS = [
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      { ?subject a dbo:SoccerLeague } UNION { ?subject a dbo:FootballLeague }
      ?subject rdfs:label ?label ; dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 300
    """,
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      { ?subject a dbo:SoccerLeague } UNION { ?subject a dbo:FootballLeague }
      ?subject rdfs:label ?label ; dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 300
    """,
    # Torneos y copas
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbr: <http://dbpedia.org/resource/>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject dbo:sport <http://dbpedia.org/resource/Association_football> ;
               a dbo:SportsLeague ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 300
    """,
]

# ── Entrenadores ──────────────────────────────────────────────────────────────
QUERIES_MANAGERS = [
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:SoccerManager ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 300
    """,
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:SoccerManager ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 300
    """,
]

# ── Estadios ──────────────────────────────────────────────────────────────────
QUERIES_STADIUMS = [
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:Stadium ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 200
    """,
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:Stadium ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 200
    """,
]

# ── Copas del Mundo ───────────────────────────────────────────────────────────
QUERIES_WORLDCUPS = [
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbr: <http://dbpedia.org/resource/>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject dbo:isFinalOf <http://dbpedia.org/resource/FIFA_World_Cup> ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?abstract)='es' || lang(?abstract)='en')
    } LIMIT 100
    """,
    # Copas del Mundo específicas
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:FootballMatch ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?abstract)='es' && CONTAINS(LCASE(STR(?subject)), 'world_cup'))
    } LIMIT 100
    """,
]

# ── Selecciones nacionales ────────────────────────────────────────────────────
QUERIES_NATIONAL_TEAMS = [
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:NationalSoccerTeam ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='es' && lang(?abstract)='es')
    } LIMIT 200
    """,
    """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?subject ?label ?abstract WHERE {
      ?subject a dbo:NationalSoccerTeam ;
               rdfs:label ?label ;
               dbo:abstract ?abstract .
      FILTER (lang(?label)='en' && lang(?abstract)='en')
    } LIMIT 200
    """,
]


def harvest_dbpedia() -> list[dict]:
    """Recolecta todos los documentos desde DBpedia SPARQL."""
    docs = []
    seen = set()

    tasks = [
        ("jugadores",     QUERIES_PLAYERS,       "player"),
        ("clubes",        QUERIES_CLUBS,          "club"),
        ("competiciones", QUERIES_COMPETITIONS,   "competition"),
        ("entrenadores",  QUERIES_MANAGERS,       "manager"),
        ("estadios",      QUERIES_STADIUMS,       "stadium"),
        ("selecciones",   QUERIES_NATIONAL_TEAMS, "national_team"),
        ("mundiales",     QUERIES_WORLDCUPS,      "world_cup"),
    ]

    for category, queries, doc_type in tasks:
        cat_count = 0
        for q in queries:
            log.info(f"  DBpedia SPARQL [{category}] consultando...")
            bindings = sparql_query(q)
            for b in bindings:
                uri_key = "club" if doc_type == "club" else "subject"
                doc = bindings_to_doc(b, uri_key=uri_key, doc_type=doc_type)
                if not doc:
                    continue
                key = hashlib.md5(doc["text"][:200].encode()).hexdigest()
                if key in seen:
                    continue
                seen.add(key)
                docs.append(doc)
                cat_count += 1
            time.sleep(0.5)  # respetar rate limit de DBpedia
        log.info(f"  ✓ {category}: {cat_count} documentos")

    log.info(f"DBpedia total: {len(docs)} documentos")
    return docs


# ════════════════════════════════════════════════════════════════════════════
# 2. Wikipedia API — artículos completos
# ════════════════════════════════════════════════════════════════════════════

WIKI_API_ES = "https://es.wikipedia.org/api/rest_v1/page/summary/"
WIKI_API_EN = "https://en.wikipedia.org/api/rest_v1/page/summary/"

# Artículos clave de fútbol para buscar en Wikipedia
WIKIPEDIA_ARTICLES_ES = [
    # Reglas y conceptos
    "Fútbol", "Fuera_de_juego", "Penalti_(fútbol)", "Tarjeta_roja",
    "Tarjeta_amarilla", "Tiro_libre", "Saque_de_esquina", "VAR_(fútbol)",
    "Offside", "Hat-trick", "Gol_olímpico", "Autogol", "Tiempo_extra_(deporte)",
    "Tanda_de_penaltis", "Regla_del_gol_de_oro", "Línea_de_gol_(tecnología)",
    # Posiciones
    "Portero_(fútbol)", "Defensa_(fútbol)", "Centrocampista", "Delantero_(fútbol)",
    "Lateral_(fútbol)", "Libero_(fútbol)", "Mediapunta",
    # Tácticas
    "Tiki-taka", "Pressing_(fútbol)", "Contraataque_(fútbol)", "Sistema_4-3-3",
    "Sistema_4-4-2", "Sistema_3-5-2",
    # Competiciones internacionales
    "Copa_Mundial_de_Fútbol", "UEFA_Champions_League", "UEFA_Europa_League",
    "Copa_América", "UEFA_Euro", "Copa_Africana_de_Naciones",
    "Copa_de_Oro_de_la_CONCACAF", "Copa_FIFA_del_Mundo_de_Clubes",
    "Supercopa_de_Europa",
    # Ligas
    "Primera_División_de_España", "Premier_League", "Bundesliga",
    "Serie_A", "Ligue_1", "Eredivisie", "Primeira_Liga",
    "Liga_MX", "Major_League_Soccer", "Liga_Argentina_de_Fútbol_Profesional",
    "Campeonato_Brasileiro_Série_A",
    # Copas nacionales
    "Copa_del_Rey", "FA_Cup", "Copa_DFB", "Copa_Italia",
    # Selecciones principales
    "Selección_de_fútbol_de_Argentina", "Selección_de_fútbol_de_Brasil",
    "Selección_de_fútbol_de_Francia", "Selección_de_fútbol_de_Alemania",
    "Selección_de_fútbol_de_España", "Selección_de_fútbol_de_Italia",
    "Selección_de_fútbol_de_Inglaterra", "Selección_de_fútbol_de_Portugal",
    "Selección_de_fútbol_de_Uruguay", "Selección_de_fútbol_de_México",
    "Selección_de_fútbol_de_Colombia", "Selección_de_fútbol_de_Países_Bajos",
    "Selección_de_fútbol_de_Bélgica", "Selección_de_fútbol_de_Croacia",
    "Selección_de_fútbol_de_Marruecos", "Selección_de_fútbol_de_Japón",
    "Selección_de_fútbol_de_Senegal", "Selección_de_fútbol_de_Nigeria",
    # Mundiales
    "Copa_Mundial_de_Fútbol_de_2022", "Copa_Mundial_de_Fútbol_de_2018",
    "Copa_Mundial_de_Fútbol_de_2014", "Copa_Mundial_de_Fútbol_de_2010",
    "Copa_Mundial_de_Fútbol_de_2006", "Copa_Mundial_de_Fútbol_de_2002",
    "Copa_Mundial_de_Fútbol_de_1998",
    # Jugadores históricos
    "Pelé", "Diego_Maradona", "Ronaldo_Nazário", "Zinedine_Zidane",
    "Ronaldinho", "Johan_Cruyff", "Franz_Beckenbauer", "Michel_Platini",
    "Roberto_Baggio", "Thierry_Henry", "Andrés_Iniesta", "Xavi_Hernández",
    "Iker_Casillas", "Gianluigi_Buffon", "Paolo_Maldini", "Cafu",
    "Roberto_Carlos", "David_Beckham", "Raúl_González",
    # Jugadores actuales
    "Lionel_Messi", "Cristiano_Ronaldo", "Kylian_Mbappé",
    "Erling_Haaland", "Neymar_Jr", "Vinicius_Júnior", "Rodri_(futbolista)",
    "Lamine_Yamal", "Pedri", "Jude_Bellingham", "Mohamed_Salah",
    "Sadio_Mané", "Kevin_De_Bruyne", "Harry_Kane", "Robert_Lewandowski",
    "Karim_Benzema", "Luka_Modrić", "Toni_Kroos", "Sergio_Ramos",
    "Virgil_van_Dijk", "Alisson_Becker", "Manuel_Neuer", "Marc-André_ter_Stegen",
    # Mexicanos — selección y Liga MX
    "Guillermo_Ochoa", "Hirving_Lozano", "Raúl_Jiménez",
    "Andrés_Guardado", "Carlos_Vela", "Javier_Hernández",
    "Hugo_Sánchez_(futbolista)", "Cuauhtémoc_Blanco",
    "Santiago_Giménez_(futbolista)", "Edson_Álvarez",
    "César_Montes", "Henry_Martín", "Roberto_Alvarado",
    "Orbelín_Pineda", "Jordan_Carrillo",
    "Alexis_Vega", "Luis_Chávez_(futbolista)",
    # Argentinos / Sudamericanos
    "Sergio_Agüero", "Gabriel_Batistuta", "Hernán_Crespo",
    "Rodrigo_De_Paul", "Ángel_Di_María", "Lautaro_Martínez",
    "Julián_Álvarez", "Alexis_Mac_Allister",
    # Colombianos / venezolanos / etc.
    "James_Rodríguez", "Falcao", "Luis_Díaz_(futbolista)",
    "Jhon_Durán",
    # Brasileños adicionales
    "Raphinha", "Endrick_(futbolista)", "Rodrygo",
    # Clubes europeos
    "Real_Madrid_Club_de_Fútbol", "FC_Barcelona", "Manchester_City_FC",
    "Liverpool_FC", "Bayern_de_Múnich", "Paris_Saint-Germain_FC",
    "Chelsea_FC", "Arsenal_FC", "Manchester_United_FC",
    "Juventus_FC", "AC_Milan", "FC_Internazionale_Milano",
    "Borussia_Dortmund", "Atlético_de_Madrid", "Sevilla_FC",
    "Ajax", "Benfica", "Porto_(fútbol)", "Sporting_de_Lisboa",
    "Celtic_FC", "Rangers_FC",
    # Clubes de América Latina
    "Club_América", "Guadalajara_(fútbol)", "Cruz_Azul",
    "UNAM_(fútbol)", "Tigres_UANL", "Monterrey_(fútbol)",
    "Club_Pachuca", "Atlas_FC", "Club_Toluca",
    "Club_León", "Club_Necaxa", "Santos_Laguna",
    "Club_Puebla", "Club_Tijuana", "Mazatlán_FC",
    "Boca_Juniors", "River_Plate", "Flamengo",
    "Corinthians", "São_Paulo_FC", "Santos_FC_(Brasil)",
    "Nacional_(Uruguay)", "Peñarol",
    # Estadios
    "Estadio_Santiago_Bernabéu", "Camp_Nou", "Wembley_(estadio)",
    "Old_Trafford", "Allianz_Arena", "Estadio_Azteca",
    "Maracaná", "Estadio_Monumental_(Buenos_Aires)",
    # Entrenadores
    "Pep_Guardiola", "Jürgen_Klopp", "Carlo_Ancelotti",
    "José_Mourinho", "Diego_Simeone", "Didier_Deschamps",
    "Lionel_Scaloni", "Luis_Enrique",
    # Premios y reconocimientos
    "Balón_de_Oro", "Premio_The_Best_de_la_FIFA",
    "Bota_de_Oro_europea", "Guante_de_Oro",
    # Historia del fútbol
    "Historia_del_fútbol", "FIFA", "UEFA", "CONMEBOL",
    "CONCACAF", "CAF_(fútbol)", "AFC_(fútbol)",
]

WIKIPEDIA_ARTICLES_EN = [
    # Key players not well covered in Spanish
    "Kylian_Mbappé", "Erling_Haaland", "Jude_Bellingham",
    "Lamine_Yamal", "Phil_Foden", "Bukayo_Saka", "Marcus_Rashford",
    "Son_Heung-min", "Takumi_Minamino", "Riyad_Mahrez",
    # World Cups in English
    "2026_FIFA_World_Cup", "2022_FIFA_World_Cup", "2018_FIFA_World_Cup",
    # Champions League finals
    "2023–24_UEFA_Champions_League", "2022–23_UEFA_Champions_League",
    "UEFA_Champions_League_records_and_statistics",
    # Premier League
    "2023–24_Premier_League", "Premier_League_records_and_statistics",
    # Transfer records
    "List_of_most_expensive_association_football_transfers",
    "List_of_FIFA_World_Cup_top_scorers",
    "List_of_men's_FIFA_World_Cup_finals",
    # Concepts
    "Association_football", "Football_pitch", "Offside_(association_football)",
    "Penalty_kick_(association_football)", "VAR_(association_football)",
]


def fetch_wikipedia_summary(title: str, lang: str = "es") -> Optional[dict]:
    """Descarga el resumen de un artículo de Wikipedia."""
    base = WIKI_API_ES if lang == "es" else WIKI_API_EN
    try:
        resp = SESSION.get(base + title, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        extract = data.get("extract", "")
        if not extract or len(extract) < 100:
            return None
        page_url = data.get("content_urls", {}).get("desktop", {}).get("page", "")
        return {
            "url":    page_url or f"https://{lang}.wikipedia.org/wiki/{title}",
            "text":   f"{data.get('title','')}: {extract}",
            "source": f"wikipedia_{lang}",
            "type":   "wikipedia",
        }
    except Exception as e:
        log.debug(f"Wikipedia error ({title}): {e}")
        return None


def harvest_wikipedia() -> list[dict]:
    """Descarga artículos de Wikipedia en español e inglés."""
    docs = []
    seen = set()
    total = len(WIKIPEDIA_ARTICLES_ES) + len(WIKIPEDIA_ARTICLES_EN)
    count = 0

    for title in WIKIPEDIA_ARTICLES_ES:
        doc = fetch_wikipedia_summary(title, lang="es")
        count += 1
        if doc:
            key = hashlib.md5(doc["text"][:200].encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                docs.append(doc)
        if count % 20 == 0:
            log.info(f"  Wikipedia ES: {count}/{len(WIKIPEDIA_ARTICLES_ES)} artículos, {len(docs)} exitosos")
        time.sleep(0.15)

    for title in WIKIPEDIA_ARTICLES_EN:
        doc = fetch_wikipedia_summary(title, lang="en")
        count += 1
        if doc:
            key = hashlib.md5(doc["text"][:200].encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                docs.append(doc)
        time.sleep(0.15)

    log.info(f"Wikipedia total: {len(docs)} artículos descargados")
    return docs


# ════════════════════════════════════════════════════════════════════════════
# 3. Wikidata — datos estructurados de jugadores y clubes
# ════════════════════════════════════════════════════════════════════════════

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

WIKIDATA_PLAYERS_QUERY = """
SELECT ?player ?playerLabel ?birthdate ?nationalityLabel ?positionLabel
       ?clubLabel ?goalsLabel ?capsLabel WHERE {
  ?player wdt:P31 wd:Q5 ;          # humano
          wdt:P106 wd:Q937857 .    # futbolista
  OPTIONAL { ?player wdt:P569 ?birthdate }
  OPTIONAL { ?player wdt:P27 ?nationality }
  OPTIONAL { ?player wdt:P413 ?position }
  OPTIONAL { ?player wdt:P54 ?club }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "es,en" }
} LIMIT 500
"""

WIKIDATA_MEXICAN_PLAYERS_QUERY = """
SELECT ?player ?playerLabel ?birthdate ?positionLabel ?clubLabel WHERE {
  ?player wdt:P31 wd:Q5 ;
          wdt:P106 wd:Q937857 ;
          wdt:P27 wd:Q96 .         # nacionalidad = México
  OPTIONAL { ?player wdt:P569 ?birthdate }
  OPTIONAL { ?player wdt:P413 ?position }
  OPTIONAL { ?player wdt:P54 ?club }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "es,en" }
} LIMIT 300
"""

WIKIDATA_CLUBS_QUERY = """
SELECT ?club ?clubLabel ?countryLabel ?leagueLabel ?foundedLabel WHERE {
  ?club wdt:P31 wd:Q476028 .    # club de fútbol
  OPTIONAL { ?club wdt:P17 ?country }
  OPTIONAL { ?club wdt:P118 ?league }
  OPTIONAL { ?club wdt:P571 ?founded }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "es,en" }
} LIMIT 500
"""


def fetch_wikidata(query: str) -> list[dict]:
    """Consulta Wikidata SPARQL."""
    try:
        resp = SESSION.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers={"Accept": "application/sparql-results+json"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])
    except Exception as e:
        log.warning(f"Wikidata error: {e}")
        return []


def harvest_wikidata() -> list[dict]:
    """Recolecta datos estructurados de Wikidata."""
    docs = []
    seen = set()

    def _wikidata_player_to_doc(b: dict, label_key: str = "playerLabel") -> Optional[dict]:
        label       = b.get(label_key, {}).get("value", "")
        nationality = b.get("nationalityLabel", {}).get("value", "")
        position    = b.get("positionLabel", {}).get("value", "")
        club        = b.get("clubLabel", {}).get("value", "")
        uri         = b.get("player", {}).get("value", "")
        if not label or label.startswith("Q"):
            return None
        parts = [f"{label} es un futbolista profesional."]
        if nationality: parts.append(f"Nacionalidad: {nationality}.")
        if position:    parts.append(f"Posición: {position}.")
        if club:        parts.append(f"Club actual: {club}.")
        text = " ".join(parts)
        if len(text) < 50:
            return None
        return {"url": uri, "text": text, "source": "wikidata", "type": "player", "entity": label}

    # Jugadores generales
    log.info("  Wikidata: consultando jugadores...")
    bindings = fetch_wikidata(WIKIDATA_PLAYERS_QUERY)
    for b in bindings:
        doc = _wikidata_player_to_doc(b)
        if not doc:
            continue
        key = hashlib.md5(doc["text"].encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            docs.append(doc)
    log.info(f"  ✓ Wikidata jugadores: {len(docs)}")

    # Jugadores mexicanos (consulta específica para mejor cobertura)
    log.info("  Wikidata: consultando jugadores mexicanos...")
    mx_before = len(docs)
    bindings = fetch_wikidata(WIKIDATA_MEXICAN_PLAYERS_QUERY)
    for b in bindings:
        doc = _wikidata_player_to_doc(b)
        if not doc:
            continue
        doc["text"] = doc["text"].replace("es un futbolista profesional.", "es un futbolista mexicano profesional.")
        key = hashlib.md5(doc["text"].encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            docs.append(doc)
    log.info(f"  ✓ Wikidata jugadores mexicanos: {len(docs) - mx_before}")

    # Clubes
    before = len(docs)
    log.info("  Wikidata: consultando clubes...")
    bindings = fetch_wikidata(WIKIDATA_CLUBS_QUERY)
    for b in bindings:
        label   = b.get("clubLabel", {}).get("value", "")
        country = b.get("countryLabel", {}).get("value", "")
        league  = b.get("leagueLabel", {}).get("value", "")
        uri     = b.get("club", {}).get("value", "")
        if not label or label.startswith("Q"):
            continue
        parts = [f"{label} es un club de fútbol profesional."]
        if country: parts.append(f"País: {country}.")
        if league:  parts.append(f"Liga: {league}.")
        text = " ".join(parts)
        key = hashlib.md5(text.encode()).hexdigest()
        if key not in seen and len(text) > 50:
            seen.add(key)
            docs.append({"url": uri, "text": text, "source": "wikidata", "type": "club"})
    log.info(f"  ✓ Wikidata clubes: {len(docs) - before}")

    return docs


# ════════════════════════════════════════════════════════════════════════════
# 4. COMMON CRAWL — Motor principal: CDX multi-índice + WARCs
# ════════════════════════════════════════════════════════════════════════════

import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed

CC_DATA = "https://data.commoncrawl.org/"

# Índices CC más recientes (de más nuevo a más antiguo)
CC_INDEXES = [
    "https://index.commoncrawl.org/CC-MAIN-2024-51-index",
    "https://index.commoncrawl.org/CC-MAIN-2024-46-index",
    "https://index.commoncrawl.org/CC-MAIN-2024-38-index",
    "https://index.commoncrawl.org/CC-MAIN-2024-22-index",
    "https://index.commoncrawl.org/CC-MAIN-2024-10-index",
]

# Dominios de fútbol especializados con sus patrones de URL
# Cada entrada: (nombre_dominio, patron_url_para_CDX, paginas_objetivo)
CC_FOOTBALL_DOMAINS = [
    # ── Estadísticas y datos de jugadores/clubes ──────────────────────────
    ("transfermarkt_es",      "www.transfermarkt.es/*profil*spieler*",      8),
    ("transfermarkt_com",     "www.transfermarkt.com/*profil*spieler*",     8),
    ("transfermarkt_mx",      "www.transfermarkt.mx/*profil*spieler*",      5),
    ("fbref_players",         "fbref.com/en/players/*/*",                   8),
    ("fbref_squads",          "fbref.com/en/squads/*/*",                    5),
    ("soccerway_players",     "int.soccerway.com/players/*/*/",             6),
    ("soccerway_teams",       "int.soccerway.com/teams/*/*/",               4),
    ("worldfootball_players", "www.worldfootball.net/player_summary/*/*/",  6),
    ("worldfootball_teams",   "www.worldfootball.net/teams/*/*/",           4),
    ("national_teams",        "www.national-football-teams.com/player/*",   5),
    ("ceroacero_jugadores",   "www.ceroacero.es/jugador/*",                 6),
    ("ceroacero_equipos",     "www.ceroacero.es/equipo/*",                  4),
    ("soccerstats",           "www.soccerstats.com/player.asp*",            4),
    ("footballdb",            "www.footballdatabase.eu/football.player.*",  4),
    # ── Medios especializados México / CONCACAF ───────────────────────────
    ("mediotiempo_futbol",    "www.mediotiempo.com/futbol/*",               10),
    ("mediotiempo_mx",        "www.mediotiempo.com/futbol/liga-mx/*",       8),
    ("record_mx",             "www.record.com.mx/futbol-mexico/*",          6),
    ("record_inter",          "www.record.com.mx/futbol-internacional/*",   4),
    ("halftime_mx",           "www.halftime.mx/futbol/*",                   4),
    ("espn_deport",           "espndeportes.espn.com/futbol/*",             6),
    # ── Medios deportivos en español ──────────────────────────────────────
    ("marca_futbol",          "www.marca.com/futbol/*",                     8),
    ("as_futbol",             "as.com/futbol/*",                            8),
    ("mundodeportivo",        "www.mundodeportivo.com/futbol/*",            5),
    ("sport_es",              "www.sport.es/es/futbol/*",                   4),
    ("eurosport_es",          "www.eurosport.es/futbol/*",                  4),
    # ── Medios en inglés ──────────────────────────────────────────────────
    ("goal_en",               "www.goal.com/en/*",                          6),
    ("espn_soccer",           "www.espn.com/soccer/player/_/id/*",          6),
    ("skysports",             "www.skysports.com/football/player/*",        5),
    ("bbc_football",          "www.bbc.co.uk/sport/football/*",             5),
    ("90min",                 "www.90min.com/*",                            4),
    # ── Ratings y análisis ────────────────────────────────────────────────
    ("sofascore_player",      "www.sofascore.com/player/*",                 5),
    ("whoscored_player",      "www.whoscored.com/Players/*",                5),
    # ── Historia y archivo ────────────────────────────────────────────────
    ("rsssf",                 "www.rsssf.org/*",                            3),
    ("footballhistory",       "www.footballhistory.org/*",                  3),
]

# Dominios prioritarios para búsqueda dinámica de entidades desconocidas
CC_ENTITY_DOMAINS = [
    ("transfermarkt.es",            "www.transfermarkt.es/*{slug}*"),
    ("transfermarkt.com",           "www.transfermarkt.com/*{slug}*"),
    ("fbref.com",                   "fbref.com/*{slug}*"),
    ("soccerway.com",               "int.soccerway.com/*{slug}*"),
    ("worldfootball.net",           "www.worldfootball.net/*{slug}*"),
    ("ceroacero.es",                "www.ceroacero.es/*{slug}*"),
    ("mediotiempo.com",             "www.mediotiempo.com/*{slug}*"),
    ("record.com.mx",               "www.record.com.mx/*{slug}*"),
    ("nacional-football-teams.com", "www.national-football-teams.com/*{slug}*"),
    ("sofascore.com",               "www.sofascore.com/*{slug}*"),
    ("goal.com",                    "www.goal.com/*{slug}*"),
    ("marca.com",                   "www.marca.com/*{slug}*"),
    ("as.com",                      "as.com/*{slug}*"),
]


def _normalize_slug(name: str) -> str:
    """Convierte nombre de entidad a slug URL (minúsculas, sin tildes, guiones)."""
    normalized = unicodedata.normalize("NFD", name.lower())
    ascii_name = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name).strip("-")
    return slug


def _detect_type_from_url(url: str) -> str:
    """Infiere el tipo de entidad desde la URL."""
    u = url.lower()
    # Transfermarkt: /spieler/ = jugador, /verein/ = club, /trainer/ = entrenador
    if any(k in u for k in ["/spieler/", "/player/", "/jugador/", "footballer",
                              "playerprofile", "/players/", "/jugadores/"]):
        return "player"
    if any(k in u for k in ["/verein/", "/club/", "/equipo/", "/team/",
                              "_fc", "fc_", "/squads/", "/clubs/"]):
        return "club"
    if any(k in u for k in ["/trainer/", "/entrenador/", "/manager/", "/coaches/"]):
        return "manager"
    if any(k in u for k in ["league", "/liga/", "championship", "cup",
                              "champions", "world_cup", "premier", "/comps/"]):
        return "competition"
    if any(k in u for k in ["stadium", "estadio", "ground", "arena"]):
        return "stadium"
    return "concept"


def _extract_entity_from_url(url: str, domain: str = "") -> str:
    """Extrae nombre de entidad desde la URL (optimizado por dominio)."""
    try:
        path = url.rstrip("/").split("?")[0]
        slug = path.split("/")[-1]
        # Transfermarkt: URL contiene el nombre directamente
        if "transfermarkt" in domain:
            parts = path.split("/")
            if len(parts) >= 3:
                slug = parts[-3]  # penúltimo segmento suele ser el nombre
        # FBref: nombre tras último /
        return slug.replace("-", " ").replace("_", " ").strip()
    except Exception:
        return ""


def cdx_search_index(cc_index_url: str, url_pattern: str,
                     limit: int = 5, timeout: int = 20) -> list[dict]:
    """Busca páginas en un índice CDX específico de Common Crawl."""
    try:
        resp = SESSION.get(
            cc_index_url,
            params={
                "url":    url_pattern,
                "output": "json",
                "limit":  limit,
                "fl":     "url,timestamp,filename,offset,length,status,mime",
                "filter": "status:200",
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            return []
        records = []
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass
        return records
    except Exception as e:
        log.debug(f"CDX error [{cc_index_url[-20:]}] ({url_pattern[:50]}): {e}")
        return []


def cdx_search_multi_index(url_pattern: str, limit_per_index: int = 5,
                           indexes: list = None) -> list[dict]:
    """
    Busca en múltiples índices CC en paralelo y deduplica por URL.
    Retorna los registros más recientes primero.
    """
    if indexes is None:
        indexes = CC_INDEXES

    all_records: list[dict] = []
    seen_urls: set = set()

    def _search_one(idx_url: str) -> list[dict]:
        return cdx_search_index(idx_url, url_pattern, limit=limit_per_index)

    with ThreadPoolExecutor(max_workers=min(len(indexes), 5)) as ex:
        futures = {ex.submit(_search_one, idx): idx for idx in indexes}
        for fut in as_completed(futures):
            try:
                recs = fut.result()
                for r in recs:
                    url = r.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_records.append(r)
            except Exception:
                pass

    # Más recientes primero
    all_records.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    return all_records


def fetch_warc_record(filename: str, offset: int, length: int) -> Optional[str]:
    """Descarga y extrae HTML de un registro WARC de Common Crawl."""
    if not filename or length <= 0:
        return None
    url = CC_DATA + filename
    try:
        resp = SESSION.get(
            url,
            headers={"Range": f"bytes={offset}-{offset + length - 1}"},
            timeout=25,
        )
        if resp.status_code not in (200, 206):
            return None
        raw = resp.content
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
        text = raw.decode("utf-8", errors="ignore")
        for marker in ["<!DOCTYPE", "<!doctype", "<html", "<HTML"]:
            idx = text.find(marker)
            if idx >= 0:
                return text[idx:]
        return text
    except Exception as e:
        log.debug(f"WARC fetch error: {e}")
        return None


def extract_text_from_html(html: str, max_len: int = 3000,
                            domain: str = "") -> str:
    """
    Extrae texto limpio de HTML.
    Aplica selectores específicos por dominio para mayor precisión.
    """
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer",
                          "aside", "form", "noscript", "iframe", "svg"]):
            tag.decompose()

        # Selectores específicos por dominio de fútbol
        domain_selectors = {
            "transfermarkt": [".data-header", ".spielerdaten", ".tm-player-header",
                               ".box", "#player-summary-data"],
            "fbref":         ["#content", "#div_stats_standard", ".stats_table"],
            "soccerway":     [".block_player_passport", ".block_player_career"],
            "mediotiempo":   [".content-body", ".article-body", ".nota-detalle"],
            "marca":         [".article-body", ".ue-c-article__body"],
            "as":            [".article-body", ".cuerpo-noticia"],
            "goal":          [".article__body", "[data-cy='article-content']"],
            "espn":          [".article-body", ".PlayerHeader__Bio"],
            "ceroacero":     [".jugador-datos", ".tabla-estadisticas"],
        }

        # Intentar selector específico del dominio
        for d_key, selectors in domain_selectors.items():
            if d_key in domain.lower():
                for sel in selectors:
                    el = soup.select_one(sel)
                    if el:
                        txt = el.get_text(separator=" ", strip=True)
                        if len(txt) > 200:
                            return txt[:max_len]

        # Selectores genéricos de artículo
        for selector in ["article", "main", "#mw-content-text",
                          ".mw-body-content", ".article-body", "#article-body",
                          "[role='main']", ".content", "#content"]:
            el = soup.select_one(selector)
            if el:
                txt = el.get_text(separator=" ", strip=True)
                if len(txt) > 200:
                    return txt[:max_len]

        # Fallback: párrafos
        paras = [p.get_text(strip=True) for p in soup.find_all("p")
                 if len(p.get_text(strip=True)) > 50]
        return " ".join(paras)[:max_len]
    except Exception as e:
        log.debug(f"HTML extraction error: {e}")
        return ""


def _process_warc_record(rec: dict, domain: str, seen: set) -> Optional[dict]:
    """Descarga y procesa un registro WARC. Retorna doc o None."""
    url      = rec.get("url", "")
    filename = rec.get("filename", "")
    offset   = int(rec.get("offset", 0))
    length   = int(rec.get("length", 0))
    mime     = rec.get("mime", "")

    if not url or not filename or length <= 0:
        return None
    if mime and "html" not in mime.lower():
        return None

    html = fetch_warc_record(filename, offset, length)
    if not html:
        return None

    text = extract_text_from_html(html, max_len=3000, domain=domain)
    if not text or len(text) < 150:
        return None

    key = hashlib.md5(text[:200].encode()).hexdigest()
    if key in seen:
        return None
    seen.add(key)

    doc_type = _detect_type_from_url(url)
    entity   = _extract_entity_from_url(url, domain)

    return {
        "url":    url,
        "text":   text,
        "source": "common_crawl",
        "type":   doc_type,
        "entity": entity,
        "domain": domain,
    }


def harvest_common_crawl(max_pages: int = 500) -> list[dict]:
    """
    Motor principal de ingesta: recolecta contenido de fútbol desde Common Crawl.

    Características:
    - Busca en 5 índices CC en paralelo (CC_INDEXES)
    - Cubre 30+ dominios especializados de fútbol (estadísticas, medios, análisis)
    - Extracción inteligente de texto por dominio
    - Descarga paralela de registros WARC
    - Objetivo: 200-500 páginas de alta calidad
    """
    docs: list[dict] = []
    seen: set = set()

    pages_per_domain = max(2, max_pages // len(CC_FOOTBALL_DOMAINS))
    log.info(f"Common Crawl — Iniciando recolección principal:")
    log.info(f"  Índices: {len(CC_INDEXES)} | Dominios: {len(CC_FOOTBALL_DOMAINS)} | Objetivo: {max_pages} páginas")

    for domain_name, url_pattern, target in CC_FOOTBALL_DOMAINS:
        if len(docs) >= max_pages:
            break

        # Cantidad para este dominio
        domain_target = min(target, pages_per_domain)
        log.info(f"  Dominio [{domain_name}]: buscando {domain_target} páginas...")

        # Buscar en múltiples índices CC
        records = cdx_search_multi_index(url_pattern,
                                          limit_per_index=domain_target,
                                          indexes=CC_INDEXES[:3])
        if not records:
            log.debug(f"    Sin registros para {url_pattern}")
            continue

        domain_count = 0
        # Procesar registros en paralelo (hasta 4 workers por dominio)
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = [
                ex.submit(_process_warc_record, rec, domain_name, seen)
                for rec in records[:domain_target * 2]  # extra margen
            ]
            for fut in as_completed(futures):
                if domain_count >= domain_target or len(docs) >= max_pages:
                    break
                try:
                    doc = fut.result()
                    if doc:
                        docs.append(doc)
                        domain_count += 1
                        log.info(f"    CC [{domain_name}] {doc['url'][:65]} ({len(doc['text'])}c)")
                except Exception as e:
                    log.debug(f"    Worker error: {e}")

        log.info(f"    ✓ {domain_name}: {domain_count} páginas")
        time.sleep(0.1)  # cortesía entre dominios

    log.info(f"Common Crawl total: {len(docs)} páginas de fútbol recolectadas")
    return docs


def search_entity_cc(entity_name: str, max_results: int = 5,
                     timeout_secs: int = 20) -> list[dict]:
    """
    Búsqueda dinámica en Common Crawl para una entidad específica.
    Usado como fallback cuando la entidad NO está en el índice pre-construido.

    Proceso:
    1. Normaliza el nombre a slug URL (ej. "Jordan Carrillo" → "jordan-carrillo")
    2. Busca en los 2 índices CC más recientes a través de dominios especializados
    3. Descarga y extrae texto de los registros encontrados
    4. Verifica que el texto mencione la entidad buscada
    5. Retorna documentos listos para generar respuesta

    Args:
        entity_name: Nombre de la entidad (jugador, equipo, etc.)
        max_results: Máximo de documentos a retornar
        timeout_secs: Tiempo máximo total (segundos)
    """
    slug = _normalize_slug(entity_name)
    docs: list[dict] = []
    seen: set = set()

    log.info(f"[CC-Live] Búsqueda dinámica: '{entity_name}' (slug: '{slug}')")

    entity_lower = entity_name.lower()
    entity_parts = [p for p in entity_lower.split() if len(p) >= 3]

    def _entity_in_text(text: str) -> bool:
        t = text.lower()
        return entity_lower in t or (
            entity_parts and all(p in t for p in entity_parts)
        )

    # Usar solo los 2 índices más recientes para velocidad
    live_indexes = CC_INDEXES[:2]

    for domain, pattern_template in CC_ENTITY_DOMAINS:
        if len(docs) >= max_results:
            break

        pattern = pattern_template.format(slug=slug)

        # Buscar en 2 índices
        records = cdx_search_multi_index(
            pattern, limit_per_index=3, indexes=live_indexes
        )
        if not records:
            continue

        for rec in records:
            if len(docs) >= max_results:
                break

            url      = rec.get("url", "")
            filename = rec.get("filename", "")
            offset   = int(rec.get("offset", 0))
            length   = int(rec.get("length", 0))

            if not url or not filename or length <= 0:
                continue

            html = fetch_warc_record(filename, offset, length)
            if not html:
                continue

            text = extract_text_from_html(html, max_len=3000, domain=domain)
            if not text or len(text) < 150:
                continue

            # Verificar que el texto menciona la entidad
            if not _entity_in_text(text):
                continue

            key = hashlib.md5(text[:200].encode()).hexdigest()
            if key in seen:
                continue
            seen.add(key)

            doc_type = _detect_type_from_url(url)
            docs.append({
                "url":    url,
                "text":   text,
                "source": "cc_live",
                "type":   doc_type,
                "entity": entity_name,
                "domain": domain,
            })
            log.info(f"  [CC-Live] Encontrado en {domain}: {url[:70]}")

    log.info(f"[CC-Live] Total para '{entity_name}': {len(docs)} documentos")
    return docs


# ════════════════════════════════════════════════════════════════════════════
# 5. DATOS DE RESPALDO AMPLIADOS
# ════════════════════════════════════════════════════════════════════════════

FALLBACK_DOCS = [
    # ── Conceptos básicos ──
    {"url":"https://es.wikipedia.org/wiki/Fútbol","text":"El fútbol asociación es el deporte más popular del mundo con más de 250 millones de jugadores en más de 200 países. Se juega entre dos equipos de once jugadores con el objetivo de introducir el balón en la portería rival. La FIFA es el organismo rector internacional fundado en 1904. Un partido dura 90 minutos divididos en dos tiempos de 45 minutos. El portero es el único jugador que puede tocar el balón con las manos dentro de su área.","source":"fallback","type":"concept"},
    {"url":"https://es.wikipedia.org/wiki/Offside","text":"El fuera de juego u offside es una regla del fútbol. Un jugador está en posición de offside si, en el momento del pase, se encuentra más cerca de la línea de meta rival que el balón y el penúltimo defensor. El VAR ha permitido revisar estas decisiones con precisión milimétrica, generando controversias. La regla no aplica en saques de banda, saques de puerta ni saques de esquina.","source":"fallback","type":"rule"},
    {"url":"https://es.wikipedia.org/wiki/Penalti","text":"El penalti es una patada directa a la portería desde el punto de penalti a 11 metros. Se concede cuando hay una infracción dentro del área propia. El portero debe permanecer en la línea de gol hasta que el balón sea golpeado. La tanda de penaltis resuelve eliminatorias empatadas. En el Mundial Qatar 2022, Argentina venció a Francia en la final mediante penaltis con marcador 3-3 en el tiempo reglamentario.","source":"fallback","type":"rule"},
    {"url":"https://es.wikipedia.org/wiki/VAR","text":"El VAR (Video Assistant Referee) es una tecnología implementada en el fútbol desde 2018 en el Mundial de Rusia. Permite al árbitro revisar en monitor cuatro tipos de situaciones: goles, penaltis, tarjetas rojas directas y errores de identidad. Ha reducido errores evidentes pero genera debate por decisiones de offside milimétrico y ralentización del juego.","source":"fallback","type":"technology"},
    # ── Jugadores actuales ──
    {"url":"https://dbpedia.org/resource/Lionel_Messi","text":"Lionel Andrés Messi Cuccittini, nacido el 24 de junio de 1987 en Rosario, Argentina, es considerado el mejor jugador de la historia del fútbol. Ganó 8 Balones de Oro, la Copa del Mundo 2022 con Argentina ante Francia, y anotó 672 goles en 778 partidos con el FC Barcelona. Actualmente juega en el Inter Miami CF de la MLS. Es el máximo goleador de la historia de la selección argentina con más de 109 goles. Conocido como Leo Messi, debutó en el FC Barcelona con 17 años y ganó 4 Ligas de Campeones. Es capitán de la selección argentina.","source":"fallback","type":"player","entity":"Lionel Messi"},
    {"url":"https://dbpedia.org/resource/Cristiano_Ronaldo","text":"Cristiano Ronaldo dos Santos Aveiro, nacido el 5 de febrero de 1985 en Funchal, Madeira, Portugal, ha marcado más de 900 goles en su carrera profesional. Ganó 5 Ligas de Campeones, 5 Balones de Oro y la Eurocopa 2016 con Portugal. Jugó en Sporting CP, Manchester United, Real Madrid, Juventus y actualmente en el Al Nassr de Arabia Saudita. Es el máximo goleador de la selección portuguesa con más de 130 goles. Conocido como CR7.","source":"fallback","type":"player","entity":"Cristiano Ronaldo"},
    {"url":"https://dbpedia.org/resource/Kylian_Mbappe","text":"Kylian Mbappé Lottin, nacido el 20 de diciembre de 1998 en Bondy, Francia, juega como delantero para el Real Madrid CF y la selección de Francia. Ganó el Mundial 2018 con Francia siendo el segundo jugador más joven en anotar en una final mundialista tras Pelé. Fue máximo goleador del mundo en 2023 y firmó con el Real Madrid en 2024. Anotó el hat-trick más famoso de la historia en la final del Mundial Qatar 2022. Mbappé es considerado el mejor jugador joven del mundo.","source":"fallback","type":"player","entity":"Kylian Mbappé"},
    {"url":"https://dbpedia.org/resource/Erling_Haaland","text":"Erling Braut Haaland, nacido el 21 de julio de 2000 en Leeds, es un delantero noruego del Manchester City y de la selección de Noruega. En su primera temporada en la Premier League (2022-23) batió el récord con 36 goles, ganando la Triple Corona junto al City. Su padre Alfie Haaland también fue futbolista profesional. Es conocido por su físico imponente: 1,94 metros y velocidad explosiva.","source":"fallback","type":"player","entity":"Erling Haaland"},
    {"url":"https://dbpedia.org/resource/Vinicius_Jr","text":"Vinícius José Paixão de Oliveira Júnior, conocido como Vinicius Jr o Vinicius Junior, nacido el 12 de julio de 2000 en São Gonçalo, Brasil, juega como extremo izquierdo en el Real Madrid y la selección de Brasil. Ganó dos Ligas de Campeones con el Real Madrid (2022, 2024) y fue nominado al Balón de Oro 2024. Es conocido por su velocidad, regate y el polémico baile que realiza al anotar goles. En 2023 fue víctima de incidentes racistas en España.","source":"fallback","type":"player","entity":"Vinicius Jr"},
    {"url":"https://dbpedia.org/resource/Jude_Bellingham","text":"Jude Victor William Bellingham, nacido el 29 de junio de 2003 en Stourbridge, Inglaterra, es mediocampista del Real Madrid y la selección inglesa. Fue el primer jugador en anotar un gol en cuatro competiciones distintas del Real Madrid en una sola temporada (2023-24). Llegó al Real Madrid desde el Borussia Dortmund por más de 100 millones de euros. Es considerado el mejor mediocampista joven del mundo.","source":"fallback","type":"player","entity":"Jude Bellingham"},
    {"url":"https://dbpedia.org/resource/Lamine_Yamal","text":"Lamine Yamal Nasraoui Ebana, nacido el 13 de julio de 2007 en Esplugues de Llobregat, España, es extremo derecho del FC Barcelona y la selección española. Ganó la Eurocopa 2024 con España, siendo el jugador más joven en marcar en una Eurocopa con tan solo 16 años. Es considerado el mayor talento de su generación.","source":"fallback","type":"player","entity":"Lamine Yamal"},
    {"url":"https://dbpedia.org/resource/Mohamed_Salah","text":"Mohamed Salah Hamed Mahrous Ghaly, nacido el 15 de junio de 1992 en Nagrig, Egipto, juega como extremo derecho en el Liverpool FC y en la selección de Egipto. Es el máximo goleador africano de la historia de la Premier League. Ganó la Liga de Campeones de la UEFA 2019 con el Liverpool. Ha ganado la Bota de Oro de la Premier League en múltiples ocasiones. Es considerado el mejor jugador africano de su generación y uno de los mejores extremos de la historia.","source":"fallback","type":"player","entity":"Mohamed Salah"},
    {"url":"https://dbpedia.org/resource/Kevin_De_Bruyne","text":"Kevin De Bruyne, nacido el 28 de junio de 1991 en Gante, Bélgica, es mediocampista del Manchester City y la selección belga. Es considerado uno de los mejores mediocampistas del mundo por su visión de juego y precisión en los pases. Ganó múltiples Premier Leagues, la Champions League 2023 y la Triple Corona con el Manchester City. Es el cerebro creativo del equipo de Pep Guardiola.","source":"fallback","type":"player","entity":"Kevin De Bruyne"},
    {"url":"https://dbpedia.org/resource/Rodri","text":"Rodrigo Hernández Cascante, conocido como Rodri, nacido el 22 de junio de 1996 en Madrid, España, es mediocampista defensivo del Manchester City y de la selección española. Ganó el Balón de Oro 2024, convirtiéndose en el primer centrocampista defensivo en ganarlo en décadas. Fue parte fundamental del equipo que ganó la Eurocopa 2024 con España y la Triple Corona 2023 con el Manchester City. Es considerado el mejor mediocampista del mundo en su posición.","source":"fallback","type":"player","entity":"Rodri"},
    {"url":"https://dbpedia.org/resource/Pedri","text":"Pedro González López, conocido como Pedri, nacido el 25 de noviembre de 2002 en Tegueste, Tenerife, España, es mediocampista del FC Barcelona y de la selección española. Ganó la Eurocopa 2024 con España. Es conocido por su técnica excepcional, visión de juego y capacidad de driblar bajo presión. Considerado uno de los mejores mediocampistas jóvenes del mundo. Fue finalista del Balón de Oro Joven en varias ocasiones.","source":"fallback","type":"player","entity":"Pedri"},
    {"url":"https://dbpedia.org/resource/Phil_Foden","text":"Philip Walter Foden, nacido el 28 de mayo de 2000 en Stockport, Inglaterra, es extremo y mediocampista del Manchester City y de la selección inglesa. Ganó la Triple Corona 2023 con el Manchester City y fue elegido jugador del año de la Premier League en la temporada 2023-24. Es conocido por su técnica refinada, su versatilidad y su capacidad goleadora. Fue formado en la academia del Manchester City desde niño.","source":"fallback","type":"player","entity":"Phil Foden"},
    {"url":"https://dbpedia.org/resource/Bukayo_Saka","text":"Bukayo Ayoyinka Saka, nacido el 5 de septiembre de 2001 en Ealing, Londres, es extremo del Arsenal FC y de la selección inglesa. Es considerado una de las mayores promesas del fútbol inglés y mundial. Con el Arsenal ha sido figura del equipo que retó la hegemonía del Manchester City en la Premier League. Ganó el premio Young Player of the Year de la Premier League. Es conocido por su velocidad, técnica y capacidad de marcar y asistir.","source":"fallback","type":"player","entity":"Bukayo Saka"},
    # ── Mexicanos ──
    {"url":"https://dbpedia.org/resource/Guillermo_Ochoa","text":"Francisco Guillermo Ochoa Magaña, nacido el 13 de julio de 1985 en Guadalajara, México, es portero de la selección mexicana. Es el portero más internacional de la historia de México con más de 140 caps. Participó en 6 Copas del Mundo consecutivas (2006-2026), un récord para un portero. Fue famoso por su actuación en el Mundial 2014 deteniendo un penalti a Neymar. Jugó en América, Ajax, Málaga, Ajaccio y varios clubes europeos.","source":"fallback","type":"player"},
    {"url":"https://dbpedia.org/resource/Hirving_Lozano","text":"Hirving Rodrigo Lozano Bahena 'Chucky', nacido el 30 de septiembre de 1995 en Ciudad de México, es extremo de la selección mexicana. Fue el primer mexicano en ganar la Eredivisie con el PSV Eindhoven. Jugó en el Napoli de la Serie A italiana y fue figura en el famoso gol contra Alemania en el Mundial 2018. Es conocido por su velocidad y capacidad de desborde.","source":"fallback","type":"player"},
    {"url":"https://dbpedia.org/resource/Hugo_Sanchez","text":"Hugo Sánchez Márquez, nacido el 11 de julio de 1958 en Ciudad de México, es considerado el mejor jugador mexicano de la historia. Jugó en el Real Madrid de 1985 a 1992, ganando 5 ligas consecutivas. Fue el máximo goleador de La Liga española en 5 ocasiones. Su acrobático estilo de marcar y sus volteretas al festejar goles lo hicieron legendario. Ganó el Pichichi 5 veces.","source":"fallback","type":"player","entity":"Hugo Sánchez"},
    # Mexicanos adicionales — Liga MX y selección nacional
    {"url":"https://es.wikipedia.org/wiki/Jordan_Carrillo","text":"Jordan Carrillo es un futbolista mexicano que juega como mediocampista en el Club Universidad Nacional (Pumas UNAM) de la Liga MX. Ha formado parte de la cantera y del primer equipo de los Pumas UNAM, siendo una de las figuras del mediocampo del club universitario. Es conocido por su capacidad de recuperación de balón, su visión de juego y su liderazgo en el mediocampo. Ha sido convocado a las categorías juveniles y mayores de la selección de fútbol de México. Pumas UNAM es su club formador y en el que ha desarrollado su carrera profesional en la primera división del fútbol mexicano.","source":"fallback","type":"player","entity":"Jordan Carrillo"},
    {"url":"https://es.wikipedia.org/wiki/Santiago_Gim%C3%A9nez","text":"Santiago Giménez Alarcón, nacido el 18 de abril de 2001 en Buenos Aires pero de nacionalidad mexicana, es un delantero centro que juega en el Feyenoord de la Eredivisie holandesa y en la selección de fútbol de México. Es hijo del exfutbolista Christian Giménez. Se convirtió en uno de los máximos goleadores del Feyenoord en la Eredivisie y en competiciones europeas, ganándose el apodo de 'El Bebote'. Es el delantero referente de la selección mexicana y uno de los jugadores más cotizados de Latinoamérica.","source":"fallback","type":"player","entity":"Santiago Giménez"},
    {"url":"https://es.wikipedia.org/wiki/Edson_%C3%81lvarez","text":"Edson Álvarez Noriega, nacido el 24 de octubre de 1997 en Ciudad de México, es un mediocampista defensivo que juega en el West Ham United de la Premier League inglesa y en la selección de fútbol de México, de la que es capitán. Comenzó su carrera en el Club América antes de fichar por el Ajax de Ámsterdam, donde ganó varios títulos de la Eredivisie. En 2023 se convirtió en uno de los jugadores mexicanos más caros de la historia al fichar por el West Ham United. Es considerado el mejor mediocampista defensivo de la historia reciente del fútbol mexicano.","source":"fallback","type":"player","entity":"Edson Álvarez"},
    {"url":"https://es.wikipedia.org/wiki/Henry_Mart%C3%ADn","text":"Henry Martín Verdugo, nacido el 19 de octubre de 1992 en Mazatlán, Sinaloa, es un delantero mexicano que juega en el Club América de la Liga MX. Es uno de los goleadores históricos del Club América y de la Liga MX. Ha sido convocado a la selección mexicana y es conocido por su olfato goleador y su capacidad de juego aéreo. Con el Club América ha ganado múltiples títulos de liga.","source":"fallback","type":"player","entity":"Henry Martín"},
    {"url":"https://es.wikipedia.org/wiki/Roberto_Alvarado","text":"Roberto Alvarado Hernández, nacido el 7 de julio de 1999 en Lagos de Moreno, Jalisco, es un mediocampista mexicano que juega en las Chivas de Guadalajara de la Liga MX y en la selección de fútbol de México. Es conocido por su velocidad, su capacidad de desborde y su potente disparo. Es una de las figuras del mediocampo de Guadalajara y ha sido parte importante de la selección mexicana en los últimos años.","source":"fallback","type":"player","entity":"Roberto Alvarado"},
    {"url":"https://es.wikipedia.org/wiki/C%C3%A9sar_Montes","text":"César Jasib Montes Garza, nacido el 24 de febrero de 1997 en Monterrey, Nuevo León, es un defensa central mexicano que juega en el RCD Espanyol de la Segunda División de España y en la selección de fútbol de México. Es uno de los centrales más sólidos de la selección mexicana y fue figura del Monterrey en la Liga MX antes de dar el salto a Europa.","source":"fallback","type":"player","entity":"César Montes"},
    {"url":"https://es.wikipedia.org/wiki/Orbell%C3%ADn_Pineda","text":"Orbelín Pineda García, nacido el 24 de marzo de 1996 en Tepic, Nayarit, es un mediocampista mexicano que juega en el AEK Athens de la Superliga de Grecia y en la selección de fútbol de México. Jugó en Cruz Azul y Chivas antes de fichar por el Celta de Vigo en España. Es conocido por su velocidad y capacidad creativa en el mediocampo. Ha sido una pieza clave en la selección mexicana.","source":"fallback","type":"player","entity":"Orbelín Pineda"},
    {"url":"https://es.wikipedia.org/wiki/Raúl_Jiménez","text":"Raúl Alonso Jiménez González, nacido el 5 de mayo de 1991 en Tepeji del Río, Hidalgo, es un delantero mexicano que juega en el Fulham FC de la Premier League inglesa y en la selección de fútbol de México. Jugó en el Club América y Atlético de Madrid antes de llegar al Wolverhampton Wanderers, donde se convirtió en ídolo y fue víctima de una grave lesión craneal en 2020 de la que se recuperó milagrosamente. Es el segundo máximo goleador de la historia de la selección mexicana.","source":"fallback","type":"player","entity":"Raúl Jiménez"},
    # ── Históricos ──
    {"url":"https://dbpedia.org/resource/Pele","text":"Edson Arantes do Nascimento, conocido como Pelé, nacido el 23 de octubre de 1940 en Três Corações, Brasil, y fallecido el 29 de diciembre de 2022, es considerado por muchos el mejor futbolista de todos los tiempos. Pelé ganó 3 Copas del Mundo con Brasil (1958, 1962, 1970). Marcó más de 1000 goles en su carrera. En 1958 se convirtió en el jugador más joven en ganar el Mundial con tan solo 17 años.","source":"fallback","type":"player","entity":"Pelé"},
    {"url":"https://dbpedia.org/resource/Diego_Maradona","text":"Diego Armando Maradona, nacido el 30 de octubre de 1960 en Lanús, Argentina, y fallecido el 25 de noviembre de 2020, es considerado junto a Pelé el mejor futbolista de la historia. Maradona ganó la Copa del Mundo 1986 con Argentina. En ese torneo marcó 'El gol del siglo' contra Inglaterra y también el polémico 'Gol de la mano de Dios'. Maradona jugó en Argentinos Juniors, Boca Juniors, FC Barcelona y el SSC Nápoles, donde ganó 2 Scudetti.","source":"fallback","type":"player","entity":"Diego Maradona"},
    {"url":"https://dbpedia.org/resource/Ronaldinho","text":"Ronaldo de Assis Moreira, conocido como Ronaldinho, nacido el 21 de marzo de 1980 en Porto Alegre, Brasil, ganó el Balón de Oro en 2005. Ronaldinho fue el artífice del FC Barcelona entre 2003 y 2008, ganando 2 Ligas y una Champions League. Ganó el Mundial 2002 con Brasil. Era conocido por su desborde, sus regates imposibles y su sonrisa permanente. Ronaldinho es considerado el jugador más elegante y entretenido de su generación.","source":"fallback","type":"player","entity":"Ronaldinho"},
    # ── Clubes ──
    {"url":"https://dbpedia.org/resource/Real_Madrid_CF","text":"El Real Madrid Club de Fútbol, fundado en 1902 en Madrid, España, es el club más exitoso de la historia de la UEFA Champions League con 15 títulos europeos. Ha ganado 36 Ligas españolas y 20 Copas del Rey. Su estadio Santiago Bernabéu tiene capacidad para más de 80,000 espectadores tras su remodelación en 2023. Jugadores históricos: Di Stéfano, Puskas, Zidane, Ronaldo Nazário, Raúl, Cristiano Ronaldo. Actuales: Vinicius Jr, Bellingham, Mbappé.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/FC_Barcelona","text":"El Fútbol Club Barcelona, fundado en 1899 en Barcelona, España, ha ganado 5 Ligas de Campeones de la UEFA, 27 Ligas españolas y 31 Copas del Rey. Su estadio Spotify Camp Nou es el mayor de Europa. La Academia La Masia formó a Messi, Xavi, Iniesta y muchos más. Famoso por el tiki-taka de Guardiola (2008-2012). En 2023 inauguró el nuevo Camp Nou reformado.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/Manchester_City_FC","text":"El Manchester City Football Club, fundado en 1880 en Mánchester, Inglaterra, ganó la Triple Corona en 2023 (Premier League, FA Cup y Champions League) bajo Pep Guardiola. Ha ganado 9 Premier Leagues. Propiedad del grupo Abu Dhabi United Group desde 2008. Jugadores clave: Erling Haaland, Kevin De Bruyne, Phil Foden. Juegan en el Etihad Stadium con capacidad para 53,000 espectadores.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/Liverpool_FC","text":"El Liverpool Football Club, fundado en 1892, es uno de los clubes más exitosos de Inglaterra con 19 Premier Leagues y 6 Ligas de Campeones de la UEFA. El mítico estadio Anfield acoge el himno 'You'll Never Walk Alone'. Bajo Jürgen Klopp ganaron la Champions 2019 y la Premier League 2020. Jugadores históricos: Kenny Dalglish, Steven Gerrard, Mohamed Salah, Luis Suárez.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/Bayern_Munich","text":"El Fußball-Club Bayern München, fundado en 1900 en Múnich, Alemania, es el club más exitoso de la Bundesliga con más de 32 títulos. Ha ganado 6 Ligas de Campeones de la UEFA. Juega en el Allianz Arena. Jugadores históricos: Franz Beckenbauer, Gerd Müller, Oliver Kahn, Robert Lewandowski. Actualmente entrenado por Vincent Kompany. El club aplica el modelo 50+1 que garantiza control de los socios.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/Club_America","text":"El Club de Fútbol América, fundado en 1916 en Ciudad de México, es el club de fútbol más popular de México con 14 títulos de liga. Juega en el Estadio Azteca, el mayor estadio de América Latina. Es conocido como 'Las Águilas'. Ha ganado múltiples CONCACAF Champions Cups y es el club más valioso de la Liga MX. Rivales históricos: Guadalajara (el Clásico Nacional) y Cruz Azul.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/Chivas_Guadalajara","text":"El Club Deportivo Guadalajara, conocido como Chivas, fundado en 1906, es el único equipo de la Liga MX que tiene política de alinear solo jugadores mexicanos. Ha ganado 12 títulos de liga. Es el rival histórico del América en el Clásico Nacional, el partido más visto en México. Tiene su propio estadio, el Akron Stadium. Es conocido como 'el Rebaño Sagrado'.","source":"fallback","type":"club"},
    {"url":"https://dbpedia.org/resource/Boca_Juniors","text":"El Club Atlético Boca Juniors, fundado en 1905 en Buenos Aires, Argentina, es uno de los clubes más populares del mundo. Ha ganado 6 Copas Libertadores y 34 títulos de liga argentina. Su estadio La Bombonera en el barrio de La Boca es famoso por su atmósfera eléctrica. El Superclásico contra River Plate es considerado el partido de fútbol con más seguidores en el mundo.","source":"fallback","type":"club"},
    # ── Competiciones ──
    {"url":"https://dbpedia.org/resource/UEFA_Champions_League","text":"La UEFA Champions League es el torneo de clubes más prestigioso del mundo, organizado por la UEFA anualmente. El Real Madrid CF es el club más exitoso con 15 títulos. La final se disputa en estadio neutral. Fue fundada en 1955 como Copa de Europa. Los clubes clasifican según su posición en sus ligas nacionales. El himno oficial 'Champions League Anthem' es una de las músicas más reconocibles del deporte mundial.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/FIFA_World_Cup","text":"La Copa Mundial de la FIFA es el torneo de selecciones nacionales más importante, celebrado cada cuatro años. Brasil es el país más exitoso con 5 títulos (1958, 1962, 1970, 1994, 2002). La Copa 2022 en Qatar fue ganada por Argentina ante Francia en una final épica que terminó 3-3 y se resolvió en penaltis. La Copa 2026 se celebrará en Estados Unidos, Canadá y México con 48 selecciones por primera vez.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/Premier_League","text":"La Premier League es la primera división del fútbol inglés y la liga más vista del mundo en más de 188 países. Fue fundada en 1992. Manchester City es el campeón más reciente. Manchester United es el club histórico más laureado con 20 títulos. La temporada va de agosto a mayo con 20 equipos. Los últimos 3 descienden. Produce los mayores ingresos televisivos del fútbol mundial.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/La_Liga","text":"LaLiga EA Sports es la primera división del fútbol español, fundada en 1929. Real Madrid y FC Barcelona dominan históricamente con más de 35 y 27 títulos respectivamente. El Clásico Real Madrid vs Barcelona es el partido más seguido del mundo. En la temporada 2023-24 el Real Madrid ganó el título. Otros clubes históricos: Atlético de Madrid, Athletic Club, Sevilla, Valencia.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/Bundesliga","text":"La Bundesliga es la primera división del fútbol alemán, fundada en 1963. El Bayern de Múnich ha ganado más de 32 títulos. La liga tiene el mayor promedio de asistencia de Europa gracias a precios accesibles y la regla 50+1. Der Klassiker es el enfrentamiento entre Bayern Múnich y Borussia Dortmund. Robert Lewandowski fue el máximo goleador histórico con 312 goles en la Bundesliga.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/Serie_A","text":"La Serie A es la primera división del fútbol italiano. La Juventus FC es el club más laureado con 36 títulos. El Inter de Milán ganó el último scudetto disponible. Los grandes clubes son Juventus, Inter, AC Milan, Roma, Napoli y Lazio. El Napoli ganó su tercer scudetto en 2023 con el goleador nigeriano Victor Osimhen. La Serie A fue dominante en los años 90 con figuras como Baggio, Maldini y Ronaldo Nazário.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/Liga_MX","text":"La Liga MX es la primera división del fútbol mexicano. Es la liga con mayor asistencia promedio de América. El Club América es el más laureado con 14 títulos. El Estadio Azteca en Ciudad de México fue sede de dos Copas del Mundo (1970 y 1986). La Liga MX tiene dos torneos anuales: Apertura (julio-diciembre) y Clausura (enero-junio). Los clubes más exitosos son América, Guadalajara, Cruz Azul y Toluca.","source":"fallback","type":"competition"},
    {"url":"https://dbpedia.org/resource/Copa_America","text":"La Copa América es el torneo de selecciones más antiguo del mundo, organizado por CONMEBOL desde 1916. Argentina es el campeón más exitoso con 16 títulos incluyendo la Copa América 2021 y 2024. Uruguay tiene 15 títulos. La Copa 2024 fue ganada por Argentina en Estados Unidos, siendo el torneo más visto de la historia de la competición. Brasil, Uruguay, Chile y Colombia también han sido campeones.","source":"fallback","type":"competition"},
    # ── Entrenadores ──
    {"url":"https://dbpedia.org/resource/Pep_Guardiola","text":"Josep 'Pep' Guardiola Sala, nacido el 18 de enero de 1971 en Santpedor, España, es considerado el mejor entrenador de la historia del fútbol moderno. Guardiola ganó la Triple Corona con el FC Barcelona (2009), el triplete con el Bayern Múnich (2013) y la Triple Corona con el Manchester City (2023). Ha ganado más de 35 títulos como entrenador. Guardiola revolucionó el fútbol con el tiki-taka y la presión alta.","source":"fallback","type":"manager","entity":"Pep Guardiola"},
    {"url":"https://dbpedia.org/resource/Carlo_Ancelotti","text":"Carlo Ancelotti, nacido el 10 de junio de 1959 en Reggiolo, Italia, es el único entrenador en ganar las ligas de los 5 grandes campeonatos europeos. Ancelotti ha ganado 4 Ligas de Campeones como entrenador (AC Milan 2003, 2007 y Real Madrid 2014, 2022, 2024). Actualmente dirige al Real Madrid. Ancelotti también dirigió al Chelsea, Bayern Múnich, PSG y Napoli.","source":"fallback","type":"manager","entity":"Carlo Ancelotti"},
    {"url":"https://dbpedia.org/resource/Diego_Simeone","text":"Diego Pablo Simeone, nacido el 28 de abril de 1970 en Buenos Aires, Argentina, dirige el Atlético de Madrid desde 2011. Simeone ha ganado 2 Ligas españolas, la UEFA Europa League 2 veces y la UEFA Super Cup. Su estilo defensivo e intenso es conocido como 'Cholismo'. Como jugador, Simeone ganó el Mundial 1994 con Argentina y jugó en el Atlético de Madrid, Lazio y el Inter de Milán.","source":"fallback","type":"manager","entity":"Diego Simeone"},
    {"url":"https://dbpedia.org/resource/Lionel_Scaloni","text":"Lionel Sebastián Scaloni, nacido el 16 de mayo de 1978 en Pujato, Santa Fe, Argentina, es el director técnico de la selección de fútbol de Argentina desde 2018. Scaloni llevó a Argentina a ganar la Copa América 2021, la Finalissima 2022 y la Copa Mundial FIFA Qatar 2022, donde Argentina venció a Francia en la final. También ganó la Copa América 2024. Como jugador se desempeñaba como lateral derecho o mediocampista. Bajo la dirección de Scaloni, Lionel Messi ganó su primer título mundialista con Argentina en Qatar 2022.","source":"fallback","type":"manager","entity":"Lionel Scaloni"},
    # ── Estadios ──
    {"url":"https://dbpedia.org/resource/Estadio_Azteca","text":"El Estadio Azteca en Ciudad de México, con capacidad para 87,000 espectadores, es el estadio más grande de América Latina y uno de los más famosos del mundo. Sede del Mundial 1970 (ganado por Brasil) y del Mundial 1986 (ganado por Argentina con el gol de Maradona). Será sede nuevamente del Mundial 2026. Aquí Maradona marcó tanto 'El gol del siglo' como 'La mano de Dios'.","source":"fallback","type":"stadium"},
    {"url":"https://dbpedia.org/resource/Santiago_Bernabeu","text":"El Estadio Santiago Bernabéu en Madrid, España, fue completamente renovado en 2023 con un techo retráctil y capacidad para más de 81,000 espectadores. Es la sede del Real Madrid CF y ha albergado múltiples finales de la Copa de Europa. Fue nombrado en honor al histórico presidente del Real Madrid Santiago Bernabéu. En 2022 fue elegido el mejor estadio del mundo.","source":"fallback","type":"stadium"},
    # ── Balón de Oro ──
    {"url":"https://dbpedia.org/resource/Ballon_dOr","text":"El Balón de Oro es el premio individual más prestigioso del fútbol, otorgado por la revista France Football desde 1956. Lionel Messi lo ha ganado un récord de 8 veces (2009, 2010, 2011, 2012, 2015, 2019, 2021, 2023). Cristiano Ronaldo lo ha ganado 5 veces. En 2024 lo ganó Rodri del Manchester City, siendo el primer centrocampista defensivo en ganarlo desde Ronaldinho.","source":"fallback","type":"award"},
]


# ════════════════════════════════════════════════════════════════════════════
# 6. THESPORTSDB — API gratuita de datos estructurados
# ════════════════════════════════════════════════════════════════════════════

THESPORTSDB_BASE = "https://www.thesportsdb.com/api/v1/json/3"

# Jugadores y equipos a consultar en TheSportsDB
TSDB_PLAYERS = [
    # Estrellas mundiales
    "Lionel Messi", "Cristiano Ronaldo", "Kylian Mbappe", "Erling Haaland",
    "Vinicius Jr", "Jude Bellingham", "Lamine Yamal", "Pedri", "Rodri",
    "Phil Foden", "Bukayo Saka", "Harry Kane", "Son Heung-min",
    "Mohamed Salah", "Kevin De Bruyne", "Robert Lewandowski", "Luka Modric",
    "Neymar Jr", "Raphinha", "Rodrygo", "Endrick",
    # Mexicanos
    "Guillermo Ochoa", "Hirving Lozano", "Raul Jimenez", "Edson Alvarez",
    "Santiago Gimenez", "Henry Martin", "Roberto Alvarado", "Cesar Montes",
    "Carlos Vela", "Andres Guardado", "Javier Hernandez",
    # Argentinos
    "Lautaro Martinez", "Rodrigo De Paul", "Angel Di Maria", "Julian Alvarez",
    "Paulo Dybala",
    # Colombianos / otros
    "James Rodriguez", "Falcao", "Luis Diaz",
    # Históricos
    "Pele", "Diego Maradona", "Ronaldo Nazario", "Zinedine Zidane",
    "Ronaldinho", "Johan Cruyff", "Roberto Carlos",
]

TSDB_TEAMS = [
    "Real Madrid", "Barcelona", "Manchester City", "Liverpool",
    "Bayern Munich", "Paris Saint-Germain", "Arsenal", "Chelsea",
    "Juventus", "AC Milan", "Inter Milan", "Atletico Madrid",
    "Club America", "Guadalajara", "Cruz Azul", "Pumas UNAM",
    "Tigres UANL", "Monterrey", "Boca Juniors", "River Plate",
    "Flamengo", "Manchester United", "Borussia Dortmund",
]


def _tsdb_player_to_doc(player: dict) -> Optional[dict]:
    """Convierte respuesta de TheSportsDB a documento."""
    name = player.get("strPlayer", "").strip()
    if not name:
        return None

    parts = [f"{name} es un futbolista profesional."]
    nat   = player.get("strNationality", "")
    pos   = player.get("strPosition", "")
    team  = player.get("strTeam", "")
    born  = player.get("dateBorn", "")
    desc  = (player.get("strDescriptionES") or
             player.get("strDescriptionEN") or "").strip()

    if nat:   parts.append(f"Nacionalidad: {nat}.")
    if pos:   parts.append(f"Posición: {pos}.")
    if team:  parts.append(f"Club actual: {team}.")
    if born:  parts.append(f"Fecha de nacimiento: {born}.")
    if desc:  parts.append(desc[:600])

    text = " ".join(parts)
    if len(text) < 60:
        return None

    player_id = player.get("idPlayer", "")
    url = f"https://www.thesportsdb.com/player.php?id={player_id}"
    return {
        "url":    url,
        "text":   text,
        "source": "thesportsdb",
        "type":   "player",
        "entity": name,
    }


def _tsdb_team_to_doc(team: dict) -> Optional[dict]:
    """Convierte respuesta de equipo de TheSportsDB a documento."""
    name = team.get("strTeam", "").strip()
    if not name:
        return None

    parts = [f"{name} es un club de fútbol profesional."]
    country = team.get("strCountry", "")
    league  = team.get("strLeague", "")
    founded = team.get("intFormedYear", "")
    stadium = team.get("strStadium", "")
    desc    = (team.get("strDescriptionES") or
               team.get("strDescriptionEN") or "").strip()

    if country:  parts.append(f"País: {country}.")
    if league:   parts.append(f"Liga: {league}.")
    if founded:  parts.append(f"Fundado en: {founded}.")
    if stadium:  parts.append(f"Estadio: {stadium}.")
    if desc:     parts.append(desc[:600])

    text = " ".join(parts)
    if len(text) < 60:
        return None

    team_id = team.get("idTeam", "")
    url = f"https://www.thesportsdb.com/team.php?id={team_id}"
    return {
        "url":    url,
        "text":   text,
        "source": "thesportsdb",
        "type":   "club",
        "entity": name,
    }


def harvest_thesportsdb() -> list[dict]:
    """Recolecta datos de jugadores y equipos desde TheSportsDB API (gratuita)."""
    docs = []
    seen = set()

    def _add(doc: Optional[dict]):
        if not doc:
            return
        key = hashlib.md5(doc["text"][:200].encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            docs.append(doc)

    # Jugadores
    log.info(f"  TheSportsDB: consultando {len(TSDB_PLAYERS)} jugadores...")
    for name in TSDB_PLAYERS:
        try:
            r = SESSION.get(
                f"{THESPORTSDB_BASE}/searchplayers.php",
                params={"p": name}, timeout=10,
            )
            r.raise_for_status()
            players = (r.json().get("player") or [])
            for p in players[:2]:  # máx 2 resultados por búsqueda
                _add(_tsdb_player_to_doc(p))
        except Exception as e:
            log.debug(f"TheSportsDB player error ({name}): {e}")
        time.sleep(0.15)

    # Equipos
    log.info(f"  TheSportsDB: consultando {len(TSDB_TEAMS)} equipos...")
    for name in TSDB_TEAMS:
        try:
            r = SESSION.get(
                f"{THESPORTSDB_BASE}/searchteams.php",
                params={"t": name}, timeout=10,
            )
            r.raise_for_status()
            teams = (r.json().get("teams") or [])
            for t in teams[:1]:  # máx 1 por búsqueda
                _add(_tsdb_team_to_doc(t))
        except Exception as e:
            log.debug(f"TheSportsDB team error ({name}): {e}")
        time.sleep(0.15)

    log.info(f"TheSportsDB total: {len(docs)} entidades")
    return docs


# ════════════════════════════════════════════════════════════════════════════
# 7. PIPELINE PRINCIPAL
# ════════════════════════════════════════════════════════════════════════════

def run_ingestion(use_live: bool = True,
                  use_cc: bool = True,
                  cc_pages: int = 300,
                  max_docs: int = 8000) -> list[dict]:
    """
    Pipeline de ingesta completo.

    Args:
        use_live:  Si True, consulta DBpedia, Wikipedia, Wikidata, TheSportsDB.
        use_cc:    Si True, ejecuta la recolección masiva de Common Crawl.
                   Common Crawl es el motor principal: 300+ páginas de 30+ dominios.
        cc_pages:  Número objetivo de páginas a recolectar de Common Crawl.
        max_docs:  Máximo total de documentos únicos.
    """
    docs = []
    seen = set()

    def add_docs(new_docs: list[dict], label: str):
        added = 0
        for d in new_docs:
            key = hashlib.md5(d.get("text", "")[:200].encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                docs.append(d)
                added += 1
            if len(docs) >= max_docs:
                break
        log.info(f"  [{label}] +{added} docs (total: {len(docs)})")

    # Siempre agregar datos de respaldo primero (base garantizada)
    add_docs(FALLBACK_DOCS, "Respaldo")

    if use_live:
        log.info("═══ Iniciando ingesta en vivo ═══")

        log.info("── Common Crawl (motor principal) ──")
        if use_cc:
            add_docs(harvest_common_crawl(max_pages=cc_pages), "CommonCrawl")
        else:
            log.info("  Common Crawl omitido (use_cc=False)")

        log.info("── TheSportsDB (datos estructurados) ──")
        add_docs(harvest_thesportsdb(), "TheSportsDB")

        log.info("── Wikipedia (artículos de fútbol) ──")
        add_docs(harvest_wikipedia(), "Wikipedia")

        log.info("── DBpedia SPARQL ──")
        add_docs(harvest_dbpedia(), "DBpedia")

        log.info("── Wikidata ──")
        add_docs(harvest_wikidata(), "Wikidata")

    log.info(f"Total documentos únicos: {len(docs)}")

    DATA_DIR.mkdir(exist_ok=True)
    out_path = DATA_DIR / "documents.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    log.info(f"Guardado en {out_path}")

    return docs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SourceSeek Ingesta")
    parser.add_argument("--no-cc",   action="store_true", help="Omitir Common Crawl")
    parser.add_argument("--offline", action="store_true", help="Solo datos de respaldo")
    parser.add_argument("--cc-pages", type=int, default=300, help="Páginas CC objetivo")
    args = parser.parse_args()

    docs = run_ingestion(
        use_live=not args.offline,
        use_cc=not args.no_cc,
        cc_pages=args.cc_pages,
    )
    print(f"\n✓ Ingesta completa: {len(docs)} documentos")
