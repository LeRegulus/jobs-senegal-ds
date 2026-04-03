import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re
from datetime import date
from pathlib import Path

# ── Configuration ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
DELAY_SECONDS = 1.5   # pause entre chaque requête
OUTPUT_DIR    = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ── Parsing d'une seule carte d'offre ───────────────────────
def parse_job_card(card: BeautifulSoup) -> dict:
    """Extrait les champs d'un élément <div class='job-card'>."""

    def safe_text(tag) -> str:
        # Retourne "" si le tag est None ou vide
        return tag.get_text(separator=" ").strip() if tag else ""

    title    = safe_text(card.find("h2", class_="job-title"))
    company  = safe_text(card.find("span", class_="company"))
    location = safe_text(card.find("span", class_="location"))
    salary   = safe_text(card.find("span", class_="salary"))
    contract = safe_text(card.find("span", class_="contract"))
    desc     = safe_text(card.find("p",    class_="description"))

    # Récupère le lien relatif de l'offre
    link_tag = card.find("a", class_="job-link")
    link     = link_tag.get("href", "") if link_tag else ""

    return {
        "title":    title,
        "company":  company,
        "location": location,
        "salary_raw": salary,
        "contract": contract,
        "description": desc,
        "link":    link,
        "scraped_at": str(date.today()),
    }


# ── Scraping depuis un fichier HTML local (mock) ────────────
def scrape_local(html_path: str) -> list[dict]:
    """Scrape depuis un fichier HTML local — pour les tests."""
    logger.info(f"Lecture fichier local : {html_path}")

    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    cards = soup.find_all("div", class_="job-card")
    logger.info(f"Cartes trouvées : {len(cards)}")

    jobs = [parse_job_card(c) for c in cards]
    return [j for j in jobs if j["title"]]  # filtre vides


# ── Scraping depuis une vraie URL (emploi.sn) ───────────────
def scrape_url(url: str, max_pages: int = 5) -> list[dict]:

    if not is_scraping_allowed("https://www.emploi.sn"):
        raise PermissionError("Scraping interdit par robots.txt")

    """Scrape plusieurs pages d'un job board en ligne."""
    all_jobs = []

    for page in range(1, max_pages + 1):
        page_url = f"{url}?page={page}"
        logger.info(f"Scraping page {page} : {page_url}")

        try:
            resp = requests.get(
                page_url,
                headers=HEADERS,
                timeout=10
            )
            resp.raise_for_status()  # lève une exception si 4xx/5xx

        except requests.exceptions.HTTPError as e:
            logger.error(f"Erreur HTTP page {page} : {e}")
            break
        except requests.exceptions.ConnectionError:
            logger.error("Connexion impossible — vérifie internet")
            break
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout page {page} — on continue")
            continue

        soup  = BeautifulSoup(resp.text, "html.parser")
        cards = soup.find_all("div", class_="job-card")

        if not cards:
            logger.info("Aucune offre sur cette page — fin")
            break

        page_jobs = [parse_job_card(c) for c in cards]
        all_jobs.extend(page_jobs)
        logger.info(f"  → {len(page_jobs)} offres collectées")

        time.sleep(DELAY_SECONDS)  # pause polie

    return all_jobs


# ── Sauvegarde CSV ──────────────────────────────────────────
def save_to_csv(jobs: list[dict], filename: str = None) -> str:
    """Sauvegarde la liste de dicts en CSV horodaté."""
    if not jobs:
        logger.warning("Aucune offre à sauvegarder")
        return ""

    filename = filename or f"jobs_raw_{date.today()}.csv"
    path     = OUTPUT_DIR / filename

    df = pd.DataFrame(jobs)
    df.to_csv(path, index=False, encoding="utf-8-sig")

    logger.info(f"Sauvegardé : {path} ({len(df)} lignes)")
    return str(path)

def get_with_retry(url, headers, max_retries=3):
    """Réessaie jusqu'à 3 fois avec délai croissant."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt  # 1s, 2s, 4s
            logger.warning(f"Tentative {attempt+1} échouée, attente {wait}s")
            time.sleep(wait)
    return None

# ── 2. Déduplication par titre + entreprise ─────────────────
def deduplicate(jobs: list[dict]) -> list[dict]:
    """Supprime les doublons (même titre + même entreprise)."""
    seen = set()
    unique = []
    for job in jobs:
        key = (job["title"].lower(), job["company"].lower())
        if key not in seen:
            seen.add(key)
            unique.append(job)
    logger.info(f"Doublons supprimés : {len(jobs)-len(unique)}")
    return unique


# ── 3. Vérifier robots.txt avant de scraper ─────────────────
from urllib.robotparser import RobotFileParser

def is_scraping_allowed(base_url: str, path: str = "/") -> bool:
    """Vérifie que le scraping est autorisé sur ce chemin."""
    rp = RobotFileParser()
    rp.set_url(f"{base_url}/robots.txt")
    try:
        rp.read()
        allowed = rp.can_fetch("*", base_url + path)
        logger.info(f"robots.txt → scraping {'autorisé' if allowed else 'INTERDIT'}")
        return allowed
    except:
        logger.warning("Impossible de lire robots.txt — prudence")
        return True

# ── Point d'entrée principal ────────────────────────────────
if __name__ == "__main__":

    # --- MODE 1 : test sur fichier local (recommandé pour débuter)
    jobs = scrape_local("data/mock_jobs.html")

    # --- MODE 2 : vrai scraping en ligne (décommenter quand prêt)
    # jobs = scrape_url("https://www.emploi.sn/offres-emploi", max_pages=3)

    # Aperçu console
    for j in jobs:
        print(f"✓ {j['title']} — {j['company']} ({j['location']})")

    print(f"\nTotal : {len(jobs)} offres")

    # Sauvegarde
    path = save_to_csv(jobs)
    print(f"\nFichier CSV : {path}")

    # Aperçu DataFrame
    df = pd.DataFrame(jobs)
    print("\n--- Aperçu ---")
    print(df[["title", "company", "location", "salary_raw"]])