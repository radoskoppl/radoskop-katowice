#!/usr/bin/env python3
"""
Scraper danych głosowań Rady Miasta Katowice.

Źródło: katowice.esesja.pl (platforma eSesja)
eSesja ma dostęp do głosowań bez JavaScript.

Struktura eSesja:
  1. Lista sesji: https://katowice.esesja.pl/glosowania/
  2. Sesja: /glosowania/{session_id} — lista głosowań
  3. Głosowanie: /glosowania/{vote_id}/{hash} — wyniki per radny
     — Format: tabela z kolumnami: Lp. / Radny / Głos (ZA / PRZECIW / WSTRZYMAŁ SIĘ / NIEOBECNY)

UWAGA: Uruchom lokalnie — sandbox Cowork blokuje domeny.

Użycie:
    pip install requests beautifulsoup4 lxml
    python scrape_katowice.py [--output docs/data.json] [--profiles docs/profiles.json]
"""

import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from itertools import combinations
from pathlib import Path

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Zainstaluj: pip install beautifulsoup4 lxml")
    sys.exit(1)

try:
    import requests

def compact_named_votes(output):
    """Convert named_votes from string arrays to indexed format for smaller JSON."""
    for kad in output.get("kadencje", []):
        names = set()
        for v in kad.get("votes", []):
            nv = v.get("named_votes", {})
            for cat_names in nv.values():
                for n in cat_names:
                    if isinstance(n, str):
                        names.add(n)
        if not names:
            continue
        index = sorted(names, key=lambda n: n.split()[-1] + " " + n)
        name_to_idx = {n: i for i, n in enumerate(index)}
        kad["councilor_index"] = index
        for v in kad.get("votes", []):
            nv = v.get("named_votes", {})
            for cat in nv:
                nv[cat] = sorted(name_to_idx[n] for n in nv[cat] if isinstance(n, str) and n in name_to_idx)
    return output



def save_split_output(output, out_path):
    """Save output as split files: data.json (index) + kadencja-{id}.json per kadencja."""
    import json as _json
    from pathlib import Path as _Path
    compact_named_votes(output)
    out_path = _Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stubs = []
    for kad in output.get("kadencje", []):
        kid = kad["id"]
        stubs.append({"id": kid, "label": kad.get("label", f"Kadencja {kid}")})
        kad_path = out_path.parent / f"kadencja-{kid}.json"
        with open(kad_path, "w", encoding="utf-8") as f:
            _json.dump(kad, f, ensure_ascii=False, separators=(",", ":"))
    index = {
        "generated": output.get("generated", ""),
        "default_kadencja": output.get("default_kadencja", ""),
        "kadencje": stubs,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        _json.dump(index, f, ensure_ascii=False, separators=(",", ":"))


except ImportError:
    print("Zainstaluj: pip install requests")
    sys.exit(1)


# ============================================================================
# Config
# ============================================================================

ESESJA_BASE = "https://katowice.esesja.pl"
ESESJA_VOTES_LIST = f"{ESESJA_BASE}/glosowania/"

KADENCJE = {
    "2024-2029": {
        "label": "IX kadencja (2024–2029)",
        "start": "2024-05-07",
    }
}

HEADERS = {
    "User-Agent": "Radoskop/1.0 (https://katowice.radoskop.pl; kontakt@radoskop.pl)",
    "Accept": "text/html",
}

DELAY = 1.0

# Placeholder for councilors with clubs — fill from profiles.json or manual list
COUNCILORS = {
    # KO (Koalicja Obywatelska) - 14 members
    "Beata Bala": "KO",
    "Patryk Białas": "KO",
    "Łukasz Borkowski": "KO",
    "Elżbieta Grodzka-Łopuszyńska": "KO",
    "Urszula Machowska": "KO",
    "Jarosław Makowski": "KO",
    "Tomasz Maśnica": "KO",
    "Ewa Sadkowska": "KO",
    "Magdalena Skwarek": "KO",
    "Adam Szymczyk": "KO",
    "Andrzej Warmuz": "KO",
    "Barbara Wnęk-Gabor": "KO",
    "Magdalena Wieczorek": "KO",
    "Adam Lejman-Gąska": "KO",
    # PiS (Prawo i Sprawiedliwość) - 5 members
    "Leszek Piechota": "PiS",
    "Krystyna Panek": "PiS",
    "Piotr Pietrasz": "PiS",
    "Mariusz Skiba": "PiS",
    "Piotr Trząski": "PiS",
    # Forum Samorządowe i Marcin Krupa - 9 members
    "Jacek Kalisz": "Forum",
    "Maciej Biskupski": "Forum",
    "Łukasz Hankus": "Forum",
    "Dawid Kamiński": "Forum",
    "Barbara Mańdok": "Forum",
    "Borys Pronobis": "Forum",
    "Maria Ryś": "Forum",
    "Adam Skoworon": "Forum",
    "Krzysztof Pieczyński": "Forum",
    "Roch Sobula": "Forum",
    "Damian Stępień": "Forum",
}


# ============================================================================
# Step 1: Fetch and parse session list (voting archive)
# ============================================================================

def fetch_votes_list(session, debug=False):
    """Pobiera listę wszystkich głosowań z archiwum."""
    if debug:
        print(f"[DEBUG] GET {ESESJA_VOTES_LIST}")

    try:
        resp = session.get(ESESJA_VOTES_LIST, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"BŁĄD: Nie można pobrać listy głosowań: {e}")
        raise


def parse_votes_list(html, debug=False):
    """Parsuje listę sesji/głosowań z archiwum.

    Szuka tabel z głosowaniami — każdy wiersz to głosowanie z:
    - Linkiem do szczegółów
    - Datą sesji
    - Liczbą głosów
    """
    soup = BeautifulSoup(html, "html.parser")

    sessions = []
    votes_meta = []

    # Szukamy tabel z danymi głosowań
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")[1:]  # Skip header

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Pierwsza komórka zwykle zawiera numer/datę, ostatnia link
            link_cell = cells[-1]
            a = link_cell.find("a")

            if not a:
                continue

            href = a.get("href", "")
            if not href.startswith("/glosowania/"):
                continue

            # Ekstraktuj vote_id i hash z URL
            m = re.search(r"/glosowania/(\d+)/([a-f0-9]+)", href)
            if not m:
                continue

            vote_id = m.group(1)
            vote_hash = m.group(2)

            # Tekst z komórek
            text_parts = [cell.get_text(strip=True) for cell in cells]

            votes_meta.append({
                "vote_id": vote_id,
                "vote_hash": vote_hash,
                "url": ESESJA_BASE + href,
                "text": text_parts,
            })

    if debug:
        print(f"[DEBUG] Znaleziono {len(votes_meta)} głosowań")

    return votes_meta


# ============================================================================
# Step 2: Fetch and parse individual vote results
# ============================================================================

def fetch_vote_detail(session, vote_url, debug=False):
    """Pobiera szczegóły jednego głosowania."""
    if debug:
        print(f"[DEBUG] GET {vote_url}")

    try:
        resp = session.get(vote_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        if debug:
            print(f"[DEBUG] Błąd pobrania {vote_url}: {e}")
        return None


def parse_vote_detail(html, vote_url, debug=False):
    """Parsuje szczegóły głosowania.

    Szuka:
    - Tematu głosowania
    - Tabeli z imieniem radnego i głosem (ZA/PRZECIW/WSTRZYMAŁ SIĘ/NIEOBECNY)
    """
    soup = BeautifulSoup(html, "html.parser")

    # Szukaj tematu
    topic = ""
    h1 = soup.find("h1")
    if h1:
        topic = h1.get_text(strip=True)
    else:
        # Spróbuj znaleźć w divach z klasą
        for div in soup.find_all("div", class_="title"):
            topic = div.get_text(strip=True)
            if topic:
                break

    # Parsuj tabelę z głosami
    votes_detail = {
        "topic": topic,
        "counts": {"za": 0, "przeciw": 0, "wstrzymal_sie": 0, "nieobecni": 0},
        "named_votes": {"za": [], "przeciw": [], "wstrzymal_sie": [], "brak_glosu": [], "nieobecni": []},
    }

    table = soup.find("table")
    if not table:
        return votes_detail

    rows = table.find_all("tr")[1:]  # Skip header

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        # Zwykle: Lp. / Radny / Głos
        name_cell = cells[0] if len(cells) >= 2 else None
        vote_cell = cells[1] if len(cells) >= 2 else None

        # Lub: Lp. / Radny / Głos (gdzie Głos to trzecia kolumna)
        if len(cells) >= 3:
            name_cell = cells[1]
            vote_cell = cells[2]

        if not name_cell or not vote_cell:
            continue

        name = name_cell.get_text(strip=True)
        vote = vote_cell.get_text(strip=True).upper()

        # Pomiń wiersze nagłówkowe
        if not name or name.isdigit() or "radny" in name.lower():
            continue

        if "ZA" in vote:
            votes_detail["named_votes"]["za"].append(name)
            votes_detail["counts"]["za"] += 1
        elif "PRZECIW" in vote:
            votes_detail["named_votes"]["przeciw"].append(name)
            votes_detail["counts"]["przeciw"] += 1
        elif "WSTRZYMAŁ" in vote or "WSTRZYMUJĘ" in vote:
            votes_detail["named_votes"]["wstrzymal_sie"].append(name)
            votes_detail["counts"]["wstrzymal_sie"] += 1
        elif "NIEOBECN" in vote:
            votes_detail["named_votes"]["nieobecni"].append(name)
            votes_detail["counts"]["nieobecni"] += 1

    return votes_detail


# ============================================================================
# Step 3: Build output structures
# ============================================================================

def load_profiles(profiles_path: str) -> dict:
    """Załaduj profiles.json z mapowaniem radny → klub."""
    path = Path(profiles_path)
    if not path.exists():
        print(f"  UWAGA: Brak {profiles_path} — kluby będą oznaczone jako '?'")
        return {}

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    for p in data.get("profiles", []):
        name = p["name"]
        kadencje = p.get("kadencje", {})
        if kadencje:
            latest = list(kadencje.values())[-1]
            result[name] = {
                "name": name,
                "club": latest.get("club", "?"),
                "district": latest.get("okręg"),
            }
    return result


def compute_club_majority(vote: dict, profiles: dict) -> dict[str, str]:
    """Dla każdego klubu oblicz główną pozycję w głosowaniu."""
    club_votes = defaultdict(lambda: {"za": 0, "przeciw": 0, "wstrzymal_sie": 0})

    for cat in ["za", "przeciw", "wstrzymal_sie"]:
        for name in vote["named_votes"].get(cat, []):
            club = profiles.get(name, {}).get("club", "?")
            if club != "?":
                club_votes[club][cat] += 1

    majority = {}
    for club, counts in club_votes.items():
        best = max(counts, key=counts.get)
        majority[club] = best

    return majority


def build_councilors(all_votes: list[dict], profiles: dict) -> list[dict]:
    """Zbuduj statystyki radnych z danych głosowań."""
    all_names = set()
    for v in all_votes:
        for cat_names in v["named_votes"].values():
            all_names.update(cat_names)

    councilors = {}
    for name in sorted(all_names):
        prof = profiles.get(name, {})
        councilors[name] = {
            "name": name,
            "club": prof.get("club", "?"),
            "district": prof.get("district"),
            "votes_za": 0,
            "votes_przeciw": 0,
            "votes_wstrzymal": 0,
            "votes_brak": 0,
            "votes_nieobecny": 0,
            "votes_with_club": 0,
            "votes_against_club": 0,
            "rebellions": [],
        }

    for v in all_votes:
        club_majority = compute_club_majority(v, profiles)

        for name in v["named_votes"].get("za", []):
            if name in councilors:
                councilors[name]["votes_za"] += 1
                _check_rebellion(councilors[name], "za", club_majority, v)

        for name in v["named_votes"].get("przeciw", []):
            if name in councilors:
                councilors[name]["votes_przeciw"] += 1
                _check_rebellion(councilors[name], "przeciw", club_majority, v)

        for name in v["named_votes"].get("wstrzymal_sie", []):
            if name in councilors:
                councilors[name]["votes_wstrzymal"] += 1
                _check_rebellion(councilors[name], "wstrzymal_sie", club_majority, v)

        for name in v["named_votes"].get("brak_glosu", []):
            if name in councilors:
                councilors[name]["votes_brak"] += 1

        for name in v["named_votes"].get("nieobecni", []):
            if name in councilors:
                councilors[name]["votes_nieobecny"] += 1

    total_votes = len(all_votes)

    result = []
    for c in councilors.values():
        present_votes = c["votes_za"] + c["votes_przeciw"] + c["votes_wstrzymal"] + c["votes_brak"]
        aktywnosc = (present_votes / total_votes * 100) if total_votes > 0 else 0
        total_club_votes = c["votes_with_club"] + c["votes_against_club"]
        zgodnosc = (c["votes_with_club"] / total_club_votes * 100) if total_club_votes > 0 else 0

        result.append({
            "name": c["name"],
            "club": c["club"],
            "district": c["district"],
            "frekwencja": 0.0,  # Na podstawie sesji, nie głosowań
            "aktywnosc": round(aktywnosc, 1),
            "zgodnosc_z_klubem": round(zgodnosc, 1),
            "votes_za": c["votes_za"],
            "votes_przeciw": c["votes_przeciw"],
            "votes_wstrzymal": c["votes_wstrzymal"],
            "votes_brak": c["votes_brak"],
            "votes_nieobecny": c["votes_nieobecny"],
            "votes_total": total_votes,
            "rebellion_count": len(c["rebellions"]),
            "rebellions": c["rebellions"][:20],
            "has_activity_data": False,
            "activity": None,
        })

    return sorted(result, key=lambda x: x["name"])


def _check_rebellion(councilor: dict, vote_cat: str, club_majority: dict, vote: dict):
    """Sprawdź, czy radny głosował inaczej niż większość klubu."""
    club = councilor["club"]
    if club == "?" or club not in club_majority:
        return

    majority_cat = club_majority[club]
    if vote_cat == majority_cat:
        councilor["votes_with_club"] += 1
    else:
        councilor["votes_against_club"] += 1
        councilor["rebellions"].append({
            "vote_id": vote.get("id", ""),
            "session": vote.get("session_date", ""),
            "topic": vote.get("topic", "")[:120],
            "their_vote": vote_cat,
            "club_majority": majority_cat,
        })


def compute_similarity(all_votes: list[dict], councilors_list: list[dict]) -> tuple[list, list]:
    """Oblicz pary radnych o najwyższym/najniższym podobieństwie głosowania."""
    name_to_club = {c["name"]: c["club"] for c in councilors_list}
    vectors = defaultdict(dict)

    for v in all_votes:
        for cat in ["za", "przeciw", "wstrzymal_sie"]:
            for name in v["named_votes"].get(cat, []):
                vectors[name][v["id"]] = cat

    names = sorted(vectors.keys())
    pairs = []

    for a, b in combinations(names, 2):
        common = set(vectors[a].keys()) & set(vectors[b].keys())
        if len(common) < 10:
            continue

        same = sum(1 for vid in common if vectors[a][vid] == vectors[b][vid])
        score = round(same / len(common) * 100, 1)

        pairs.append({
            "a": a,
            "b": b,
            "club_a": name_to_club.get(a, "?"),
            "club_b": name_to_club.get(b, "?"),
            "score": score,
            "common_votes": len(common),
        })

    pairs.sort(key=lambda x: x["score"], reverse=True)
    top = pairs[:20]
    bottom = pairs[-20:][::-1]

    return top, bottom


def build_sessions(all_votes: list[dict]) -> list[dict]:
    """Zbuduj dane sesji z liczby głosowań."""
    votes_by_date = defaultdict(list)
    for v in all_votes:
        date = v.get("session_date", "unknown")
        votes_by_date[date].append(v)

    result = []
    for date in sorted(votes_by_date.keys()):
        session_votes = votes_by_date[date]
        attendees = set()

        for v in session_votes:
            for cat in ["za", "przeciw", "wstrzymal_sie", "brak_glosu"]:
                attendees.update(v["named_votes"].get(cat, []))

        result.append({
            "date": date,
            "number": "",  # Brak numeru sesji w eSesja
            "vote_count": len(session_votes),
            "attendee_count": len(attendees),
            "attendees": sorted(attendees),
            "speakers": [],
        })

    return result


def make_slug(name: str) -> str:
    """Create URL-safe slug from Polish name."""
    replacements = {
        'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n',
        'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
        'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N',
        'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z',
    }
    slug = name.lower()
    for pl, ascii_c in replacements.items():
        slug = slug.replace(pl, ascii_c)
    slug = slug.replace(' ', '-').replace("'", "")
    return slug


def build_profiles_json(output: dict, profiles_path: str):
    """Build profiles.json from data.json councilors (kadencje format with slugs)."""
    profiles = []
    for kad in output["kadencje"]:
        kid = kad["id"]
        for c in kad["councilors"]:
            entry = {
                "club": c.get("club", "?"),
                "frekwencja": c.get("frekwencja", 0),
                "aktywnosc": c.get("aktywnosc", 0),
                "zgodnosc_z_klubem": c.get("zgodnosc_z_klubem", 0),
                "votes_za": c.get("votes_za", 0),
                "votes_przeciw": c.get("votes_przeciw", 0),
                "votes_wstrzymal": c.get("votes_wstrzymal", 0),
                "votes_brak": c.get("votes_brak", 0),
                "votes_nieobecny": c.get("votes_nieobecny", 0),
                "votes_total": c.get("votes_total", 0),
                "rebellion_count": c.get("rebellion_count", 0),
                "rebellions": c.get("rebellions", []),
                "has_voting_data": True,
                "has_activity_data": c.get("has_activity_data", False),
                "roles": [],
                "notes": "",
                "former": False,
                "mid_term": False,
            }
            if c.get("activity"):
                entry["activity"] = c["activity"]
            profiles.append({
                "name": c["name"],
                "slug": make_slug(c["name"]),
                "kadencje": {kid: entry},
            })

    path = Path(profiles_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"profiles": profiles}, f, ensure_ascii=False, indent=2)
    print(f"  Zapisano profiles.json: {len(profiles)} profili")


# ============================================================================
# Main
# ============================================================================

def scrape(output_path, profiles_path, debug=False):
    """Główna funkcja scrapowania."""
    session = requests.Session()

    print("\n=== Radoskop Katowice — Scraper głosowań (eSesja) ===")

    # Pobierz listę głosowań
    print("\n[1/3] Pobieranie listy głosowań...")
    try:
        html = fetch_votes_list(session, debug=debug)
        votes_meta = parse_votes_list(html, debug=debug)
    except Exception as e:
        print(f"BŁĄD: {e}")
        sys.exit(1)

    if not votes_meta:
        print("BŁĄD: Nie znaleziono głosowań")
        sys.exit(1)

    print(f"  Znaleziono: {len(votes_meta)} głosowań")

    # Pobierz szczegóły każdego głosowania
    print(f"\n[2/3] Pobieranie szczegółów ({len(votes_meta)} głosowań)...")
    all_votes = []

    for i, meta in enumerate(votes_meta):
        html = fetch_vote_detail(session, meta["url"], debug=debug)
        if html:
            vote_detail = parse_vote_detail(html, meta["url"], debug=debug)

            # Ekstraktuj datę z URL (jeśli dostępna)
            date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", meta["url"])
            session_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}" if date_match else "unknown"

            vote_id = f"{session_date}_{i:03d}_000"

            all_votes.append({
                "id": vote_id,
                "source_url": meta["url"],
                "session_date": session_date,
                "session_number": "",
                "topic": vote_detail.get("topic", ""),
                "druk": None,
                "resolution": None,
                "counts": vote_detail.get("counts", {}),
                "named_votes": vote_detail.get("named_votes", {}),
            })

        if (i + 1) % 50 == 0:
            print(f"  Pobrano: {i+1}/{len(votes_meta)}...")

        time.sleep(DELAY)

    print(f"  Pobrano: {len(all_votes)} głosowań z danymi imiennymi")

    if not all_votes:
        print("BŁĄD: Brak głosowań z danymi")
        sys.exit(1)

    # Załaduj profile
    print(f"\n[3/3] Budowanie danych wyjściowych...")
    profiles = load_profiles(profiles_path)

    if not profiles:
        profiles = {name: {"name": name, "club": club, "district": None}
                   for name, club in COUNCILORS.items()}
        if profiles:
            print(f"  Załadowano profile: {len(profiles)} radnych (z listy hardcoded)")
        else:
            print(f"  UWAGA: Brak profili — kluby będą oznaczone jako '?'")
    else:
        print(f"  Załadowano profile: {len(profiles)} radnych")

    # Zbuduj struktury
    kid = "2024-2029"
    councilors = build_councilors(all_votes, profiles)
    sessions_data = build_sessions(all_votes)
    sim_top, sim_bottom = compute_similarity(all_votes, councilors)

    club_counts = defaultdict(int)
    for c in councilors:
        club_counts[c["club"]] += 1

    print(f"\n  {len(sessions_data)} sesji, {len(all_votes)} głosowań, {len(councilors)} radnych")
    print(f"  Kluby: {dict(club_counts)}")

    # Zbuduj output
    kad_output = {
        "id": kid,
        "label": KADENCJE[kid]["label"],
        "clubs": {club: count for club, count in sorted(club_counts.items())},
        "sessions": sessions_data,
        "total_sessions": len(sessions_data),
        "total_votes": len(all_votes),
        "total_councilors": len(councilors),
        "councilors": councilors,
        "votes": all_votes,
        "similarity_top": sim_top,
        "similarity_bottom": sim_bottom,
    }

    output = {
        "generated": datetime.now().isoformat(),
        "default_kadencja": kid,
        "kadencje": [kad_output],
    }

    # Zapisz
    out_path = Path(output_path)
    save_split_output(output, out_path)

    print(f"\nGotowe! Zapisano do {out_path}")

    # Scal z profiles.json
    build_profiles_json(output, profiles_path)


def main():
    parser = argparse.ArgumentParser(
        description="Scraper Rady Miasta Katowice (eSesja)"
    )
    parser.add_argument(
        "--output", default="docs/data.json",
        help="Plik wyjściowy (domyślnie: docs/data.json)"
    )
    parser.add_argument(
        "--profiles", default="docs/profiles.json",
        help="Plik profiles.json (domyślnie: docs/profiles.json)"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Włącz szczegółowe logowanie"
    )
    args = parser.parse_args()

    scrape(
        output_path=args.output,
        profiles_path=args.profiles,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
