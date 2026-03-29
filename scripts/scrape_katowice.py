#!/usr/bin/env python3
"""
Scraper danych głosowań Rady Miasta Katowice.

Źródło: katowice.esesja.pl (platforma eSesja)
eSesja ma dostęp do głosowań bez JavaScript.

Struktura eSesja:
  1. Archiwum glosowan: https://katowice.esesja.pl/glosowania/
     Linki do sesji w formacie /listaglosowan/{UUID}
  2. Lista glosowan w sesji: /listaglosowan/{UUID}
     Linki do pojedynczych glosowan /glosowanie/{ID}/{HASH}
  3. Wyniki glosowania: /glosowanie/{ID}/{HASH}
     div.wim > h3 (kategoria: ZA/PRZECIW/...) > div.osobaa (nazwisko)

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
except ImportError:
    print("Zainstaluj: pip install requests")
    sys.exit(1)

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


# Build flexible name lookup: eSesja uses "Lastname Firstname"
# while COUNCILORS uses "Firstname Lastname".
def _build_name_lookup(councilors: dict[str, str]) -> dict[str, str]:
    lookup = {}
    for name, club in councilors.items():
        lookup[name] = club
        parts = name.split()
        if len(parts) >= 2:
            lookup[f"{parts[-1]} {' '.join(parts[:-1])}"] = club
            lookup[f"{parts[-1]} {parts[0]}"] = club
    return lookup

_CLUB_LOOKUP = _build_name_lookup(COUNCILORS)


def resolve_club(name: str) -> str:
    """Resolve a councillor name (any format) to their club."""
    if name in _CLUB_LOOKUP:
        return _CLUB_LOOKUP[name]
    parts = name.split()
    if parts:
        last = parts[0]
        for key, club in _CLUB_LOOKUP.items():
            if key.split()[0] == last or key.split()[-1] == last:
                return club
    return "?"


# Polish month name mapping
MONTHS_PL = {
    "stycznia": 1, "lutego": 2, "marca": 3, "kwietnia": 4,
    "maja": 5, "czerwca": 6, "lipca": 7, "sierpnia": 8,
    "września": 9, "października": 10, "listopada": 11, "grudnia": 12,
}


# ============================================================================
# Step 1: Fetch session list from eSesja /glosowania/ archive
# ============================================================================

def fetch_soup(http_session, url):
    """GET a page and return BeautifulSoup."""
    resp = http_session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    # eSesja pages declare windows-1250 in meta charset but the HTTP header
    # omits charset, so requests falls back to ISO-8859-1 which mangles
    # Polish characters (ł→³, ą→¹, ę→ê, etc.)
    if "esesja" in url:
        resp.encoding = "windows-1250"
    return BeautifulSoup(resp.text, "html.parser")


def fetch_session_list(http_session, debug=False):
    """Fetch all session entries from the paginated /glosowania/ archive.

    eSesja lists sessions as <a href="/listaglosowan/{UUID}"> with text like
    "sesja Rady Miasta Katowice w dniu 20 marca 2026, godz. 11:00".
    Pages: /glosowania/, /glosowania/2, /glosowania/3, ...
    """
    sessions = []
    page = 1

    while True:
        url = ESESJA_VOTES_LIST if page == 1 else f"{ESESJA_VOTES_LIST}{page}"
        if debug:
            print(f"[DEBUG] GET {url}")

        try:
            soup = fetch_soup(http_session, url)
        except Exception as e:
            print(f"  Blad pobierania {url}: {e}")
            break

        found_on_page = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/listaglosowan/" not in href:
                continue

            text = a.get_text(strip=True)

            # Extract date from "w dniu 20 marca 2026"
            m = re.search(r'w\s+dniu\s+(\d{1,2})\s+(\w+)\s+(\d{4})', text)
            if not m:
                continue

            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = int(m.group(3))
            month = MONTHS_PL.get(month_name)
            if not month:
                continue

            date_str = f"{year}-{month:02d}-{day:02d}"
            full_url = href if href.startswith("http") else ESESJA_BASE + href

            sessions.append({
                "date": date_str,
                "url": full_url,
                "title": text,
            })
            found_on_page += 1

        if found_on_page == 0:
            break

        # Check for next page
        next_link = soup.find("a", href=re.compile(rf'/glosowania/{page + 1}\b'))
        if not next_link:
            break
        page += 1

    # Deduplicate by URL
    seen = set()
    unique = []
    for s in sessions:
        if s["url"] not in seen:
            seen.add(s["url"])
            unique.append(s)

    # Filter by kadencja start date
    kadencja_start = KADENCJE["2024-2029"]["start"]
    filtered = [s for s in unique if s["date"] >= kadencja_start]

    if debug:
        print(f"[DEBUG] {len(unique)} sesji ogolnie, {len(filtered)} w kadencji 2024-2029")

    return sorted(filtered, key=lambda x: x["date"])


# ============================================================================
# Step 2: Fetch votes from a session's /listaglosowan/ page, then each vote
# ============================================================================

def fetch_session_votes(http_session, session_info, debug=False):
    """Fetch /listaglosowan/UUID page, collect /glosowanie/ID/HASH links,
    then fetch each vote detail page.
    """
    try:
        soup = fetch_soup(http_session, session_info["url"])
    except Exception as e:
        print(f"    Blad pobierania sesji: {e}")
        return []

    # Collect unique /glosowanie/ID/HASH links
    seen_urls = set()
    vote_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/glosowanie/" not in href or "/listaglosowan/" in href:
            continue
        url = href if href.startswith("http") else ESESJA_BASE + href
        if url in seen_urls:
            continue
        seen_urls.add(url)
        vote_links.append(url)

    if debug:
        print(f"    [DEBUG] {len(vote_links)} linkow do glosowan")

    votes = []
    for idx, url in enumerate(vote_links):
        vote = fetch_single_vote(http_session, url, session_info, idx, debug)
        if vote:
            votes.append(vote)
        time.sleep(DELAY * 0.5)

    return votes


def fetch_single_vote(http_session, url, session_info, vote_idx, debug=False):
    """Fetch a single /glosowanie/ID/HASH page and parse named results.

    eSesja HTML structure:
      <div class='wim'><h3>ZA<span class='za'> (30)</span></h3>
        <div class='osobaa'>Surname FirstName</div>
      </div>
    """
    try:
        soup = fetch_soup(http_session, url)
    except Exception as e:
        if debug:
            print(f"    [DEBUG] Blad pobierania {url}: {e}")
        return None

    # Extract topic from h1
    topic = ""
    h1 = soup.find("h1")
    if h1:
        topic = h1.get_text(strip=True)[:500]
    # Clean eSesja prefixes
    topic = re.sub(r'^Wyniki g\u0142osowania jawnego w sprawie:\s*', '', topic).strip()
    topic = re.sub(r'^Wyniki g\u0142osowania w sprawie:?\s*', '', topic).strip()
    topic = re.sub(r'^G\u0142osowanie\s+w\s+sprawie\s+', '', topic).strip()
    if not topic:
        topic = f"Glosowanie {vote_idx + 1}"

    named_votes = {
        "za": [],
        "przeciw": [],
        "wstrzymal_sie": [],
        "brak_glosu": [],
        "nieobecni": [],
    }

    counts = {
        "za": 0,
        "przeciw": 0,
        "wstrzymal_sie": 0,
        "brak_glosu": 0,
        "nieobecni": 0,
    }

    # Parse named votes from div.wim sections.
    # Each div.wim has an h3 header (ZA/PRZECIW/...) and div.osobaa children.
    category_map = {
        "za": "za",
        "przeciw": "przeciw",
        "wstrzymuj": "wstrzymal_sie",
        "brak g": "brak_glosu",
        "nieobecn": "nieobecni",
    }

    for wim in soup.find_all("div", class_="wim"):
        h3 = wim.find("h3")
        if not h3:
            continue
        h3_text = h3.get_text(strip=True).upper()
        cat_key = None
        for prefix, key in category_map.items():
            if h3_text.upper().startswith(prefix.upper()):
                cat_key = key
                break
        if not cat_key:
            continue
        for osoba in wim.find_all("div", class_="osobaa"):
            name = osoba.get_text(strip=True)
            if name and len(name) > 2:
                named_votes[cat_key].append(name)

    total_named = sum(len(v) for v in named_votes.values())
    if total_named == 0:
        return None

    for cat in named_votes:
        counts[cat] = len(named_votes[cat])

    return {
        "topic": topic[:500],
        "counts": counts,
        "named_votes": named_votes,
    }


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

def scrape(output_path, profiles_path, debug=False, max_sessions=0):
    """Glowna funkcja scrapowania."""
    http_session = requests.Session()

    print("\n=== Radoskop Katowice  Scraper glosowan (eSesja) ===")

    # Step 1: fetch session list from paginated /glosowania/ archive
    print("\n[1/3] Pobieranie listy sesji...")
    session_list = fetch_session_list(http_session, debug=debug)

    if not session_list:
        print("BLAD: Nie znaleziono sesji")
        sys.exit(1)

    print(f"  Znaleziono: {len(session_list)} sesji")

    if max_sessions > 0:
        session_list = session_list[:max_sessions]

    # Step 2: for each session, fetch its vote list and then each vote detail
    print(f"\n[2/3] Pobieranie glosowan z {len(session_list)} sesji...")
    all_votes = []

    for i, session_info in enumerate(session_list):
        print(f"  [{i+1}/{len(session_list)}] {session_info['date']}  {session_info.get('title', '')[:60]}")
        session_votes = fetch_session_votes(http_session, session_info, debug=debug)

        for idx, vote_detail in enumerate(session_votes):
            vote_id = f"{session_info['date']}_{idx:03d}_000"
            all_votes.append({
                "id": vote_id,
                "source_url": vote_detail.get("source_url", session_info["url"]),
                "session_date": session_info["date"],
                "session_number": "",
                "topic": vote_detail.get("topic", ""),
                "druk": None,
                "resolution": None,
                "counts": vote_detail.get("counts", {}),
                "named_votes": vote_detail.get("named_votes", {}),
            })

        print(f"    {len(session_votes)} glosowan")
        time.sleep(DELAY)

    print(f"  Pobrano: {len(all_votes)} glosowan z danymi imiennymi")

    if not all_votes:
        print("BLAD: Brak glosowan z danymi")
        sys.exit(1)

    print(f"\n[3/3] Budowanie danych wyjsciowych...")
    profiles = load_profiles(profiles_path)

    if not profiles:
        # Build profiles keyed by both "Firstname Lastname" and "Lastname Firstname"
        # so lookups work regardless of name format from eSesja.
        profiles = {}
        for name, club in COUNCILORS.items():
            entry = {"name": name, "club": club, "district": None}
            profiles[name] = entry
            parts = name.split()
            if len(parts) >= 2:
                profiles[f"{parts[-1]} {' '.join(parts[:-1])}"] = entry
                profiles[f"{parts[-1]} {parts[0]}"] = entry
        if profiles:
            print(f"  Zaladowano profile: {len(COUNCILORS)} radnych (z listy hardcoded)")
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
        help="Wlacz szczegolowe logowanie"
    )
    parser.add_argument(
        "--max-sessions", type=int, default=0,
        help="Maks. sesji (0=wszystkie)"
    )
    args = parser.parse_args()

    scrape(
        output_path=args.output,
        profiles_path=args.profiles,
        debug=args.debug,
        max_sessions=args.max_sessions,
    )


if __name__ == "__main__":
    main()
