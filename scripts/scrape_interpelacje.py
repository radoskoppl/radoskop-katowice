#!/usr/bin/env python3
"""
Scraper interpelacji i zapytań radnych z BIP Katowice.

Źródło: https://bip.katowice.eu/Rada_Miasta/

BIP Katowice zawiera sekcje z interpelacjami i zapytaniami — strony HTML
z tabelami zawierającymi podstawowe informacje i linkami do szczegółów.

UWAGA: Uruchom lokalnie — sandbox Cowork blokuje domeny.

Użycie:
  python3 scrape_interpelacje.py [--output docs/interpelacje.json]
                                 [--fetch-details]
                                 [--debug]
"""

import argparse
import json
import os
import re
import sys
import time

try:
    import requests
except ImportError:
    print("Wymagany moduł: pip install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Wymagany moduł: pip install beautifulsoup4")
    sys.exit(1)


# ============================================================================
# Config
# ============================================================================

BASE_URL = "https://bip.katowice.eu"
INTERPELACJE_URL = f"{BASE_URL}/Rada_Miasta/"

KADENCJE = {
    "IX": {"label": "IX kadencja (2024–2029)"},
}

HEADERS = {
    "User-Agent": "Radoskop/1.0 (https://katowice.radoskop.pl; kontakt@radoskop.pl)",
    "Accept": "text/html",
}

DELAY = 0.5


# ============================================================================
# Config — kategoryzacja interpelacji
# ============================================================================

CATEGORIES = {
    "transport": ["transport", "komunikacj", "autobus", "tramwaj", "drog", "ulic", "rondo",
                  "chodnik", "przejści", "parkow", "rower", "ścieżk"],
    "infrastruktura": ["infrastru", "remont", "naprawa", "budow", "inwesty", "moderniz",
                       "oświetl", "kanalizacj", "wodociąg", "nawierzch", "most"],
    "bezpieczeństwo": ["bezpiecz", "straż", "policj", "monitoring", "kradzież", "wandal",
                       "przestęp", "patrol"],
    "edukacja": ["szkoł", "edukacj", "przedszkol", "żłob", "nauczyc", "kształc",
                 "oświat", "uczni"],
    "zdrowie": ["zdrow", "szpital", "leczni", "medyc", "lekarz", "przychodni",
                "ambulat"],
    "środowisko": ["środowisk", "zieleń", "drzew", "park ", "recykl", "odpady",
                   "śmieci", "klimat", "ekolog", "powietrz", "smog", "hałas"],
    "mieszkalnictwo": ["mieszka", "lokal", "zasob", "czynsz", "wspólnot", "kamieni",
                       "dewelop", "budynek"],
    "kultura": ["kultur", "bibliotek", "muzeum", "teatr", "koncert", "festiwal",
                "zabytek", "zabytk"],
    "sport": ["sport", "boisko", "stadion", "basen", "siłowni", "hala sport",
              "rekrea"],
    "pomoc społeczna": ["społeczn", "pomoc", "bezdomn", "senior", "niepełnospr",
                        "opiek", "zasiłk"],
    "budżet": ["budżet", "finansow", "wydatk", "dotacj", "środki", "pieniąd",
               "podatk"],
    "administracja": ["administrac", "urzęd", "pracowni", "regulam", "organizac",
                      "procedur", "biurokrac"],
}


# ============================================================================
# Scraping — list page
# ============================================================================

def fetch_interpelacje_page(session, url, debug=False):
    """Pobiera stronę z interpelacjami/zapytaniami."""
    if debug:
        print(f"  [DEBUG] GET {url}")

    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        if debug:
            print(f"  [DEBUG] Błąd: {e}")
        return None


def parse_interpelacje_page(html, kadencja_name, debug=False):
    """Parsuje stronę listy interpelacji/zapytań.

    BIP Katowice zawiera tabele z danymi:
    - Przedmiot interpelacji (link)
    - Radny
    - Data
    - Status
    """
    soup = BeautifulSoup(html, "html.parser")

    records = []
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")[1:]  # Skip header

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            record = {}

            # Pierwsza kolumna — przedmiot z linkiem
            first_cell = cells[0]
            a = first_cell.find("a")
            if a:
                record["przedmiot"] = a.get_text(strip=True)
                href = a.get("href", "")
                if href.startswith("/"):
                    record["bip_url"] = BASE_URL + href
                elif href.startswith("http"):
                    record["bip_url"] = href

            # Reszta kolumn — radny, data, status
            if len(cells) >= 2:
                record.setdefault("radny", cells[1].get_text(strip=True))
            if len(cells) >= 3:
                record.setdefault("data_wplywu", parse_date(cells[2].get_text(strip=True)))
            if len(cells) >= 4:
                record.setdefault("status", cells[3].get_text(strip=True))

            # Określ typ
            subject_lower = record.get("przedmiot", "").lower()
            if "zapytanie" in subject_lower:
                record["typ"] = "zapytanie"
            elif "wniosek" in subject_lower:
                record["typ"] = "wniosek"
            else:
                record["typ"] = "interpelacja"

            if record.get("przedmiot"):
                record.setdefault("status", "")
                record.setdefault("bip_url", "")
                record.setdefault("data_wplywu", "")
                record.setdefault("radny", "")
                record["kadencja"] = kadencja_name
                records.append(record)

    if debug:
        print(f"  [DEBUG] Parsed {len(records)} records")

    return records


# ============================================================================
# Scraping — detail page
# ============================================================================

def fetch_detail(session, bip_url, debug=False):
    """Pobiera szczegóły interpelacji z jej strony."""
    if not bip_url:
        return {}

    if debug:
        print(f"  [DEBUG] GET {bip_url}")

    try:
        resp = session.get(bip_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        detail = {}

        # Szukaj informacji w tabelach
        for row in soup.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue

            label = th.get_text(strip=True).lower()
            val = td.get_text(strip=True)

            if "typ" in label:
                detail["typ_full"] = val
            elif "nr" in label or "numer" in label:
                detail["nr_sprawy"] = val
            elif "data" in label and ("wpływu" in label or "złożenia" in label):
                detail["data_wplywu"] = parse_date(val)
            elif "odpowiedź" in label or "odpowiedzi" in label:
                detail["data_odpowiedzi"] = parse_date(val)

        # Szukaj linków do załączników
        attachments = []
        for a in soup.find_all("a"):
            href = a.get("href", "")
            text = a.get_text(strip=True)

            if "attachment" in href or "zalacznik" in href or "download" in href:
                full_url = BASE_URL + href if href.startswith("/") else href
                attachments.append({"nazwa": text, "url": full_url})

                text_lower = text.lower()
                if "odpowiedź" in text_lower or "odpowiedzi" in text_lower:
                    detail["odpowiedz_url"] = full_url
                elif not detail.get("tresc_url"):
                    detail["tresc_url"] = full_url

        if attachments:
            detail["zalaczniki"] = attachments

        return detail

    except Exception as e:
        if debug:
            print(f"  [DEBUG] Error fetching detail {bip_url}: {e}")
        return {}


def parse_date(raw):
    """Konwertuje datę na format YYYY-MM-DD."""
    if not raw:
        return ""

    raw = raw.strip()

    # DD.MM.YYYY lub DD.MM.YYYY HH:MM
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # YYYY-MM-DD
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return raw[:10]

    return raw


# ============================================================================
# Category classification
# ============================================================================

def classify_category(przedmiot):
    """Klasyfikuje kategorię interpelacji na podstawie przedmiotu."""
    if not przedmiot:
        return "inne"

    text = przedmiot.lower()

    for cat, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw in text:
                return cat

    return "inne"


# ============================================================================
# Main
# ============================================================================

def scrape(output_path, fetch_details=True, debug=False):
    """Główna funkcja scrapowania."""
    session = requests.Session()
    all_records = []

    print("\n=== Radoskop Katowice — Scraper interpelacji ===")

    kad_name = "IX"
    print(f"\n=== {KADENCJE[kad_name]['label']} ===")

    print(f"\n[1/2] Pobieranie listy interpelacji...")

    html = fetch_interpelacje_page(session, INTERPELACJE_URL, debug=debug)
    if not html:
        print("  BŁĄD: Nie można pobrać listy")
        sys.exit(1)

    records = parse_interpelacje_page(html, kad_name, debug=debug)
    print(f"  Pobrano: {len(records)} rekordów")

    if fetch_details and records:
        print(f"\n[2/2] Pobieram szczegóły ({len(records)} rekordów)...")

        for i, rec in enumerate(records):
            bip_url = rec.get("bip_url", "")
            if bip_url:
                detail = fetch_detail(session, bip_url, debug=debug)
                if detail:
                    rec.update({k: v for k, v in detail.items() if v})

            if (i + 1) % 50 == 0:
                print(f"  Szczegóły: {i+1}/{len(records)}")

            time.sleep(DELAY)

    all_records.extend(records)

    # Klasyfikuj kategorie i normalizuj pola
    for rec in all_records:
        rec["kategoria"] = classify_category(rec.get("przedmiot", ""))

        # Normalizuj status
        status = rec.get("status", "").lower()
        if "udzielono" in status:
            rec["odpowiedz_status"] = "udzielono odpowiedzi"
        elif "oczekuje" in status:
            rec["odpowiedz_status"] = "oczekuje na odpowiedź"
        else:
            rec["odpowiedz_status"] = status

        # Ensure consistent output fields
        rec.setdefault("data_wplywu", "")
        rec.setdefault("data_odpowiedzi", "")
        rec.setdefault("tresc_url", "")
        rec.setdefault("odpowiedz_url", "")
        rec.setdefault("nr_sprawy", "")

    # Sortuj od najnowszych
    all_records.sort(
        key=lambda x: x.get("data_wplywu", "") or x.get("bip_url", ""),
        reverse=True,
    )

    # Stats
    interp = sum(1 for r in all_records if r.get("typ") == "interpelacja")
    zap = sum(1 for r in all_records if r.get("typ") == "zapytanie")
    answered = sum(1 for r in all_records if "udzielono" in r.get("odpowiedz_status", ""))

    print(f"\n=== Podsumowanie ===")
    print(f"Interpelacje: {interp}")
    print(f"Zapytania:    {zap}")
    print(f"Z odpowiedzią: {answered}")
    print(f"Razem:        {len(all_records)}")

    # Zapisz
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\nZapisano: {output_path} ({size_kb:.1f} KB)")
    print(f"Gotowe: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Scraper interpelacji i zapytań radnych z BIP Katowice"
    )
    parser.add_argument(
        "--output", default="docs/interpelacje.json",
        help="Ścieżka do pliku wyjściowego (domyślnie: docs/interpelacje.json)"
    )
    parser.add_argument(
        "--skip-details", action="store_true",
        help="Pomiń pobieranie szczegółów (szybciej, ale brak dat i załączników)"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Włącz szczegółowe logowanie"
    )
    args = parser.parse_args()

    scrape(
        output_path=args.output,
        fetch_details=not args.skip_details,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
