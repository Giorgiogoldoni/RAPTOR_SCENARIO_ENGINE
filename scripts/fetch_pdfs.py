#!/usr/bin/env python3
"""
RSE PDF Fetcher
Scarica automaticamente i report istituzionali pubblici
FMI, BCE, OCSE, Fed — trimestrale/semestrale
"""
import os, json, datetime, requests, hashlib
from pathlib import Path

PDF_SOURCES = [
    {
        "id": "fmi_weo",
        "name": "FMI World Economic Outlook",
        "org": "FMI",
        "url": "https://www.imf.org/en/Publications/WEO/Issues/2025/04/01/world-economic-outlook-april-2025",
        "pdf_pattern": "https://www.imf.org/-/media/Files/Publications/WEO/2025/April/English/text.ashx",
        "frequency": "semestrale",
        "months": [4, 10],
        "priority": "alta",
    },
    {
        "id": "bce_bulletin",
        "name": "BCE Economic Bulletin",
        "org": "BCE",
        "url": "https://www.ecb.europa.eu/pub/economic-bulletin/html/index.en.html",
        "pdf_pattern": "https://www.ecb.europa.eu/pub/pdf/ecbu/eb{year}{issue}.en.pdf",
        "frequency": "bimestrale",
        "months": [1, 3, 5, 7, 9, 11],
        "priority": "alta",
    },
    {
        "id": "ocse_outlook",
        "name": "OCSE Economic Outlook",
        "org": "OCSE",
        "url": "https://www.oecd.org/economic-outlook/",
        "pdf_pattern": None,
        "frequency": "semestrale",
        "months": [5, 11],
        "priority": "media",
    },
    {
        "id": "fed_sep",
        "name": "Fed Summary of Economic Projections",
        "org": "Fed",
        "url": "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20250319.htm",
        "pdf_pattern": None,
        "frequency": "trimestrale",
        "months": [3, 6, 9, 12],
        "priority": "alta",
    },
    {
        "id": "blackrock_outlook",
        "name": "BlackRock Global Investment Outlook",
        "org": "BlackRock",
        "url": "https://www.blackrock.com/corporate/insights/blackrock-investment-institute/global-outlook",
        "pdf_pattern": None,
        "frequency": "mensile",
        "months": list(range(1,13)),
        "priority": "media",
    },
    {
        "id": "jpm_guide",
        "name": "JPMorgan Guide to the Markets",
        "org": "JPMorgan",
        "url": "https://am.jpmorgan.com/it/it/asset-management/adv/insights/market-insights/guide-to-the-markets/",
        "pdf_pattern": None,
        "frequency": "trimestrale",
        "months": [1, 4, 7, 10],
        "priority": "alta",
    },
    {
        "id": "vanguard_outlook",
        "name": "Vanguard Economic and Market Outlook",
        "org": "Vanguard",
        "url": "https://institutional.vanguard.com/insights/research-commentary/2025/vanguard-economic-market-outlook",
        "pdf_pattern": None,
        "frequency": "annuale",
        "months": [1],
        "priority": "media",
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RSE-Bot/1.0; research purposes)",
    "Accept": "application/pdf,text/html,*/*",
}

def download_pdf(source, output_dir):
    """Tenta di scaricare il PDF dalla fonte"""
    pdf_dir = Path(output_dir) / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{source['id']}_{datetime.datetime.now().strftime('%Y%m')}.pdf"
    filepath = pdf_dir / filename

    # Se già scaricato questo mese, skip
    if filepath.exists():
        print(f"  ✓ {source['name']} — già presente ({filename})")
        return str(filepath), False

    if not source.get("pdf_pattern"):
        print(f"  ⚠ {source['name']} — URL PDF non configurato (download manuale)")
        return None, False

    try:
        url = source["pdf_pattern"]
        print(f"  ↓ {source['name']} da {url[:60]}...")
        resp = requests.get(url, headers=HEADERS, timeout=30, stream=True)

        if resp.status_code == 200 and "pdf" in resp.headers.get("content-type","").lower():
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            size_kb = filepath.stat().st_size // 1024
            print(f"  ✓ Scaricato {filename} ({size_kb}KB)")
            return str(filepath), True
        else:
            print(f"  ✗ HTTP {resp.status_code} — contenuto non PDF")
            return None, False

    except Exception as e:
        print(f"  ✗ Errore: {e}")
        return None, False

def update_download_log(results, output_dir):
    """Aggiorna il log dei download"""
    log_path = Path(output_dir) / "download_log.json"

    log = {}
    try:
        with open(log_path) as f:
            log = json.load(f)
    except:
        pass

    for source_id, result in results.items():
        log[source_id] = {
            "last_attempt": datetime.datetime.utcnow().isoformat() + "Z",
            "last_success": result.get("path") and datetime.datetime.utcnow().isoformat() + "Z",
            "path": result.get("path"),
            "new_download": result.get("new"),
        }

    with open(log_path, "w") as f:
        json.dump(log, f, indent=2)

    return log

def main():
    os.makedirs("data/pdfs", exist_ok=True)
    print(f"📚 RSE PDF Fetcher — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   Fonti configurate: {len(PDF_SOURCES)}")

    current_month = datetime.datetime.now().month
    results = {}
    new_downloads = []

    for source in PDF_SOURCES:
        # Scarica solo se questo è il mese giusto
        if current_month in source.get("months", []):
            path, is_new = download_pdf(source, "data")
            results[source["id"]] = {"path": path, "new": is_new, "source": source}
            if is_new and path:
                new_downloads.append(source["id"])
        else:
            print(f"  — {source['name']} — non questo mese (mesi: {source['months']})")

    log = update_download_log(results, "data")

    # Output summary
    summary = {
        "last_run": datetime.datetime.utcnow().isoformat() + "Z",
        "new_downloads": new_downloads,
        "sources": [
            {
                "id": s["id"],
                "name": s["name"],
                "org": s["org"],
                "priority": s["priority"],
                "last_download": log.get(s["id"], {}).get("last_success"),
                "path": log.get(s["id"], {}).get("path"),
            }
            for s in PDF_SOURCES
        ]
    }

    with open("data/pdf_sources.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n✅ Completato")
    print(f"   Nuovi download: {len(new_downloads)}")
    if new_downloads:
        print(f"   → {', '.join(new_downloads)}")
    print(f"   Log: data/download_log.json")

if __name__ == "__main__":
    main()
