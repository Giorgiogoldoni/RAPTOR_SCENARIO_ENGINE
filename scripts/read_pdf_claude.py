#!/usr/bin/env python3
"""
RSE PDF Reader — Claude API
Legge i PDF istituzionali scaricati e ne estrae probabilità scenari
Usa Claude API per interpretare il testo in JSON strutturato
"""
import os, json, datetime, base64, sys
import anthropic
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("⚠ pdfplumber non installato — pip install pdfplumber")
    sys.exit(1)

# Scenari RSE con descrizione per Claude
SCENARIO_KEYS = {
    "petrolio_alto": "Petrolio persistentemente alto (>$90/barile per 6+ mesi)",
    "guerra_commerciale": "Guerra commerciale USA-Cina con dazi generalizzati >25%",
    "crisi_taiwan": "Crisi Taiwan — escalation militare o blocco commerciale",
    "hormuz": "Chiusura Stretto di Hormuz — shock offerta energetica",
    "recessione_germania": "Recessione tecnica in Germania (-0.5%+ PIL per 2 trimestri)",
    "crisi_btp": "Crisi BTP Italia — spread BTP-Bund oltre 300bps",
    "bce_alza": "BCE alza i tassi di nuovo — fine ciclo taglio",
    "deflazione_eu": "Deflazione Eurozona — BCE torna a QE",
    "hard_landing_usa": "Hard landing USA — recessione entro 12 mesi",
    "soft_landing": "Soft landing USA confermato — atterraggio perfetto",
    "stagflazione_usa": "Stagflazione USA — inflazione alta + crescita zero",
    "boom_ai": "Boom AI continua — produttività esplode, mercati alle stelle",
    "flash_crash": "Flash crash — caduta -20%+ in 2 settimane poi rimbalzo",
    "bear_market": "Bear market prolungato — -40% in 18 mesi",
    "bull_estremo": "Bull market estremo — S&P500 +50% in 2 anni",
    "inversione_curva": "Inversione curva tassi — segnale recessione",
    "bitcoin_mainstream": "Bitcoin mainstream — istituzionali entrano massicci",
    "regolamentazione_crypto": "Regolamentazione crypto UE — MiCA impatto mercati",
    "carbon_tax": "Carbon tax europea — energia rinnovabile esplode",
    "crisi_idrica": "Crisi idrica globale — commodity agricole alle stelle",
    "rinascita_industriale_eu": "Rinascita industriale Europa — reshoring + sussidi verdi",
    "boom_demografico_africa": "Boom demografico Africa — nuovo motore crescita globale",
    "rivoluzione_farmaceutica": "Rivoluzione farmaceutica — GLP-1, longevità, biotech",
    "fusione_nucleare": "Fusione nucleare commerciale — energia quasi gratuita entro 10 anni",
    "pace_ucraina": "Pace in Ucraina — ricostruzione, gas russo torna",
    "superciclo_commodity": "Superciclo commodity — domanda EM + underinvestment offerta",
    "dollaro_debole": "Dollaro debole strutturale — EM e oro beneficiano",
    "credito_privato_boom": "Credito privato boom — alternative agli ETF obbligazionari",
    "small_cap_outperform": "Small cap outperformance — value rotation dopo anni growth",
    "dividendi_moda": "Dividendi tornano di moda — income investing strutturale",
    "invecchiamento_europa": "Invecchiamento Europa — healthcare, pharma, previdenza crescono",
    "gen_z_investe": "Gen Z investe diversamente — crypto, ESG, alternative mainstream",
    "pensioni_pressione": "Pensioni sotto pressione — previdenza integrativa esplode",
    "immigrazione_europa": "Immigrazione massiccia Europa — consumi, real estate, banche",
    "robotica_automazione": "Robotica e automazione — produttività manifatturiera",
    "quantum_computing": "Quantum computing — rivoluzione cybersecurity e farmaci",
    "spazio_commerciale": "Spazio commerciale — nuova frontiera investimenti",
    "fintech_decentralizzato": "Fintech decentralizzato — banche tradizionali sotto pressione",
    "governo_tecnico_italia": "Governo tecnico in Italia — riforme strutturali, spread giù",
    "federalismo_eu": "Federalismo fiscale EU — Eurobond permanenti",
    "trump_completo": "Trump 2.0 completo — dazi, dollaro, debito USA",
    "multipolarismo": "Multipolarismo consolidato — fine egemonia dollaro",
    "lusso_italiano": "Rinascita lusso italiano — domanda Asia in ripresa",
    "turismo_record": "Turismo record Europa — boom strutturale",
    "crisi_banche_usa": "Crisi banche regionali USA — contagio o opportunità",
    "pe_democratizzato": "Private equity democratizzato — retail accede agli alternativi",
    "re_commerciale_crisi": "Real estate commerciale crolla — remote work strutturale",
    "infrastrutture_boom": "Infrastrutture boom — PNRR, difesa, transizione energetica",
    "cbdc_europeo": "CBDC europeo — euro digitale, impatto sui bond",
    "dedollarizzazione": "Dedollarizzazione accelera — BRICS, yuan, oro come riserva",
    "inflazione_strutturale": "Inflazione strutturalmente alta — nuovo regime 3-4% permanente",
    "repressione_finanziaria": "Repressione finanziaria — tassi reali negativi di nuovo",
}

EXTRACTION_PROMPT = """Sei un analista macroeconomico esperto. Ti viene fornito un estratto da un report istituzionale ({source_name}).

Il tuo compito è leggere il testo e per ciascuno dei seguenti scenari economici fornire:
1. Una probabilità in percentuale (0-100) se il report ne parla esplicitamente o implicitamente
2. La view dell'istituzione (positiva/negativa/neutra/non_menzionato)
3. Una citazione testuale breve (max 20 parole) se disponibile

SCENARI DA ANALIZZARE:
{scenario_list}

REGOLE IMPORTANTI:
- Se il report non menziona uno scenario, metti probabilità null e view "non_menzionato"
- Probabilità esplicita nel testo → usala direttamente
- View implicita → interpreta il tono (es. "rischi al ribasso significativi" = view negativa, probabilità ~40-60%)
- Sii conservativo — non inventare informazioni non presenti nel testo
- Rispondi SOLO con JSON valido, nessun altro testo

FORMATO JSON RISPOSTA:
{{
  "source": "{source_id}",
  "date": "{date}",
  "scenarios": {{
    "nome_scenario": {{
      "probability": 45,
      "view": "negativa",
      "quote": "citazione testuale breve o null"
    }}
  }},
  "macro_summary": "Riassunto in 2 frasi della view macro generale del report",
  "key_risks": ["rischio 1", "rischio 2", "rischio 3"],
  "key_opportunities": ["opportunità 1", "opportunità 2"]
}}"""

def extract_text_from_pdf(pdf_path, max_pages=30):
    """Estrae testo dalle prime max_pages pagine del PDF"""
    text_parts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total = len(pdf.pages)
            pages_to_read = min(max_pages, total)
            print(f"    PDF: {total} pagine totali, leggo le prime {pages_to_read}")

            for i, page in enumerate(pdf.pages[:pages_to_read]):
                text = page.extract_text()
                if text:
                    text_parts.append(f"[Pagina {i+1}]\n{text}")

        full_text = "\n\n".join(text_parts)
        print(f"    Estratto {len(full_text):,} caratteri")
        return full_text
    except Exception as e:
        print(f"    ✗ Errore estrazione: {e}")
        return None

def read_pdf_with_claude(text, source_id, source_name):
    """Invia il testo a Claude API per estrazione probabilità"""
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    scenario_list = "\n".join([f"- {k}: {v}" for k, v in SCENARIO_KEYS.items()])

    prompt = EXTRACTION_PROMPT.format(
        source_name=source_name,
        source_id=source_id,
        date=datetime.datetime.now().strftime("%Y-%m"),
        scenario_list=scenario_list,
    )

    # Tronca il testo se troppo lungo (~50k caratteri max)
    if len(text) > 50000:
        text = text[:50000] + "\n[... testo troncato per limite lunghezza ...]"

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[
                {
                    "role": "user",
                    "content": f"{prompt}\n\nTESTO DEL REPORT:\n{text}"
                }
            ]
        )

        response_text = message.content[0].text.strip()

        # Rimuovi eventuali backtick JSON
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]

        result = json.loads(response_text)
        print(f"    ✓ Claude ha estratto dati per {len(result.get('scenarios', {}))} scenari")
        return result

    except json.JSONDecodeError as e:
        print(f"    ✗ JSON non valido: {e}")
        return None
    except Exception as e:
        print(f"    ✗ Errore Claude API: {e}")
        return None

def merge_consensus(all_results):
    """Unisce i risultati di tutti i PDF in un consensus aggregato"""
    aggregated = {}

    for source_id, result in all_results.items():
        if not result or "scenarios" not in result:
            continue
        for scenario, data in result["scenarios"].items():
            if data.get("probability") is None:
                continue
            if scenario not in aggregated:
                aggregated[scenario] = {"probabilities": [], "views": [], "quotes": {}}
            aggregated[scenario]["probabilities"].append(data["probability"])
            aggregated[scenario]["views"].append(data.get("view", "neutro"))
            if data.get("quote"):
                aggregated[scenario]["quotes"][source_id] = data["quote"]

    # Calcola consensus finale
    consensus_probs = {}
    for scenario, data in aggregated.items():
        probs = data["probabilities"]
        if probs:
            consensus_probs[scenario] = round(sum(probs) / len(probs))

    return {
        "date": datetime.datetime.utcnow().isoformat() + "Z",
        "date_it": datetime.datetime.now().strftime("%d/%m/%Y"),
        "sources": list(all_results.keys()),
        "probabilities": consensus_probs,
        "detail": aggregated,
    }

def main():
    print(f"🤖 RSE PDF Reader — Claude API")
    print(f"   {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Verifica API key
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("✗ ANTHROPIC_API_KEY non configurata")
        print("  Aggiungi il segreto su GitHub: Settings → Secrets → ANTHROPIC_API_KEY")
        sys.exit(1)

    # Leggi log download
    try:
        with open("data/pdf_sources.json") as f:
            pdf_sources = json.load(f)
    except:
        print("✗ data/pdf_sources.json non trovato — esegui prima fetch_pdfs.py")
        sys.exit(1)

    all_results = {}

    for source in pdf_sources.get("sources", []):
        path = source.get("path")
        if not path or not Path(path).exists():
            print(f"  — {source['name']} — PDF non disponibile")
            continue

        print(f"\n  📄 {source['name']} ({source['org']})")

        # Estrai testo
        text = extract_text_from_pdf(path)
        if not text:
            continue

        # Leggi con Claude
        result = read_pdf_with_claude(text, source["id"], source["name"])
        if result:
            all_results[source["id"]] = result

            # Salva risultato individuale
            out_path = f"data/consensus_{source['id']}.json"
            with open(out_path, "w") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f"    Salvato: {out_path}")

    if not all_results:
        print("\n⚠ Nessun PDF elaborato — controlla i download")
        return

    # Merge consensus
    consensus = merge_consensus(all_results)

    with open("data/consensus.json", "w") as f:
        json.dump(consensus, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Consensus salvato: data/consensus.json")
    print(f"   Fonti elaborate: {len(all_results)}")
    print(f"   Scenari con dati: {len(consensus['probabilities'])}")

if __name__ == "__main__":
    main()
