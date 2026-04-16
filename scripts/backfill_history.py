#!/usr/bin/env python3
"""
RSE Backfill History — esegui UNA VOLTA SOLA
Scarica storico dal 2010, ricalcola regime ogni settimana,
popola regime_history nel market_data_rse.json esistente.
"""
import json, os, datetime
import yfinance as yf
import numpy as np
from datetime import date, timedelta

START_DATE   = "2010-01-04"
DATA_FILE    = "data/market_data_rse.json"
REGIMES      = ["BULL","ESPANSIONE","NEUTRO","RALLENTAMENTO","BEAR"]
BACKFILL_KEY = "backfill_done"

# ── FETCH STORICO ──────────────────────────────────────────────
HIST_TICKERS = {
    "VIX":    "^VIX",
    "US10Y":  "^TNX",
    "US2Y":   "^IRX",
    "SP500":  "^GSPC",
    "CRUDE":  "CL=F",
    "STOXX":  "^STOXX50E",
    "COPPER": "HG=F",
    "IHYG":   "IHYG.MI",
    "NIKKEI": "^N225",
    "HSI":    "^HSI",
}

def fetch_all_history():
    print("📡 Download storico dal 2010...")
    data = {}
    for key, ticker in HIST_TICKERS.items():
        try:
            df = yf.download(ticker, start=START_DATE, interval="1d",
                             progress=False, auto_adjust=True)
            if df is None or df.empty:
                print(f"  ✗ {key}")
                continue
            if hasattr(df.columns, 'get_level_values'):
                df.columns = df.columns.get_level_values(0)
            closes = df["Close"].dropna()
            data[key] = {
                str(d.date()): float(v)
                for d, v in zip(closes.index, closes.values)
            }
            print(f"  ✓ {key}: {len(data[key])} giorni")
        except Exception as e:
            print(f"  ✗ {key}: {e}")
    return data

def get_val(data, key, date_str, fallback=None):
    """Prende il valore più recente disponibile fino a date_str."""
    series = data.get(key, {})
    if not series:
        return fallback
    best = fallback
    for d in sorted(series.keys()):
        if d <= date_str:
            best = series[d]
        else:
            break
    return best

def calc_chg_month(data, key, date_str):
    """Calcola variazione % mensile (~22 giorni lavorativi)."""
    dates = sorted(d for d in data.get(key, {}) if d <= date_str)
    if len(dates) < 5:
        return 0
    current = data[key][dates[-1]]
    # ~22 giorni fa
    idx = max(0, len(dates) - 22)
    prev = data[key][dates[idx]]
    return round((current - prev) / prev * 100, 2) if prev else 0

# ── REGIME PER SETTIMANA ───────────────────────────────────────
def classify_week_backfill(data, date_str):
    """Classifica il regime per una settimana specifica."""
    vix       = get_val(data, "VIX",    date_str, 20)
    us10y     = get_val(data, "US10Y",  date_str, 4.0)
    us2y      = get_val(data, "US2Y",   date_str, 4.5)
    sp500_chg = calc_chg_month(data, "SP500",  date_str)
    stoxx_chg = calc_chg_month(data, "STOXX",  date_str)
    copper_chg= calc_chg_month(data, "COPPER", date_str)
    nikkei_chg= calc_chg_month(data, "NIKKEI", date_str)
    hsi_chg   = calc_chg_month(data, "HSI",    date_str)
    ihyg_chg  = calc_chg_month(data, "IHYG",   date_str)

    score_usa = 0
    # VIX
    if vix < 15:    score_usa += 2
    elif vix < 20:  score_usa += 1
    elif vix < 30:  score_usa -= 1
    else:           score_usa -= 2
    # Yield curve
    spread = (us10y or 4.0) - (us2y or 4.5)
    if spread > 0.5:  score_usa += 1
    elif spread < 0:  score_usa -= 1
    # S&P
    if sp500_chg > 3:    score_usa += 1
    elif sp500_chg < -5: score_usa -= 1

    score_eu = 0
    if stoxx_chg > 3:    score_eu += 1
    elif stoxx_chg < -4: score_eu -= 1
    if ihyg_chg < -3:    score_eu -= 1

    score_asia = 0
    if nikkei_chg > 3:    score_asia += 1
    elif nikkei_chg < -5: score_asia -= 1
    if hsi_chg > 3:       score_asia += 1
    elif hsi_chg < -5:    score_asia -= 1
    if copper_chg > 5:    score_asia += 1
    elif copper_chg < -5: score_asia -= 1

    # Pesi standard
    ws = score_usa*0.40 + score_eu*0.35 + score_asia*0.25
    ws = round(ws, 2)

    if ws >= 2.0:     return "BULL",          ws
    elif ws >= 0.8:   return "ESPANSIONE",    ws
    elif ws >= -0.8:  return "NEUTRO",        ws
    elif ws >= -1.5:  return "RALLENTAMENTO", ws
    else:             return "BEAR",          ws

# ── GENERA SETTIMANE ───────────────────────────────────────────
def generate_mondays(start_str, end_str):
    """Genera tutti i lunedì dal start al end."""
    start = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.datetime.strptime(end_str,   "%Y-%m-%d").date()
    # Vai al primo lunedì
    while start.weekday() != 0:
        start += timedelta(days=1)
    mondays = []
    while start <= end:
        mondays.append(start.isoformat())
        start += timedelta(days=7)
    return mondays

# ── MAIN ───────────────────────────────────────────────────────
def main():
    print("="*60)
    print("RSE BACKFILL HISTORY — run una volta sola")
    print("="*60)

    # Controlla se già eseguito
    existing = {}
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            existing = json.load(f)
        if existing.get(BACKFILL_KEY):
            print("⚠️  Backfill già eseguito. Uscita.")
            print("   Per forzare ri-esecuzione: rimuovi 'backfill_done' dal JSON")
            return

    # Fetch storico
    data = fetch_all_history()
    if not data:
        print("❌ Nessun dato scaricato — uscita")
        return

    # Genera settimane dal 2010 ad oggi
    today_str = date.today().isoformat()
    mondays   = generate_mondays(START_DATE, today_str)
    print(f"\n📅 Settimane da classificare: {len(mondays)}")

    # Classifica ogni settimana
    history = []
    regime_counts = {r: 0 for r in REGIMES}
    prev_regime = None
    run_len     = 0
    transitions = 0

    for i, monday in enumerate(mondays):
        regime, score = classify_week_backfill(data, monday)
        history.append({"date": monday, "regime": regime, "score": score})
        regime_counts[regime] += 1
        if prev_regime and regime != prev_regime:
            transitions += 1
        if regime == prev_regime:
            run_len += 1
        else:
            run_len = 1
        prev_regime = regime

        if (i+1) % 50 == 0:
            print(f"  {i+1}/{len(mondays)} — {monday} → {regime} ({score:+.2f})")

    print(f"\n✅ Classificate {len(history)} settimane")
    print(f"   Distribuzione: {regime_counts}")
    print(f"   Transizioni: {transitions}")

    # Statistiche durate
    durate = {r: [] for r in REGIMES}
    cur_r = history[0]["regime"]
    cur_len = 1
    for i in range(1, len(history)):
        if history[i]["regime"] == cur_r:
            cur_len += 1
        else:
            durate[cur_r].append(cur_len)
            cur_r   = history[i]["regime"]
            cur_len = 1

    print("\n📊 Durate medie storiche (settimane):")
    for r in REGIMES:
        if durate[r]:
            media = round(np.mean(durate[r]), 1)
            massimo = max(durate[r])
            n_episodi = len(durate[r])
            print(f"   {r:15s}: media={media:5.1f}w  max={massimo:3d}w  episodi={n_episodi}")
        else:
            print(f"   {r:15s}: nessun episodio completo")

    # Aggiorna il JSON esistente
    existing["regime_history"] = history
    existing[BACKFILL_KEY]     = today_str
    existing["backfill_info"]  = {
        "date":              today_str,
        "settimane":         len(history),
        "da":                mondays[0],
        "a":                 mondays[-1],
        "distribuzione":     regime_counts,
        "transizioni":       transitions,
        "durate_medie":      {r: round(float(np.mean(durate[r])),1) if durate[r] else None for r in REGIMES},
    }

    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Salvato {DATA_FILE}")
    print(f"   regime_history: {len(history)} settimane dal {mondays[0]} al {mondays[-1]}")
    print("   Il Markov sarà calibrato al prossimo run del workflow principale.")

if __name__ == "__main__":
    main()
