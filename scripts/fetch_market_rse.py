#!/usr/bin/env python3
"""
RSE Market Data Fetcher
Scarica indicatori macro da Yahoo Finance 2x/giorno
Alimenta data/market_data.json e aggiorna probabilità scenari
"""
import json, os, datetime
import yfinance as yf
import numpy as np

INDICATORS = {
    "VIX":      {"ticker":"^VIX",      "n":"CBOE Volatility Index",        "cat":"risk"},
    "US10Y":    {"ticker":"^TNX",      "n":"US Treasury 10Y Yield",        "cat":"rates"},
    "US2Y":     {"ticker":"^IRX",      "n":"US Treasury 2Y Yield",         "cat":"rates"},
    "CRUDE":    {"ticker":"CL=F",      "n":"WTI Crude Oil Futures",        "cat":"commodity"},
    "GOLD":     {"ticker":"GC=F",      "n":"Gold Futures",                 "cat":"commodity"},
    "DOLLAR":   {"ticker":"DX-Y.NYB",  "n":"US Dollar Index",              "cat":"fx"},
    "EURUSD":   {"ticker":"EURUSD=X",  "n":"EUR/USD",                      "cat":"fx"},
    "SP500":    {"ticker":"^GSPC",     "n":"S&P 500",                      "cat":"equity"},
    "STOXX":    {"ticker":"^STOXX50E", "n":"Euro Stoxx 50",                "cat":"equity"},
    "IHYG":     {"ticker":"IHYG.DE",   "n":"iShares EUR HY Bond (spread proxy)", "cat":"credit"},
    "IEAC":     {"ticker":"IEAC.DE",   "n":"iShares EUR Corp Bond",        "cat":"credit"},
    "IBGM":     {"ticker":"IBGM.DE",   "n":"iShares EUR Govt Bond 3-7yr",  "cat":"rates"},
    "BTPBUND":  {"ticker":"ITLYF",     "n":"BTP Futures (spread proxy)",   "cat":"rates"},
    "BITCOIN":  {"ticker":"BTC-EUR",   "n":"Bitcoin EUR",                  "cat":"crypto"},
}

def fetch_indicator(key, info):
    try:
        tk = yf.Ticker(info["ticker"])
        hist = tk.history(period="3mo")
        if hist.empty or len(hist) < 5:
            return {"ok": False, "error": "No data"}
        closes = list(hist["Close"].values)
        price = float(closes[-1])
        prev_week = float(closes[-6]) if len(closes) >= 6 else closes[0]
        prev_month = float(closes[-22]) if len(closes) >= 22 else closes[0]
        chg_week = (price - prev_week) / prev_week * 100
        chg_month = (price - prev_month) / prev_month * 100
        sma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else None
        return {
            "ok": True,
            "price": round(price, 4),
            "chg_week": round(chg_week, 2),
            "chg_month": round(chg_month, 2),
            "sma20": round(sma20, 4) if sma20 else None,
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def calc_regime(results):
    """Calcola regime di mercato dai dati macro"""
    vix = results.get("VIX", {}).get("price", 20)
    us10y = results.get("US10Y", {}).get("price", 4.0)
    us2y = results.get("US2Y", {}).get("price", 4.5)
    sp500_chg = results.get("SP500", {}).get("chg_month", 0)
    crude_chg = results.get("CRUDE", {}).get("chg_month", 0)

    score = 0
    signals = []

    # VIX
    if vix < 15:
        score += 2; signals.append(f"VIX {vix:.1f} — bassa volatilità (rialzista)")
    elif vix < 20:
        score += 1; signals.append(f"VIX {vix:.1f} — volatilità normale")
    elif vix < 30:
        score -= 1; signals.append(f"VIX {vix:.1f} — volatilità elevata (attenzione)")
    else:
        score -= 2; signals.append(f"VIX {vix:.1f} — paura elevata (ribassista)")

    # Yield curve (2y-10y spread)
    spread = us10y - us2y
    if spread > 0.5:
        score += 1; signals.append(f"Curva normale +{spread:.2f}% (espansione)")
    elif spread > 0:
        signals.append(f"Curva piatta +{spread:.2f}% (rallentamento)")
    else:
        score -= 1; signals.append(f"Curva invertita {spread:.2f}% (rischio recessione)")

    # Equity trend
    if sp500_chg > 3:
        score += 1; signals.append(f"S&P500 +{sp500_chg:.1f}% mensile (momentum rialzista)")
    elif sp500_chg < -5:
        score -= 1; signals.append(f"S&P500 {sp500_chg:.1f}% mensile (pressione ribassista)")

    # Petrolio
    if crude_chg > 10:
        signals.append(f"Petrolio +{crude_chg:.1f}% — pressione inflattiva")
    elif crude_chg < -10:
        signals.append(f"Petrolio {crude_chg:.1f}% — rallentamento domanda")

    if score >= 3:
        regime = "BULL"
    elif score >= 1:
        regime = "ESPANSIONE"
    elif score >= -1:
        regime = "NEUTRO"
    elif score >= -2:
        regime = "RALLENTAMENTO"
    else:
        regime = "BEAR"

    return {"regime": regime, "score": score, "signals": signals}

def calc_scenario_probabilities(results, regime_data):
    """Aggiorna probabilità scenari basandosi sui dati di mercato"""
    vix = results.get("VIX", {}).get("price", 20)
    us10y = results.get("US10Y", {}).get("price", 4.0)
    us2y = results.get("US2Y", {}).get("price", 4.5)
    crude = results.get("CRUDE", {}).get("price", 80)
    crude_chg = results.get("CRUDE", {}).get("chg_month", 0)
    gold_chg = results.get("GOLD", {}).get("chg_month", 0)
    sp500_chg = results.get("SP500", {}).get("chg_month", 0)
    eurusd = results.get("EURUSD", {}).get("price", 1.08)
    curve_spread = us10y - us2y

    probs = {}

    # Scenari geopolitici
    probs["petrolio_alto"] = min(85, max(10, 35 + crude_chg * 1.5))
    probs["guerra_commerciale"] = 40  # base — aggiornato da consensus
    probs["crisi_taiwan"] = 15
    probs["hormuz"] = max(5, min(40, 10 + crude_chg * 0.8))

    # Scenari macro Europa
    probs["recessione_germania"] = max(20, min(75, 45 - curve_spread * 10))
    probs["crisi_btp"] = max(5, min(60, 15 + (vix - 20) * 1.5))
    probs["bce_alza"] = max(5, min(70, 20 + crude_chg * 0.8 + (vix < 20) * 5))
    probs["deflazione_eu"] = max(5, min(30, 15 - crude_chg * 0.5))

    # Scenari macro USA
    probs["hard_landing_usa"] = max(10, min(70, 25 + (curve_spread < 0) * 20 + (vix > 25) * 10))
    probs["soft_landing"] = max(15, min(75, 55 - (vix - 20) * 2))
    probs["stagflazione_usa"] = max(5, min(50, 20 + crude_chg * 0.6))
    probs["boom_ai"] = max(30, min(80, 65 + sp500_chg * 0.5))

    # Scenari finanziari
    probs["flash_crash"] = max(5, min(40, 10 + (vix > 25) * 15 + (vix > 35) * 15))
    probs["bear_market"] = max(5, min(50, 15 + (curve_spread < 0) * 15 + (vix > 30) * 10))
    probs["bull_estremo"] = max(5, min(50, 20 + sp500_chg * 1.2 + (vix < 15) * 10))
    probs["inversione_curva"] = max(5, min(70, 30 + (curve_spread < 0) * 30))

    # Scenari crypto
    probs["bitcoin_mainstream"] = max(20, min(70, 45))
    probs["regolamentazione_crypto"] = 55

    # Scenari climatici
    probs["carbon_tax"] = 50
    probs["crisi_idrica"] = 25

    # Scenari positivi strutturali
    probs["rinascita_industriale_eu"] = max(20, min(60, 35 + (eurusd < 1.05) * 10))
    probs["boom_demografico_africa"] = 70
    probs["rivoluzione_farmaceutica"] = 75
    probs["fusione_nucleare"] = 15
    probs["pace_ucraina"] = 30

    # Scenari ciclici positivi
    probs["superciclo_commodity"] = max(20, min(65, 35 + crude_chg * 0.8 + gold_chg * 0.5))
    probs["dollaro_debole"] = max(15, min(65, 40 - (eurusd - 1.08) * 100))
    probs["credito_privato_boom"] = 55
    probs["small_cap_outperform"] = max(20, min(60, 40))
    probs["dividendi_moda"] = 50

    # Demografici
    probs["invecchiamento_europa"] = 90
    probs["gen_z_investe"] = 75
    probs["pensioni_pressione"] = 80
    probs["immigrazione_europa"] = 60

    # Tecnologici positivi
    probs["robotica_automazione"] = 70
    probs["quantum_computing"] = 25
    probs["spazio_commerciale"] = 45
    probs["fintech_decentralizzato"] = 50

    # Politici
    probs["governo_tecnico_italia"] = 20
    probs["federalismo_eu"] = 30
    probs["trump_completo"] = 55
    probs["multipolarismo"] = 65

    # Settoriali
    probs["lusso_italiano"] = 55
    probs["turismo_record"] = 65
    probs["crisi_banche_usa"] = 20
    probs["pe_democratizzato"] = 45
    probs["re_commerciale_crisi"] = 55
    probs["infrastrutture_boom"] = 70

    # Monetari
    probs["cbdc_europeo"] = 60
    probs["dedollarizzazione"] = max(30, min(70, 45 + (eurusd > 1.10) * 10))
    probs["inflazione_strutturale"] = max(20, min(65, 35 + crude_chg * 0.5))
    probs["repressione_finanziaria"] = max(15, min(55, 30 - us10y * 2))

    # Arrotonda tutto
    return {k: round(v) for k, v in probs.items()}

def main():
    os.makedirs("data", exist_ok=True)
    print(f"🌍 RSE Market Fetcher — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

    results = {}
    for key, info in INDICATORS.items():
        print(f"  {key:10s} ({info['ticker']:15s})", end=" ")
        data = fetch_indicator(key, info)
        results[key] = {**info, **data}
        if data["ok"]:
            print(f"✓ {data['price']:.4f}  Δ1m:{data.get('chg_month',0):+.1f}%")
        else:
            print(f"✗ {data.get('error','?')}")

    regime = calc_regime(results)
    probs = calc_scenario_probabilities(results, regime)

    # Leggi consensus esistente se presente
    consensus = {}
    try:
        with open("data/consensus.json") as f:
            consensus = json.load(f)
    except:
        pass

    # Leggi override manuali se presenti
    overrides = {}
    try:
        with open("data/overrides.json") as f:
            overrides = json.load(f)
    except:
        pass

    # Merge: market probs + consensus override + manual override
    final_probs = {**probs}
    for k, v in consensus.get("probabilities", {}).items():
        if k in final_probs:
            final_probs[k] = round((final_probs[k] + v) / 2)
        else:
            final_probs[k] = v
    for k, v in overrides.items():
        final_probs[k] = v

    output = {
        "meta": {
            "updated": datetime.datetime.utcnow().isoformat() + "Z",
            "updated_it": datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
            "regime": regime["regime"],
            "regime_score": regime["score"],
            "regime_signals": regime["signals"],
        },
        "indicators": results,
        "probabilities": final_probs,
        "consensus_date": consensus.get("date", "—"),
        "consensus_sources": consensus.get("sources", []),
    }

    with open("data/market_data_rse.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Salvato data/market_data_rse.json")
    print(f"   Regime: {regime['regime']} (score: {regime['score']})")
    print(f"   Probabilità calcolate: {len(final_probs)} scenari")

if __name__ == "__main__":
    main()
