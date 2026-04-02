#!/usr/bin/env python3
"""
RSE Market Data Fetcher v2.0
Regime a tre aree: USA (40%) · Europa (35%) · Asia (25%)
Pesi dinamici basati su volatilità corrente
"""
import json, os, datetime
import yfinance as yf
import numpy as np

INDICATORS = {
    # ── USA ──────────────────────────────────────────────────
    "VIX":      {"ticker": "^VIX",       "n": "CBOE Volatility Index",         "cat": "risk",      "area": "usa"},
    "US10Y":    {"ticker": "^TNX",       "n": "US Treasury 10Y Yield",         "cat": "rates",     "area": "usa"},
    "US2Y":     {"ticker": "^IRX",       "n": "US Treasury 2Y Yield",          "cat": "rates",     "area": "usa"},
    "SP500":    {"ticker": "^GSPC",      "n": "S&P 500",                       "cat": "equity",    "area": "usa"},
    "CRUDE":    {"ticker": "CL=F",       "n": "WTI Crude Oil Futures",         "cat": "commodity", "area": "global"},
    "GOLD":     {"ticker": "GC=F",       "n": "Gold Futures",                  "cat": "commodity", "area": "global"},
    "DOLLAR":   {"ticker": "DX-Y.NYB",   "n": "US Dollar Index",               "cat": "fx",        "area": "global"},
    "EURUSD":   {"ticker": "EURUSD=X",   "n": "EUR/USD",                       "cat": "fx",        "area": "global"},
    "BITCOIN":  {"ticker": "BTC-EUR",    "n": "Bitcoin EUR",                   "cat": "crypto",    "area": "global"},
    "COPPER":   {"ticker": "HG=F",       "n": "Copper Futures",                "cat": "commodity", "area": "global"},

    # ── EUROPA ───────────────────────────────────────────────
    "STOXX":    {"ticker": "^STOXX50E",  "n": "Euro Stoxx 50",                 "cat": "equity",    "area": "eu"},
    "DAX":      {"ticker": "^GDAXI",     "n": "DAX Germania",                  "cat": "equity",    "area": "eu"},
    "CAC":      {"ticker": "^FCHI",      "n": "CAC 40 Francia",                "cat": "equity",    "area": "eu"},
    "FTSE":     {"ticker": "^FTSE",      "n": "FTSE 100 Gran Bretagna",        "cat": "equity",    "area": "eu"},
    "BTP10":    {"ticker": "BTP10.MI",   "n": "Amundi BTP 10Y (spread proxy)", "cat": "rates",     "area": "eu"},
    "BUND10":   {"ticker": "EXX6.DE",    "n": "iShares Bund 10+Y",             "cat": "rates",     "area": "eu"},
    "IHYG":     {"ticker": "IHYG.MI",    "n": "iShares EUR High Yield Bond",   "cat": "credit",    "area": "eu"},
    "IEAC":     {"ticker": "IEAC.MI",    "n": "iShares EUR Corp Bond",         "cat": "credit",    "area": "eu"},

    # ── ASIA ─────────────────────────────────────────────────
    "NIKKEI":   {"ticker": "^N225",      "n": "Nikkei 225",                    "cat": "equity",    "area": "asia"},
    "HSI":      {"ticker": "^HSI",       "n": "Hang Seng Index",               "cat": "equity",    "area": "asia"},
}


def fetch_indicator(key, info):
    try:
        tk = yf.Ticker(info["ticker"])
        hist = tk.history(period="3mo")
        if hist.empty or len(hist) < 5:
            return {"ok": False, "error": "No data"}
        closes = list(hist["Close"].values)
        # Sanitize NaN
        closes = [float(v) for v in closes if v is not None and not (isinstance(v, float) and np.isnan(v))]
        if len(closes) < 5:
            return {"ok": False, "error": "Dati insufficienti"}
        price = closes[-1]
        prev_week  = closes[-6]  if len(closes) >= 6  else closes[0]
        prev_month = closes[-22] if len(closes) >= 22 else closes[0]
        chg_week   = (price - prev_week)  / prev_week  * 100 if prev_week  else 0
        chg_month  = (price - prev_month) / prev_month * 100 if prev_month else 0
        sma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else None
        # Volatilità 20gg (deviazione standard dei rendimenti giornalieri)
        if len(closes) >= 20:
            returns = [closes[i]/closes[i-1]-1 for i in range(max(1, len(closes)-20), len(closes))]
            vol20 = float(np.std(returns) * np.sqrt(252) * 100)  # annualizzata %
        else:
            vol20 = None
        return {
            "ok": True,
            "price":     round(price, 4),
            "chg_week":  round(chg_week, 2),
            "chg_month": round(chg_month, 2),
            "sma20":     round(sma20, 4) if sma20 else None,
            "vol20":     round(vol20, 2) if vol20 else None,
            "ts":        datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:80]}


def calc_regime_usa(r):
    """Regime USA — peso base 40%"""
    score = 0
    signals = []

    vix       = r.get("VIX",   {}).get("price",     20)
    us10y     = r.get("US10Y", {}).get("price",     4.0)
    us2y      = r.get("US2Y",  {}).get("price",     4.5)
    sp500_chg = r.get("SP500", {}).get("chg_month",   0)
    crude_chg = r.get("CRUDE", {}).get("chg_month",   0)

    # VIX
    if vix < 15:
        score += 2; signals.append(f"VIX {vix:.1f} — bassa volatilità (rialzista)")
    elif vix < 20:
        score += 1; signals.append(f"VIX {vix:.1f} — volatilità normale")
    elif vix < 30:
        score -= 1; signals.append(f"VIX {vix:.1f} — volatilità elevata (attenzione)")
    else:
        score -= 2; signals.append(f"VIX {vix:.1f} — paura elevata (ribassista)")

    # Curva USA
    spread = us10y - us2y
    if spread > 0.5:
        score += 1; signals.append(f"Curva USA normale +{spread:.2f}% (espansione)")
    elif spread > 0:
        signals.append(f"Curva USA piatta +{spread:.2f}% (rallentamento)")
    else:
        score -= 1; signals.append(f"Curva USA invertita {spread:.2f}% (rischio recessione)")

    # S&P 500
    if sp500_chg > 3:
        score += 1; signals.append(f"S&P500 +{sp500_chg:.1f}% mensile (momentum rialzista)")
    elif sp500_chg < -5:
        score -= 1; signals.append(f"S&P500 {sp500_chg:.1f}% mensile (pressione ribassista)")

    # Petrolio
    if crude_chg > 10:
        signals.append(f"WTI +{crude_chg:.1f}% — pressione inflattiva USA")
    elif crude_chg < -10:
        signals.append(f"WTI {crude_chg:.1f}% — rallentamento domanda")

    return {"score": score, "signals": signals, "vix": vix}


def calc_regime_eu(r):
    """Regime Europa — peso base 35%"""
    score = 0
    signals = []

    stoxx_chg = r.get("STOXX", {}).get("chg_month", 0)
    dax_chg   = r.get("DAX",   {}).get("chg_month", 0)
    cac_chg   = r.get("CAC",   {}).get("chg_month", 0)
    ftse_chg  = r.get("FTSE",  {}).get("chg_month", 0)
    eurusd    = r.get("EURUSD",{}).get("price",    1.08)

    btp_chg   = r.get("BTP10",  {}).get("chg_month", 0)
    bund_chg  = r.get("BUND10", {}).get("chg_month", 0)
    ihyg_chg  = r.get("IHYG",   {}).get("chg_month", 0)
    ieac_chg  = r.get("IEAC",   {}).get("chg_month", 0)

    # Azionario Europa (media dei 4 indici)
    eq_chg = (stoxx_chg + dax_chg + cac_chg + ftse_chg) / 4
    if eq_chg > 3:
        score += 1; signals.append(f"Azionario EU +{eq_chg:.1f}% (momentum positivo)")
    elif eq_chg < -4:
        score -= 1; signals.append(f"Azionario EU {eq_chg:.1f}% (pressione ribassista)")

    # Spread BTP-Bund (proxy: differenza di performance mensile)
    btp_bund_diff = btp_chg - bund_chg
    if btp_bund_diff < -2:
        score -= 2; signals.append(f"⚠️ STRESS PERIFERICO — spread BTP-Bund in allargamento ({btp_bund_diff:.1f}%)")
    elif btp_bund_diff < -1:
        score -= 1; signals.append(f"Spread BTP-Bund in allargamento ({btp_bund_diff:.1f}%) — attenzione")
    elif btp_bund_diff > 1:
        score += 1; signals.append(f"Spread BTP-Bund in compressione ({btp_bund_diff:.1f}%) — positivo")
    else:
        signals.append(f"Spread BTP-Bund stabile ({btp_bund_diff:.1f}%)")

    # Credito europeo
    if ihyg_chg < -3:
        score -= 1; signals.append(f"High Yield EU {ihyg_chg:.1f}% — stress credito")
    if ieac_chg < -2:
        score -= 1; signals.append(f"Corp Bond EU {ieac_chg:.1f}% — allargamento spread IG")
    if ihyg_chg > 1 and ieac_chg > 0.5:
        score += 1; signals.append(f"Credito EU positivo (HY +{ihyg_chg:.1f}%)")

    # EUR/USD
    if eurusd < 1.02:
        score -= 1; signals.append(f"EUR/USD {eurusd:.4f} — euro debole (pressione importazioni)")
    elif eurusd > 1.12:
        signals.append(f"EUR/USD {eurusd:.4f} — euro forte (attenzione export)")

    return {"score": score, "signals": signals, "btp_bund_diff": btp_bund_diff}


def calc_regime_asia(r):
    """Regime Asia — peso base 25%"""
    score = 0
    signals = []

    nikkei_chg = r.get("NIKKEI", {}).get("chg_month", 0)
    hsi_chg    = r.get("HSI",    {}).get("chg_month", 0)
    copper_chg = r.get("COPPER", {}).get("chg_month", 0)

    # Nikkei
    if nikkei_chg > 3:
        score += 1; signals.append(f"Nikkei +{nikkei_chg:.1f}% (momentum Asia positivo)")
    elif nikkei_chg < -5:
        score -= 1; signals.append(f"Nikkei {nikkei_chg:.1f}% (debolezza Giappone)")

    # Hang Seng
    if hsi_chg > 3:
        score += 1; signals.append(f"Hang Seng +{hsi_chg:.1f}% (ripresa China/HK)")
    elif hsi_chg < -5:
        score -= 1; signals.append(f"Hang Seng {hsi_chg:.1f}% (debolezza China/HK)")

    # Rame — proxy crescita globale e domanda cinese
    if copper_chg > 5:
        score += 1; signals.append(f"Rame +{copper_chg:.1f}% — domanda globale in crescita")
    elif copper_chg < -5:
        score -= 1; signals.append(f"Rame {copper_chg:.1f}% — segnale rallentamento globale")

    return {"score": score, "signals": signals}


def calc_dynamic_weights(r):
    """
    Pesi dinamici basati sulla volatilità corrente.
    Se VIX > 30 → USA domina (contagio parte da lì).
    Se stress periferico EU → EU scala.
    Default: USA 40%, EU 35%, Asia 25%.
    """
    w_usa, w_eu, w_asia = 0.40, 0.35, 0.25

    vix = r.get("VIX", {}).get("price", 20)
    btp_chg  = r.get("BTP10",  {}).get("chg_month", 0)
    bund_chg = r.get("BUND10", {}).get("chg_month", 0)
    hsi_chg  = r.get("HSI",    {}).get("chg_month", 0)

    btp_bund_diff = btp_chg - bund_chg

    # VIX alto → USA domina
    if vix > 35:
        w_usa, w_eu, w_asia = 0.55, 0.28, 0.17
    elif vix > 30:
        w_usa, w_eu, w_asia = 0.50, 0.30, 0.20

    # Stress periferico EU → EU scala
    elif btp_bund_diff < -2:
        w_usa, w_eu, w_asia = 0.35, 0.45, 0.20

    # Crollo Asia → Asia scala
    elif hsi_chg < -10:
        w_usa, w_eu, w_asia = 0.38, 0.32, 0.30

    return {"usa": w_usa, "eu": w_eu, "asia": w_asia}


def calc_global_regime(usa, eu, asia, weights):
    """Combina i tre regimi con pesi dinamici"""
    # Score pesato
    weighted_score = (
        usa["score"]  * weights["usa"]  +
        eu["score"]   * weights["eu"]   +
        asia["score"] * weights["asia"]
    )
    weighted_score = round(weighted_score, 2)

    # Regime finale
    if weighted_score >= 2.0:
        regime = "BULL"
    elif weighted_score >= 0.8:
        regime = "ESPANSIONE"
    elif weighted_score >= -0.8:
        regime = "NEUTRO"
    elif weighted_score >= -1.5:
        regime = "RALLENTAMENTO"
    else:
        regime = "BEAR"

    # Segnali combinati (max 6 totali, priorità per score estremi)
    all_signals = usa["signals"] + eu["signals"] + asia["signals"]

    return {
        "regime":       regime,
        "score":        weighted_score,
        "score_usa":    usa["score"],
        "score_eu":     eu["score"],
        "score_asia":   asia["score"],
        "weights":      weights,
        "signals":      all_signals[:8],
    }


def calc_scenario_probabilities(r, regime_data):
    """Aggiorna probabilità scenari basandosi sui dati di mercato"""
    vix        = r.get("VIX",    {}).get("price",     20)
    us10y      = r.get("US10Y",  {}).get("price",     4.0)
    us2y       = r.get("US2Y",   {}).get("price",     4.5)
    crude      = r.get("CRUDE",  {}).get("price",      80)
    crude_chg  = r.get("CRUDE",  {}).get("chg_month",   0)
    gold_chg   = r.get("GOLD",   {}).get("chg_month",   0)
    sp500_chg  = r.get("SP500",  {}).get("chg_month",   0)
    eurusd     = r.get("EURUSD", {}).get("price",     1.08)
    copper_chg = r.get("COPPER", {}).get("chg_month",   0)
    nikkei_chg = r.get("NIKKEI", {}).get("chg_month",   0)
    hsi_chg    = r.get("HSI",    {}).get("chg_month",   0)
    ihyg_chg   = r.get("IHYG",   {}).get("chg_month",   0)
    btp_chg    = r.get("BTP10",  {}).get("chg_month",   0)
    bund_chg   = r.get("BUND10", {}).get("chg_month",   0)
    btp_bund   = btp_chg - bund_chg
    curve_spread = us10y - us2y

    probs = {}

    # ── GEOPOLITICI ──────────────────────────────────────────
    probs["petrolio_alto"]       = min(90, max(10, 35 + crude_chg * 1.5))
    probs["guerra_commerciale"]  = max(20, min(70, 40 + (copper_chg < -5) * 10))
    probs["crisi_taiwan"]        = max(10, min(40, 15 + (hsi_chg < -8) * 10))
    probs["hormuz"]              = max(5,  min(50, 10 + crude_chg * 0.8))

    # ── MACRO EUROPA ─────────────────────────────────────────
    probs["recessione_germania"] = max(20, min(75, 45 - curve_spread * 8 + (r.get("DAX",{}).get("chg_month",0) < -5) * 10))
    probs["crisi_btp"]           = max(5,  min(70, 15 + (vix - 20) * 1.5 + max(0, -btp_bund) * 5))
    probs["bce_alza"]            = max(5,  min(70, 20 + crude_chg * 0.8))
    probs["deflazione_eu"]       = max(3,  min(30, 15 - crude_chg * 0.5))
    probs["stress_periferico"]   = max(5,  min(80, 20 + max(0, -btp_bund) * 15 + (ihyg_chg < -3) * 15))

    # ── MACRO USA ────────────────────────────────────────────
    probs["hard_landing_usa"]    = max(10, min(70, 25 + (curve_spread < 0) * 20 + (vix > 25) * 10))
    probs["soft_landing"]        = max(15, min(75, 55 - (vix - 20) * 2))
    probs["stagflazione_usa"]    = max(5,  min(55, 20 + crude_chg * 0.6))
    probs["boom_ai"]             = max(30, min(80, 65 + sp500_chg * 0.5))

    # ── FINANZIARI ───────────────────────────────────────────
    probs["flash_crash"]         = max(5,  min(45, 10 + (vix > 25) * 15 + (vix > 35) * 15))
    probs["bear_market"]         = max(5,  min(55, 15 + (curve_spread < 0) * 15 + (vix > 30) * 10 + (ihyg_chg < -5) * 10))
    probs["bull_estremo"]        = max(5,  min(50, 20 + sp500_chg * 1.2 + (vix < 15) * 10))
    probs["inversione_curva"]    = max(5,  min(70, 30 + (curve_spread < 0) * 30))
    probs["crisi_credito_eu"]    = max(5,  min(60, 15 + max(0, -ihyg_chg) * 5 + max(0, -btp_bund) * 8))

    # ── ASIA ─────────────────────────────────────────────────
    probs["rallentamento_cina"]  = max(10, min(70, 30 + max(0, -hsi_chg) * 2 + max(0, -copper_chg) * 2))
    probs["crisi_giappone"]      = max(5,  min(40, 10 + max(0, -nikkei_chg) * 1.5))
    probs["boom_asia_emergenti"] = max(15, min(65, 35 + (hsi_chg > 5) * 10 + (copper_chg > 5) * 10))

    # ── COMMODITY ────────────────────────────────────────────
    probs["superciclo_commodity"] = max(20, min(70, 35 + crude_chg * 0.8 + gold_chg * 0.5 + copper_chg * 0.5))
    probs["dollaro_debole"]       = max(15, min(65, 40 - (eurusd - 1.08) * 100))

    # ── STRUTTURALI (meno sensibili ai dati) ─────────────────
    probs["bitcoin_mainstream"]   = 45
    probs["regolamentazione_crypto"] = 55
    probs["carbon_tax"]           = 50
    probs["crisi_idrica"]         = 25
    probs["rinascita_industriale_eu"] = max(20, min(60, 35 + (eurusd < 1.05) * 10))
    probs["boom_demografico_africa"]  = 70
    probs["rivoluzione_farmaceutica"] = 75
    probs["fusione_nucleare"]     = 15
    probs["pace_ucraina"]         = 30
    probs["credito_privato_boom"] = 55
    probs["small_cap_outperform"] = max(20, min(60, 40))
    probs["dividendi_moda"]       = 50
    probs["invecchiamento_europa"] = 90
    probs["gen_z_investe"]        = 75
    probs["pensioni_pressione"]   = 80
    probs["immigrazione_europa"]  = 60
    probs["robotica_automazione"] = 70
    probs["quantum_computing"]    = 25
    probs["spazio_commerciale"]   = 45
    probs["fintech_decentralizzato"] = 50
    probs["governo_tecnico_italia"]  = 20
    probs["federalismo_eu"]       = 30
    probs["trump_completo"]       = 55
    probs["multipolarismo"]       = 65
    probs["lusso_italiano"]       = max(30, min(65, 55 + (eurusd > 1.10) * (-5)))
    probs["turismo_record"]       = 65
    probs["crisi_banche_usa"]     = max(10, min(45, 20 + (vix > 30) * 10))
    probs["pe_democratizzato"]    = 45
    probs["re_commerciale_crisi"] = 55
    probs["infrastrutture_boom"]  = 70
    probs["cbdc_europeo"]         = 60
    probs["dedollarizzazione"]    = max(30, min(70, 45 + (eurusd > 1.10) * 10))
    probs["inflazione_strutturale"] = max(20, min(65, 35 + crude_chg * 0.5))
    probs["repressione_finanziaria"] = max(15, min(55, 30 - us10y * 2))

    return {k: round(v) for k, v in probs.items()}


def main():
    os.makedirs("data", exist_ok=True)
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f"🌍 RSE Market Fetcher v2.0 — {now_str}")
    print(f"   Indicatori: {len(INDICATORS)} | Aree: USA · Europa · Asia")

    # ── FETCH ────────────────────────────────────────────────
    results = {}
    ok_count = 0
    for key, info in INDICATORS.items():
        print(f"  {key:10s} ({info['ticker']:15s})", end=" ")
        data = fetch_indicator(key, info)
        results[key] = {**info, **data}
        if data["ok"]:
            ok_count += 1
            print(f"✓  {data['price']:>12.4f}  Δ1m: {data.get('chg_month',0):+.1f}%")
        else:
            print(f"✗  {data.get('error','?')}")

    print(f"\n  Fetch completato: {ok_count}/{len(INDICATORS)} OK")

    # ── REGIME ───────────────────────────────────────────────
    reg_usa  = calc_regime_usa(results)
    reg_eu   = calc_regime_eu(results)
    reg_asia = calc_regime_asia(results)
    weights  = calc_dynamic_weights(results)
    regime   = calc_global_regime(reg_usa, reg_eu, reg_asia, weights)

    print(f"\n  Pesi dinamici: USA {weights['usa']*100:.0f}% · EU {weights['eu']*100:.0f}% · Asia {weights['asia']*100:.0f}%")
    print(f"  Score: USA {reg_usa['score']:+.0f} · EU {reg_eu['score']:+.0f} · Asia {reg_asia['score']:+.0f} → Globale {regime['score']:+.2f}")
    print(f"  Regime: {regime['regime']}")

    # ── SCENARI ──────────────────────────────────────────────
    probs = calc_scenario_probabilities(results, regime)

    # Merge consensus e override manuali
    consensus = {}
    try:
        with open("data/consensus.json") as f:
            consensus = json.load(f)
    except:
        pass

    overrides = {}
    try:
        with open("data/overrides.json") as f:
            overrides = json.load(f)
    except:
        pass

    final_probs = {**probs}
    for k, v in consensus.get("probabilities", {}).items():
        if k in final_probs:
            final_probs[k] = round((final_probs[k] + v) / 2)
        else:
            final_probs[k] = v
    for k, v in overrides.items():
        final_probs[k] = v

    # ── OUTPUT ───────────────────────────────────────────────
    output = {
        "meta": {
            "updated":        datetime.datetime.utcnow().isoformat() + "Z",
            "updated_it":     datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
            "regime":         regime["regime"],
            "regime_score":   regime["score"],
            "regime_score_usa":  reg_usa["score"],
            "regime_score_eu":   reg_eu["score"],
            "regime_score_asia": reg_asia["score"],
            "regime_weights": weights,
            "regime_signals": regime["signals"],
            "btp_bund_diff":  reg_eu.get("btp_bund_diff", 0),
            "fetch_ok":       ok_count,
            "fetch_total":    len(INDICATORS),
        },
        "indicators":         results,
        "probabilities":      final_probs,
        "consensus_date":     consensus.get("date", None),
        "consensus_sources":  consensus.get("sources", []),
    }

    with open("data/market_data_rse.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Salvato data/market_data_rse.json")
    print(f"   Regime: {regime['regime']} (score globale: {regime['score']:+.2f})")
    print(f"   Score USA: {reg_usa['score']:+d} | EU: {reg_eu['score']:+d} | Asia: {reg_asia['score']:+d}")
    print(f"   Scenari calcolati: {len(final_probs)}")


if __name__ == "__main__":
    main()
