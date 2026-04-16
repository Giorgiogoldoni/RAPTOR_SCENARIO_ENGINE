#!/usr/bin/env python3
"""
RSE Market Data Fetcher v3.0
Regime a tre aree: USA (40%) · Europa (35%) · Asia (25%)
Pesi dinamici basati su volatilità corrente

v3.0 — aggiunto:
  - regime_history: storico settimanale accumulato (max 3 anni)
  - regime_duration: settimane correnti vs media storica
  - forecast Markov: orizzonti 3m / 6m / 12m
  - Duration-Adjusted Markov per orizzonti lunghi
  - scenari opzionali: automatici + manuali (Opzione C)
"""

import json, os, datetime
import yfinance as yf
import numpy as np

# ── CONFIGURAZIONE ────────────────────────────────────────────

MAX_HISTORY_WEEKS = 156   # 3 anni di storico settimanale
REGIMES = ["BULL", "ESPANSIONE", "NEUTRO", "RALLENTAMENTO", "BEAR"]

# Durate medie storiche per regime (settimane) — stima iniziale
# Verranno calibrate automaticamente dal dataset accumulato
DURATE_DEFAULT = {
    "BULL":          22,
    "ESPANSIONE":    14,
    "NEUTRO":        10,
    "RALLENTAMENTO":  8,
    "BEAR":           12,
}

# ── SCENARI OPZIONALI (Opzione C) ────────────────────────────
# automatici = trigger automatici se certi indicatori superano soglie
# peso = quanto modificano le probabilità Markov (0.0-1.0)
# attivo = può essere forzato manualmente in overrides.json

SCENARI_OPZIONALI_DEFAULT = {
    "recessione_tecnica":   {"attivo": False, "peso": 0.0, "auto_trigger": "yield_curve < -0.5"},
    "crisi_credito":        {"attivo": False, "peso": 0.0, "auto_trigger": "vix > 35"},
    "shock_geopolitico":    {"attivo": False, "peso": 0.0, "auto_trigger": "crude_chg > 20"},
    "hard_landing":         {"attivo": False, "peso": 0.0, "auto_trigger": "vix > 40"},
    "soft_landing":         {"attivo": False, "peso": 0.0, "auto_trigger": None},
    "stagflazione":         {"attivo": False, "peso": 0.0, "auto_trigger": "cpi > 5"},
    "pivot_fed":            {"attivo": False, "peso": 0.0, "auto_trigger": None},
    "crisi_bancaria":       {"attivo": False, "peso": 0.0, "auto_trigger": "vix > 45"},
    "boom_ai":              {"attivo": False, "peso": 0.0, "auto_trigger": None},
    "crisi_debito_sovrano": {"attivo": False, "peso": 0.0, "auto_trigger": "btp_bund_diff < -3"},
}

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


# ══════════════════════════════════════════════════════════════
#  FETCH
# ══════════════════════════════════════════════════════════════

def fetch_indicator(key, info):
    try:
        tk   = yf.Ticker(info["ticker"])
        hist = tk.history(period="3mo")
        if hist.empty or len(hist) < 5:
            return {"ok": False, "error": "No data"}
        closes = [float(v) for v in hist["Close"].values
                  if v is not None and not (isinstance(v, float) and np.isnan(v))]
        if len(closes) < 5:
            return {"ok": False, "error": "Dati insufficienti"}
        price      = closes[-1]
        prev_week  = closes[-6]  if len(closes) >= 6  else closes[0]
        prev_month = closes[-22] if len(closes) >= 22 else closes[0]
        chg_week   = (price - prev_week)  / prev_week  * 100 if prev_week  else 0
        chg_month  = (price - prev_month) / prev_month * 100 if prev_month else 0
        sma20      = float(np.mean(closes[-20:])) if len(closes) >= 20 else None
        if len(closes) >= 20:
            returns = [closes[i]/closes[i-1]-1 for i in range(max(1, len(closes)-20), len(closes))]
            vol20   = float(np.std(returns) * np.sqrt(252) * 100)
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


# ══════════════════════════════════════════════════════════════
#  REGIME (invariato da v2.0)
# ══════════════════════════════════════════════════════════════

def calc_regime_usa(r):
    score = 0; signals = []
    vix       = r.get("VIX",   {}).get("price",    20)
    us10y     = r.get("US10Y", {}).get("price",   4.0)
    us2y      = r.get("US2Y",  {}).get("price",   4.5)
    sp500_chg = r.get("SP500", {}).get("chg_month",  0)
    crude_chg = r.get("CRUDE", {}).get("chg_month",  0)
    if vix < 15:   score += 2; signals.append(f"VIX {vix:.1f} — bassa volatilità (rialzista)")
    elif vix < 20: score += 1; signals.append(f"VIX {vix:.1f} — volatilità normale")
    elif vix < 30: score -= 1; signals.append(f"VIX {vix:.1f} — volatilità elevata (attenzione)")
    else:          score -= 2; signals.append(f"VIX {vix:.1f} — paura elevata (ribassista)")
    spread = us10y - us2y
    if spread > 0.5:  score += 1; signals.append(f"Curva USA normale +{spread:.2f}% (espansione)")
    elif spread > 0:              signals.append(f"Curva USA piatta +{spread:.2f}% (rallentamento)")
    else:             score -= 1; signals.append(f"Curva USA invertita {spread:.2f}% (rischio recessione)")
    if sp500_chg > 3:   score += 1; signals.append(f"S&P500 +{sp500_chg:.1f}% mensile (momentum rialzista)")
    elif sp500_chg < -5:score -= 1; signals.append(f"S&P500 {sp500_chg:.1f}% mensile (pressione ribassista)")
    if crude_chg > 10:  signals.append(f"WTI +{crude_chg:.1f}% — pressione inflattiva USA")
    elif crude_chg < -10:signals.append(f"WTI {crude_chg:.1f}% — rallentamento domanda")
    return {"score": score, "signals": signals, "vix": vix, "yield_curve": spread}

def calc_regime_eu(r):
    score = 0; signals = []
    stoxx_chg = r.get("STOXX", {}).get("chg_month", 0)
    dax_chg   = r.get("DAX",   {}).get("chg_month", 0)
    cac_chg   = r.get("CAC",   {}).get("chg_month", 0)
    ftse_chg  = r.get("FTSE",  {}).get("chg_month", 0)
    eurusd    = r.get("EURUSD",{}).get("price",   1.08)
    btp_chg   = r.get("BTP10", {}).get("chg_month", 0)
    bund_chg  = r.get("BUND10",{}).get("chg_month", 0)
    ihyg_chg  = r.get("IHYG",  {}).get("chg_month", 0)
    ieac_chg  = r.get("IEAC",  {}).get("chg_month", 0)
    eq_chg = (stoxx_chg + dax_chg + cac_chg + ftse_chg) / 4
    if eq_chg > 3:   score += 1; signals.append(f"Azionario EU +{eq_chg:.1f}% (momentum positivo)")
    elif eq_chg < -4:score -= 1; signals.append(f"Azionario EU {eq_chg:.1f}% (pressione ribassista)")
    btp_bund_diff = btp_chg - bund_chg
    if btp_bund_diff < -2:   score -= 2; signals.append(f"⚠️ STRESS PERIFERICO — spread in allargamento ({btp_bund_diff:.1f}%)")
    elif btp_bund_diff < -1: score -= 1; signals.append(f"Spread BTP-Bund in allargamento ({btp_bund_diff:.1f}%)")
    elif btp_bund_diff > 1:  score += 1; signals.append(f"Spread BTP-Bund in compressione ({btp_bund_diff:.1f}%)")
    else:                                signals.append(f"Spread BTP-Bund stabile ({btp_bund_diff:.1f}%)")
    if ihyg_chg < -3:                   score -= 1; signals.append(f"High Yield EU {ihyg_chg:.1f}% — stress credito")
    if ieac_chg < -2:                   score -= 1; signals.append(f"Corp Bond EU {ieac_chg:.1f}% — allargamento spread IG")
    if ihyg_chg > 1 and ieac_chg > 0.5:score += 1; signals.append(f"Credito EU positivo (HY +{ihyg_chg:.1f}%)")
    if eurusd < 1.02:   score -= 1; signals.append(f"EUR/USD {eurusd:.4f} — euro debole")
    elif eurusd > 1.12:             signals.append(f"EUR/USD {eurusd:.4f} — euro forte")
    return {"score": score, "signals": signals, "btp_bund_diff": btp_bund_diff}

def calc_regime_asia(r):
    score = 0; signals = []
    nikkei_chg = r.get("NIKKEI", {}).get("chg_month", 0)
    hsi_chg    = r.get("HSI",    {}).get("chg_month", 0)
    copper_chg = r.get("COPPER", {}).get("chg_month", 0)
    if nikkei_chg > 3:   score += 1; signals.append(f"Nikkei +{nikkei_chg:.1f}% (momentum Asia positivo)")
    elif nikkei_chg < -5:score -= 1; signals.append(f"Nikkei {nikkei_chg:.1f}% (debolezza Giappone)")
    if hsi_chg > 3:   score += 1; signals.append(f"Hang Seng +{hsi_chg:.1f}% (ripresa China/HK)")
    elif hsi_chg < -5:score -= 1; signals.append(f"Hang Seng {hsi_chg:.1f}% (debolezza China/HK)")
    if copper_chg > 5:   score += 1; signals.append(f"Rame +{copper_chg:.1f}% — domanda globale in crescita")
    elif copper_chg < -5:score -= 1; signals.append(f"Rame {copper_chg:.1f}% — segnale rallentamento globale")
    return {"score": score, "signals": signals, "hsi_chg": hsi_chg}

def calc_dynamic_weights(r):
    w_usa, w_eu, w_asia = 0.40, 0.35, 0.25
    vix      = r.get("VIX",   {}).get("price",    20)
    btp_chg  = r.get("BTP10", {}).get("chg_month",  0)
    bund_chg = r.get("BUND10",{}).get("chg_month",  0)
    hsi_chg  = r.get("HSI",   {}).get("chg_month",  0)
    btp_bund_diff = btp_chg - bund_chg
    if vix > 35:              w_usa, w_eu, w_asia = 0.55, 0.28, 0.17
    elif vix > 30:            w_usa, w_eu, w_asia = 0.50, 0.30, 0.20
    elif btp_bund_diff < -2:  w_usa, w_eu, w_asia = 0.35, 0.45, 0.20
    elif hsi_chg < -10:       w_usa, w_eu, w_asia = 0.38, 0.32, 0.30
    return {"usa": w_usa, "eu": w_eu, "asia": w_asia}

def calc_global_regime(usa, eu, asia, weights):
    weighted_score = round(
        usa["score"]  * weights["usa"] +
        eu["score"]   * weights["eu"]  +
        asia["score"] * weights["asia"], 2)
    if weighted_score >= 2.0:    regime = "BULL"
    elif weighted_score >= 0.8:  regime = "ESPANSIONE"
    elif weighted_score >= -0.8: regime = "NEUTRO"
    elif weighted_score >= -1.5: regime = "RALLENTAMENTO"
    else:                        regime = "BEAR"
    all_signals = usa["signals"] + eu["signals"] + asia["signals"]
    return {
        "regime":      regime,
        "score":       weighted_score,
        "score_usa":   usa["score"],
        "score_eu":    eu["score"],
        "score_asia":  asia["score"],
        "weights":     weights,
        "signals":     all_signals[:8],
    }


# ══════════════════════════════════════════════════════════════
#  SCENARI OPZIONALI — TRIGGER AUTOMATICI (Opzione C)
# ══════════════════════════════════════════════════════════════

def eval_auto_triggers(r, reg_usa, reg_eu):
    """
    Attiva automaticamente scenari opzionali se superano soglie.
    Restituisce dict {scenario: {"attivo": True, "peso": float, "trigger_reason": str}}
    """
    vix        = r.get("VIX",   {}).get("price",    20)
    crude_chg  = r.get("CRUDE", {}).get("chg_month",  0)
    yc         = reg_usa.get("yield_curve", 0)
    btp_bund   = reg_eu.get("btp_bund_diff", 0)

    activated = {}

    # VIX > 30 → crisi credito
    if vix > 30:
        peso = min(0.8, (vix - 30) / 20)
        activated["crisi_credito"] = {
            "attivo": True, "peso": round(peso, 2),
            "trigger_reason": f"VIX {vix:.1f} > 30"
        }

    # VIX > 35 → hard landing
    if vix > 35:
        peso = min(0.9, (vix - 35) / 15)
        activated["hard_landing"] = {
            "attivo": True, "peso": round(peso, 2),
            "trigger_reason": f"VIX {vix:.1f} > 35"
        }

    # Yield curve invertita < -0.5
    if yc < -0.5:
        peso = min(0.7, abs(yc) / 2)
        activated["recessione_tecnica"] = {
            "attivo": True, "peso": round(peso, 2),
            "trigger_reason": f"Yield curve {yc:.2f}% < -0.5%"
        }

    # Petrolio +20% in un mese
    if crude_chg > 20:
        peso = min(0.6, crude_chg / 50)
        activated["shock_geopolitico"] = {
            "attivo": True, "peso": round(peso, 2),
            "trigger_reason": f"WTI +{crude_chg:.1f}% mensile"
        }

    # BTP-Bund stress
    if btp_bund < -3:
        peso = min(0.8, abs(btp_bund) / 5)
        activated["crisi_debito_sovrano"] = {
            "attivo": True, "peso": round(peso, 2),
            "trigger_reason": f"BTP-Bund diff {btp_bund:.1f}%"
        }

    return activated


# ══════════════════════════════════════════════════════════════
#  REGIME HISTORY — accumulo settimanale
# ══════════════════════════════════════════════════════════════

def update_regime_history(existing_history, regime, score, today_str):
    """
    Aggiunge il regime della settimana corrente allo storico.
    Un record per settimana (lunedì) — sovrascrive se stesso giorno.
    """
    # Trova il lunedì della settimana corrente
    today = datetime.datetime.strptime(today_str, "%Y-%m-%d").date()
    monday = today - datetime.timedelta(days=today.weekday())
    monday_str = monday.isoformat()

    new_record = {
        "date":   monday_str,
        "regime": regime,
        "score":  score,
    }

    # Rimuovi eventuale record della stessa settimana
    history = [r for r in existing_history if r.get("date") != monday_str]
    history.append(new_record)

    # Ordina per data e tronca
    history.sort(key=lambda x: x["date"])
    history = history[-MAX_HISTORY_WEEKS:]

    return history


# ══════════════════════════════════════════════════════════════
#  DURATA REGIME
# ══════════════════════════════════════════════════════════════

def calc_regime_duration(history, current_regime):
    """
    Calcola:
    - settimane consecutive nel regime corrente
    - durata media storica per ogni regime
    - % completamento rispetto alla media
    - aspettativa settimane rimanenti
    """
    if not history:
        return {
            "settimane_correnti":    1,
            "media_storica":         DURATE_DEFAULT.get(current_regime, 12),
            "percentuale_durata":    8,
            "aspettativa_rimanente": DURATE_DEFAULT.get(current_regime, 12) - 1,
            "calibrato_su_dati":     False,
        }

    # Settimane consecutive nel regime corrente
    settimane_correnti = 0
    for record in reversed(history):
        if record["regime"] == current_regime:
            settimane_correnti += 1
        else:
            break

    # Calcola durate storiche per ogni regime
    durate_storiche = {r: [] for r in REGIMES}
    if len(history) > 1:
        regime_corrente_scan = history[0]["regime"]
        durata_corrente_scan = 1
        for i in range(1, len(history)):
            if history[i]["regime"] == regime_corrente_scan:
                durata_corrente_scan += 1
            else:
                durate_storiche[regime_corrente_scan].append(durata_corrente_scan)
                regime_corrente_scan  = history[i]["regime"]
                durata_corrente_scan  = 1
        # Ultimo episodio (in corso — non lo contiamo come completo)

    # Media storica calibrata sui dati o fallback al default
    medie = {}
    for r in REGIMES:
        if len(durate_storiche[r]) >= 2:
            medie[r] = round(float(np.mean(durate_storiche[r])), 1)
        else:
            medie[r] = DURATE_DEFAULT.get(r, 12)

    media = medie.get(current_regime, 12)
    pct   = round(min(99, settimane_correnti / media * 100), 1)
    rimanenti = max(0, round(media - settimane_correnti))

    return {
        "settimane_correnti":    settimane_correnti,
        "media_storica":         media,
        "medie_per_regime":      medie,
        "percentuale_durata":    pct,
        "aspettativa_rimanente": rimanenti,
        "calibrato_su_dati":     len(history) >= 20,
        "episodi_storici":       {r: len(v) for r, v in durate_storiche.items()},
    }


# ══════════════════════════════════════════════════════════════
#  MARKOV — FORECAST LUNGO TERMINE
# ══════════════════════════════════════════════════════════════

def calc_markov_matrix(history):
    """
    Calcola matrice di transizione Markov da storico settimanale.
    Ritorna dict {from_regime: {to_regime: probability}}
    """
    counts = {r: {r2: 0 for r2 in REGIMES} for r in REGIMES}
    totals = {r: 0 for r in REGIMES}

    for i in range(len(history) - 1):
        fr = history[i]["regime"]
        to = history[i+1]["regime"]
        if fr in counts and to in counts:
            counts[fr][to] += 1
            totals[fr] += 1

    matrix = {}
    for fr in REGIMES:
        if totals[fr] > 0:
            matrix[fr] = {to: round(counts[fr][to] / totals[fr], 4) for to in REGIMES}
        else:
            # Fallback uniforme se non ci sono dati
            matrix[fr] = {to: 1/len(REGIMES) for to in REGIMES}

    return matrix


def markov_step(distribution, matrix):
    """Applica un passo della catena di Markov."""
    new_dist = {r: 0.0 for r in REGIMES}
    for from_r, prob in distribution.items():
        if from_r in matrix:
            for to_r, trans_prob in matrix[from_r].items():
                new_dist[to_r] += prob * trans_prob
    return new_dist


def duration_adjustment(distribution, current_regime, duration_info, weeks_ahead):
    """
    Aggiusta le probabilità in base alla durata del regime corrente.
    Se sei al 90% della durata media storica, abbassa la probabilità
    di restare nel regime corrente e alza quella di transizione.
    """
    pct_durata = duration_info.get("percentuale_durata", 50)
    media      = duration_info.get("media_storica", 12)

    if media == 0 or pct_durata == 0:
        return distribution

    # Probabilità di "sopravvivenza" del regime nelle prossime settimane
    # Modello semplice: decrescente esponenzialmente dopo la media storica
    settimane_correnti = duration_info.get("settimane_correnti", 1)
    settimane_totali   = settimane_correnti + weeks_ahead

    # Probabilità che il regime duri ancora weeks_ahead settimane
    # Basata su distribuzione geometrica (tasso di uscita = 1/media)
    tasso_uscita  = 1.0 / max(media, 1)
    prob_survival = (1 - tasso_uscita) ** weeks_ahead

    # Se siamo già oltre la media storica, penalizziamo ulteriormente
    if settimane_correnti > media:
        overstay_factor = settimane_correnti / media
        prob_survival  *= max(0.1, 1 / overstay_factor)

    prob_survival = max(0.05, min(0.95, prob_survival))

    # Aggiusta la distribuzione
    current_prob = distribution.get(current_regime, 0)
    adjusted_current = current_prob * prob_survival

    # Redistribuisce la differenza agli altri regimi proporzionalmente
    diff = current_prob - adjusted_current
    new_dist = {}
    other_sum = sum(v for r, v in distribution.items() if r != current_regime)
    for r, prob in distribution.items():
        if r == current_regime:
            new_dist[r] = adjusted_current
        else:
            if other_sum > 0:
                new_dist[r] = prob + diff * (prob / other_sum)
            else:
                new_dist[r] = prob + diff / (len(REGIMES) - 1)

    return new_dist


def calc_forecast(history, current_regime, duration_info, scenari_attivi):
    """
    Calcola forecast probabilistico a 4w / 3m / 6m / 12m.
    Usa Duration-Adjusted Markov Chain.
    Incorpora scenari opzionali attivi.
    """
    if len(history) < 10:
        # Dataset insufficiente — usa probabilità basate sulla durata
        media = duration_info.get("media_storica", 12)
        pct   = duration_info.get("percentuale_durata", 50)
        prob_continua = max(20, min(80, 100 - pct))
        prob_altro    = round((100 - prob_continua) / (len(REGIMES) - 1))
        dist_base = {r: prob_altro for r in REGIMES}
        dist_base[current_regime] = prob_continua
        return {
            "4w":  _to_pct_dict(dist_base),
            "3m":  _to_pct_dict(dist_base),
            "6m":  _to_pct_dict(dist_base),
            "12m": _to_pct_dict(dist_base),
            "note": "Dataset < 10 settimane — stima basata su durata storica",
            "matrice_disponibile": False,
        }

    matrix = calc_markov_matrix(history)

    # Distribuzione iniziale: 100% nel regime corrente
    dist = {r: 0.0 for r in REGIMES}
    dist[current_regime] = 1.0

    # Orizzonti in settimane
    horizons = {"4w": 4, "3m": 13, "6m": 26, "12m": 52}
    results  = {}
    dist_step = dict(dist)

    # Applica Markov settimana per settimana fino al massimo orizzonte
    max_weeks = max(horizons.values())
    snapshots = {}

    for week in range(1, max_weeks + 1):
        # Passo Markov
        dist_step = markov_step(dist_step, matrix)
        # Duration adjustment
        dist_step = duration_adjustment(dist_step, current_regime, duration_info, week)
        # Salva snapshot agli orizzonti desiderati
        for label, w in horizons.items():
            if week == w:
                snapshots[label] = dict(dist_step)

    # Applica modificatori scenari opzionali attivi
    for label in horizons:
        d = snapshots.get(label, dist)
        d = apply_scenario_adjustments(d, scenari_attivi, horizons[label])
        results[label] = _to_pct_dict(d)

    return {
        **results,
        "note": f"Markov su {len(history)} settimane · Duration-Adjusted",
        "matrice_disponibile": True,
        "matrice": {
            fr: {to: round(matrix[fr][to]*100) for to in REGIMES}
            for fr in REGIMES
        },
    }


def apply_scenario_adjustments(distribution, scenari_attivi, weeks_ahead):
    """
    Modifica la distribuzione in base agli scenari opzionali attivi.
    scenari_attivi: dict {nome: {attivo, peso, trigger_reason}}
    """
    if not scenari_attivi:
        return distribution

    # Mappa scenario → impatto sul regime
    SCENARIO_IMPACT = {
        "recessione_tecnica":   {"BEAR": +0.3,  "RALLENTAMENTO": +0.2, "BULL": -0.25},
        "crisi_credito":        {"BEAR": +0.35, "RALLENTAMENTO": +0.15, "BULL": -0.3},
        "shock_geopolitico":    {"NEUTRO": +0.2, "RALLENTAMENTO": +0.15, "BULL": -0.2},
        "hard_landing":         {"BEAR": +0.45, "RALLENTAMENTO": +0.2,  "BULL": -0.4},
        "soft_landing":         {"BULL": +0.2,  "ESPANSIONE": +0.15,   "BEAR": -0.15},
        "stagflazione":         {"RALLENTAMENTO": +0.25, "NEUTRO": +0.1, "BULL": -0.2},
        "pivot_fed":            {"BULL": +0.25, "ESPANSIONE": +0.15,   "BEAR": -0.2},
        "crisi_bancaria":       {"BEAR": +0.4,  "RALLENTAMENTO": +0.2, "BULL": -0.35},
        "boom_ai":              {"BULL": +0.2,  "ESPANSIONE": +0.1,    "BEAR": -0.1},
        "crisi_debito_sovrano": {"BEAR": +0.3,  "RALLENTAMENTO": +0.2, "BULL": -0.25},
    }

    adjusted = dict(distribution)

    for nome, info in scenari_attivi.items():
        if not info.get("attivo"):
            continue
        peso = info.get("peso", 0.5)
        impact = SCENARIO_IMPACT.get(nome, {})

        # Scala l'impatto in base all'orizzonte
        # Scenari a breve termine impattano di più nel breve
        scale = max(0.3, 1 - weeks_ahead / 60)
        if weeks_ahead > 26:  # oltre 6 mesi lo scenario si attenua
            scale *= 0.5

        for regime, delta in impact.items():
            if regime in adjusted:
                adjusted[regime] = max(0, adjusted[regime] + delta * peso * scale)

    # Rinormalizza a 1.0
    total = sum(adjusted.values())
    if total > 0:
        adjusted = {r: v/total for r, v in adjusted.items()}

    return adjusted


def _to_pct_dict(distribution):
    """Converte distribuzione 0-1 in percentuali intere."""
    pct = {r: round(v * 100) for r, v in distribution.items()}
    # Corregge arrotondamento a 100
    diff = 100 - sum(pct.values())
    if diff != 0:
        top = max(pct, key=pct.get)
        pct[top] += diff
    return pct


# ══════════════════════════════════════════════════════════════
#  SCENARI (invariato da v2.0)
# ══════════════════════════════════════════════════════════════

def calc_scenario_probabilities(r, regime_data):
    vix        = r.get("VIX",    {}).get("price",     20)
    us10y      = r.get("US10Y",  {}).get("price",    4.0)
    us2y       = r.get("US2Y",   {}).get("price",    4.5)
    crude      = r.get("CRUDE",  {}).get("price",     80)
    crude_chg  = r.get("CRUDE",  {}).get("chg_month",  0)
    gold_chg   = r.get("GOLD",   {}).get("chg_month",  0)
    sp500_chg  = r.get("SP500",  {}).get("chg_month",  0)
    eurusd     = r.get("EURUSD", {}).get("price",   1.08)
    copper_chg = r.get("COPPER", {}).get("chg_month",  0)
    nikkei_chg = r.get("NIKKEI", {}).get("chg_month",  0)
    hsi_chg    = r.get("HSI",    {}).get("chg_month",  0)
    ihyg_chg   = r.get("IHYG",   {}).get("chg_month",  0)
    btp_chg    = r.get("BTP10",  {}).get("chg_month",  0)
    bund_chg   = r.get("BUND10", {}).get("chg_month",  0)
    btp_bund   = btp_chg - bund_chg
    curve_spread = us10y - us2y
    probs = {}
    probs["petrolio_alto"]        = min(90, max(10, 35 + crude_chg * 1.5))
    probs["guerra_commerciale"]   = max(20, min(70, 40 + (copper_chg < -5) * 10))
    probs["crisi_taiwan"]         = max(10, min(40, 15 + (hsi_chg < -8) * 10))
    probs["hormuz"]               = max(5,  min(50, 10 + crude_chg * 0.8))
    probs["recessione_germania"]  = max(20, min(75, 45 - curve_spread * 8 + (r.get("DAX",{}).get("chg_month",0) < -5) * 10))
    probs["crisi_btp"]            = max(5,  min(70, 15 + (vix - 20) * 1.5 + max(0, -btp_bund) * 5))
    probs["bce_alza"]             = max(5,  min(70, 20 + crude_chg * 0.8))
    probs["deflazione_eu"]        = max(3,  min(30, 15 - crude_chg * 0.5))
    probs["stress_periferico"]    = max(5,  min(80, 20 + max(0, -btp_bund) * 15 + (ihyg_chg < -3) * 15))
    probs["hard_landing_usa"]     = max(10, min(70, 25 + (curve_spread < 0) * 20 + (vix > 25) * 10))
    probs["soft_landing"]         = max(15, min(75, 55 - (vix - 20) * 2))
    probs["stagflazione_usa"]     = max(5,  min(55, 20 + crude_chg * 0.6))
    probs["boom_ai"]              = max(30, min(80, 65 + sp500_chg * 0.5))
    probs["flash_crash"]          = max(5,  min(45, 10 + (vix > 25) * 15 + (vix > 35) * 15))
    probs["bear_market"]          = max(5,  min(55, 15 + (curve_spread < 0) * 15 + (vix > 30) * 10 + (ihyg_chg < -5) * 10))
    probs["bull_estremo"]         = max(5,  min(50, 20 + sp500_chg * 1.2 + (vix < 15) * 10))
    probs["inversione_curva"]     = max(5,  min(70, 30 + (curve_spread < 0) * 30))
    probs["crisi_credito_eu"]     = max(5,  min(60, 15 + max(0, -ihyg_chg) * 5 + max(0, -btp_bund) * 8))
    probs["rallentamento_cina"]   = max(10, min(70, 30 + max(0, -hsi_chg) * 2 + max(0, -copper_chg) * 2))
    probs["crisi_giappone"]       = max(5,  min(40, 10 + max(0, -nikkei_chg) * 1.5))
    probs["boom_asia_emergenti"]  = max(15, min(65, 35 + (hsi_chg > 5) * 10 + (copper_chg > 5) * 10))
    probs["superciclo_commodity"] = max(20, min(70, 35 + crude_chg * 0.8 + gold_chg * 0.5 + copper_chg * 0.5))
    probs["dollaro_debole"]       = max(15, min(65, 40 - (eurusd - 1.08) * 100))
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
    probs["small_cap_outperform"] = 40
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


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    os.makedirs("data", exist_ok=True)
    now_str  = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    today_str= datetime.datetime.now().strftime('%Y-%m-%d')
    print(f"🌍 RSE Market Fetcher v3.0 — {now_str}")
    print(f"   Indicatori: {len(INDICATORS)} | Aree: USA · Europa · Asia")
    print(f"   Novità v3.0: Regime History · Markov Lungo · Duration Analysis")

    # ── FETCH ────────────────────────────────────────────────
    results = {}; ok_count = 0
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
    current_regime = regime["regime"]

    print(f"\n  Pesi: USA {weights['usa']*100:.0f}% · EU {weights['eu']*100:.0f}% · Asia {weights['asia']*100:.0f}%")
    print(f"  Score: {regime['score']:+.2f} → Regime: {current_regime}")

    # ── CARICA STORICO PRECEDENTE ─────────────────────────────
    existing = {}
    data_file = "data/market_data_rse.json"
    if os.path.exists(data_file):
        try:
            with open(data_file) as f:
                existing = json.load(f)
        except:
            existing = {}

    existing_history = existing.get("regime_history", [])
    print(f"  Storico precedente: {len(existing_history)} settimane")

    # ── AGGIORNA REGIME HISTORY ───────────────────────────────
    regime_history = update_regime_history(
        existing_history, current_regime, regime["score"], today_str)
    print(f"  Storico aggiornato: {len(regime_history)} settimane")

    # ── DURATA REGIME ─────────────────────────────────────────
    duration_info = calc_regime_duration(regime_history, current_regime)
    print(f"  Durata corrente: {duration_info['settimane_correnti']} settimane")
    print(f"  Media storica {current_regime}: {duration_info['media_storica']} settimane")
    print(f"  Completamento: {duration_info['percentuale_durata']}%")

    # ── SCENARI OPZIONALI — trigger automatici ────────────────
    scenari_auto = eval_auto_triggers(results, reg_usa, reg_eu)

    # Merge con overrides manuali
    overrides_scenari = {}
    try:
        with open("data/scenari_opzionali.json") as f:
            overrides_scenari = json.load(f)
    except:
        pass

    # Gli override manuali hanno precedenza sugli automatici
    scenari_attivi = {**scenari_auto}
    for nome, cfg in overrides_scenari.items():
        # Salta chiavi speciali che iniziano con _ (commenti/metadati)
        if nome.startswith('_') or not isinstance(cfg, dict):
            continue
        if cfg.get("attivo"):
            scenari_attivi[nome] = {
                "attivo": True,
                "peso":   cfg.get("peso", 0.5),
                "trigger_reason": "override manuale"
            }
        elif nome in scenari_attivi:
            # Override manuale disattiva il trigger automatico
            del scenari_attivi[nome]

    if scenari_attivi:
        print(f"  Scenari opzionali attivi: {', '.join(scenari_attivi.keys())}")
    else:
        print(f"  Scenari opzionali: nessuno attivo")

    # ── FORECAST MARKOV LUNGO ─────────────────────────────────
    print(f"\n  Calcolo forecast Markov...")
    forecast = calc_forecast(regime_history, current_regime, duration_info, scenari_attivi)
    print(f"  4w:  {forecast['4w']}")
    print(f"  3m:  {forecast['3m']}")
    print(f"  6m:  {forecast['6m']}")
    print(f"  12m: {forecast['12m']}")

    # ── SCENARI PROBABILITA ───────────────────────────────────
    probs = calc_scenario_probabilities(results, regime)

    # Merge consensus
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
        final_probs[k] = round((final_probs.get(k, v) + v) / 2)
    for k, v in overrides.items():
        final_probs[k] = v

    # ── OUTPUT ───────────────────────────────────────────────
    output = {
        "meta": {
            "updated":           datetime.datetime.utcnow().isoformat() + "Z",
            "updated_it":        datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
            "regime":            current_regime,
            "regime_score":      regime["score"],
            "regime_score_usa":  reg_usa["score"],
            "regime_score_eu":   reg_eu["score"],
            "regime_score_asia": reg_asia["score"],
            "regime_weights":    weights,
            "regime_signals":    regime["signals"],
            "btp_bund_diff":     reg_eu.get("btp_bund_diff", 0),
            "fetch_ok":          ok_count,
            "fetch_total":       len(INDICATORS),
        },
        # Regime corrente + durata
        "regime_duration":   duration_info,
        # Forecast Markov lungo termine
        "forecast":          forecast,
        # Scenari opzionali attivi
        "scenari_opzionali": {
            "attivi": scenari_attivi,
            "template": SCENARI_OPZIONALI_DEFAULT,
        },
        # Storico settimanale (max 3 anni)
        "regime_history":    regime_history,
        # Dati mercato
        "indicators":        results,
        # Probabilità scenari
        "probabilities":     final_probs,
        # Consensus
        "consensus_date":    consensus.get("date"),
        "consensus_sources": consensus.get("sources", []),
    }

    with open(data_file, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Salvato {data_file}")
    print(f"   Regime: {current_regime} (score: {regime['score']:+.2f})")
    print(f"   Durata: {duration_info['settimane_correnti']}w / {duration_info['media_storica']}w media ({duration_info['percentuale_durata']}%)")
    print(f"   Storico: {len(regime_history)} settimane")
    print(f"   Forecast 6m: {forecast['6m']}")
    print(f"   Scenari: {len(final_probs)}")


if __name__ == "__main__":
    main()
