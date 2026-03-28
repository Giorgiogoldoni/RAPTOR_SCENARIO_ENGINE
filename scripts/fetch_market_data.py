#!/usr/bin/env python3
"""
RWM DataBot — Fetch Market Data
Scarica prezzi, SMA50, SMA200, momentum per ETF RWM + Ultra
Gira 2x/giorno via GitHub Action
"""
import json, os, datetime
import yfinance as yf
import numpy as np

# ── UNIVERSO COMPLETO ──────────────────────────────────────────
TICKERS = {
  # RWM CLASSICO — 41 ETF
  "SWDA":  {"n":"iShares Core MSCI World",           "cl":"eq",     "cat":"rwm"},
  "VWCE":  {"n":"Vanguard FTSE All-World",            "cl":"eq",     "cat":"rwm"},
  "IQQH":  {"n":"iShares MSCI World Quality",         "cl":"eq",     "cat":"rwm"},
  "IWMO":  {"n":"iShares MSCI World Momentum",        "cl":"eq",     "cat":"rwm"},
  "ZPRV":  {"n":"SPDR MSCI USA Small Cap Value",      "cl":"small",  "cat":"rwm"},
  "QDVE":  {"n":"iShares S&P 500 IT Sector",          "cl":"eq",     "cat":"rwm"},
  "IUSN":  {"n":"iShares MSCI World Small Cap",       "cl":"small",  "cat":"rwm"},
  "IESE":  {"n":"iShares MSCI Europe Small Cap",      "cl":"small",  "cat":"rwm"},
  "IEMA":  {"n":"iShares Core MSCI EM IMI",           "cl":"eqem",   "cat":"rwm"},
  "VFEM":  {"n":"Vanguard FTSE Emerging Markets",     "cl":"eqem",   "cat":"rwm"},
  "CEBL":  {"n":"iShares MSCI EM Asia",               "cl":"eqem",   "cat":"rwm"},
  "EXCS":  {"n":"iShares MSCI EM ex-China",           "cl":"eqem",   "cat":"rwm"},
  "ISEU":  {"n":"iShares STOXX EU Select Div 30",     "cl":"income", "cat":"rwm"},
  "ISUS":  {"n":"iShares DJ US Select Dividend",      "cl":"income", "cat":"rwm"},
  "ISAP":  {"n":"iShares DJ Asia Pac Select Div",     "cl":"income", "cat":"rwm"},
  "VHYL":  {"n":"Vanguard FTSE All-World High Div",   "cl":"income", "cat":"rwm"},
  "AERO":  {"n":"iShares Aerospace & Defence",        "cl":"tema",   "cat":"rwm"},
  "HLTH":  {"n":"iShares MSCI World Health Care",     "cl":"tema",   "cat":"rwm"},
  "BTEC":  {"n":"iShares NASDAQ US Biotech",          "cl":"tema",   "cat":"rwm"},
  "BNKS":  {"n":"iShares STOXX EU 600 Banks",         "cl":"tema",   "cat":"rwm"},
  "XUCS":  {"n":"Xtrackers USA Consumer Disc.",       "cl":"tema",   "cat":"rwm"},
  "JAPN":  {"n":"Amundi MSCI Japan EUR Hdg",          "cl":"tema",   "cat":"rwm"},
  "IWDP":  {"n":"iShares Dev. Markets Property",      "cl":"re",     "cat":"rwm"},
  "IPRP":  {"n":"iShares European Property Yield",    "cl":"re",     "cat":"rwm"},
  "IGLN":  {"n":"iShares Physical Gold ETC",          "cl":"com",    "cat":"rwm"},
  "ICOM":  {"n":"iShares Diversified Commodity",      "cl":"com",    "cat":"rwm"},
  "IEGE":  {"n":"iShares EUR Govt Bond 1-3yr",        "cl":"gov",    "cat":"rwm"},
  "IBGM":  {"n":"iShares EUR Govt Bond 3-7yr",        "cl":"gov",    "cat":"rwm"},
  "IBGL":  {"n":"iShares EUR Govt Bond 7-10yr",       "cl":"gov",    "cat":"rwm"},
  "IDTL":  {"n":"iShares EUR Govt Bond 20yr+",        "cl":"gov",    "cat":"rwm"},
  "VGEA":  {"n":"Vanguard Eurozone Govt Bond",        "cl":"gov",    "cat":"rwm"},
  "IEAC":  {"n":"iShares Core EUR Corp Bond",         "cl":"corp",   "cat":"rwm"},
  "VECP":  {"n":"Vanguard EUR Corporate Bond",        "cl":"corp",   "cat":"rwm"},
  "IHYG":  {"n":"iShares EUR High Yield Corp Bond",   "cl":"corp",   "cat":"rwm"},
  "LQDE":  {"n":"iShares USD Corp Bond EUR Hdg",      "cl":"corp",   "cat":"rwm"},
  "SE15":  {"n":"iShares EUR Corp Bond 0-3yr",        "cl":"corp",   "cat":"rwm"},
  "EMBE":  {"n":"iShares JPM EM Bond EUR Hdg",        "cl":"em",     "cat":"rwm"},
  "EMDV":  {"n":"iShares JPM EM Local Bond",          "cl":"em",     "cat":"rwm"},
  "IBCI":  {"n":"iShares EUR Inflation Linked",       "cl":"il",     "cat":"rwm"},
  "XEON":  {"n":"Xtrackers EUR Overnight Rate",       "cl":"mon",    "cat":"rwm"},
  "SMART": {"n":"iShares EUR Govt Bond 0-1yr",        "cl":"mon",    "cat":"rwm"},

  # RWM ULTRA — Equity concentrato
  "EQQQ":  {"n":"Invesco NASDAQ-100",                 "cl":"eq",     "cat":"ultra"},
  "ZPRX":  {"n":"SPDR MSCI Europe Small Cap Value",   "cl":"small",  "cat":"ultra"},
  "MXUS":  {"n":"Amundi MSCI USA",                    "cl":"eq",     "cat":"ultra"},

  # RWM ULTRA — EM aggressivo
  "CINDA": {"n":"iShares MSCI India",                 "cl":"eqem",   "cat":"ultra"},
  "HMCH":  {"n":"HSBC MSCI China",                    "cl":"eqem",   "cat":"ultra"},

  # RWM ULTRA — Leva 2x
  "LQQ":   {"n":"Amundi Leva 2x NASDAQ-100",          "cl":"leva2",  "cat":"ultra"},
  "L2US":  {"n":"Amundi Leva 2x S&P500",              "cl":"leva2",  "cat":"ultra"},

  # RWM ULTRA — Crypto ETP
  "HODL":  {"n":"21Shares Crypto Basket 10",          "cl":"crypto", "cat":"ultra"},
  "BTCE":  {"n":"ETC Group Bitcoin ETP",              "cl":"crypto", "cat":"ultra"},
  "BCHN":  {"n":"Invesco Global Blockchain ETF",      "cl":"crypto", "cat":"ultra"},
}

# Mappa ticker Yahoo Finance (suffisso borsa)
YF_MAP = {
  "SWDA":"SWDA.L",   "VWCE":"VWCE.DE",  "IQQH":"IQQH.DE",  "IWMO":"IWMO.DE",
  "ZPRV":"ZPRV.DE",  "QDVE":"QDVE.DE",  "IUSN":"IUSN.DE",  "IESE":"IESE.DE",
  "IEMA":"IEMA.MI",  "VFEM":"VFEM.MI",  "CEBL":"CEBL.MI",  "EXCS":"EXCS.MI",
  "ISEU":"ISEU.DE",  "ISUS":"ISUS.DE",  "ISAP":"ISAP.DE",  "VHYL":"VHYL.DE",
  "AERO":"AERO.MI",  "HLTH":"HLTH.MI",  "BTEC":"BTEC.MI",  "BNKS":"BNKS.DE",
  "XUCS":"XUCS.DE",  "JAPN":"JAPN.MI",  "IWDP":"IWDP.DE",  "IPRP":"IPRP.MI",
  "IGLN":"IGLN.MI",  "ICOM":"ICOM.MI",  "IEGE":"IEGE.DE",  "IBGM":"IBGM.DE",
  "IBGL":"IBGL.DE",  "IDTL":"IDTL.DE",  "VGEA":"VGEA.DE",  "IEAC":"IEAC.DE",
  "VECP":"VECP.DE",  "IHYG":"IHYG.DE",  "LQDE":"LQDE.DE",  "SE15":"SE15.MI",
  "EMBE":"EMBE.MI",  "EMDV":"EMDV.MI",  "IBCI":"IBCI.DE",  "XEON":"XEON.DE",
  "SMART":"SEGA.MI", "EQQQ":"EQQQ.DE",  "ZPRX":"ZPRX.DE",  "MXUS":"CU1.DE",
  "CINDA":"CINDA.MI","HMCH":"HMCH.MI",  "LQQ":"LQQ.PA",    "L2US":"L2US.PA",
  "HODL":"HODL.DE",  "BTCE":"BTCE.DE",  "BCHN":"BCHN.DE",
}

def calc_sma(prices, period):
    if len(prices) < period:
        return None
    return float(np.mean(prices[-period:]))

def calc_momentum(prices, days):
    if len(prices) < days + 1:
        return None
    return float((prices[-1] - prices[-days]) / prices[-days] * 100)

def calc_vol20(prices):
    if len(prices) < 21:
        return None
    returns = np.diff(prices[-21:]) / prices[-21:-1]
    return float(np.std(returns) * np.sqrt(252) * 100)

def calc_maxdd(prices):
    if len(prices) < 2:
        return 0
    peak = prices[0]
    max_dd = 0
    for p in prices:
        if p > peak:
            peak = p
        dd = (peak - p) / peak * 100
        if dd > max_dd:
            max_dd = dd
    return float(max_dd)

def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices[-(period+1):])
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains)
    avg_loss = np.mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - 100 / (1 + rs))

def fetch_ticker(rwm_ticker):
    yf_ticker = YF_MAP.get(rwm_ticker, rwm_ticker + ".DE")
    try:
        tk = yf.Ticker(yf_ticker)
        hist = tk.history(period="1y")
        if hist.empty or len(hist) < 20:
            return {"ok": False, "error": "No data"}

        closes = list(hist["Close"].values)
        price = float(closes[-1])
        sma50  = calc_sma(closes, 50)
        sma200 = calc_sma(closes, 200)
        mom1m  = calc_momentum(closes, 21)
        mom3m  = calc_momentum(closes, 63)
        mom1y  = calc_momentum(closes, 252) if len(closes) >= 253 else None
        vol20  = calc_vol20(closes)
        maxdd  = calc_maxdd(closes[-126:])  # 6 mesi
        rsi    = calc_rsi(closes)

        faber200 = price > sma200 if sma200 else None
        faber50  = price > sma50  if sma50  else None

        # Score TAA (0-4)
        score = 0
        if faber200: score += 1
        if mom3m and mom3m > 0: score += 1
        if rsi and 40 <= rsi <= 70: score += 1
        if vol20 and vol20 < 20: score += 1

        # Score Ultra TAA (0-2) — segnali veloci
        ultra_score = 0
        if faber50: ultra_score += 1
        if mom1m and mom1m > 0: ultra_score += 1

        return {
            "ok": True,
            "price": round(price, 4),
            "sma50":  round(sma50,  4) if sma50  else None,
            "sma200": round(sma200, 4) if sma200 else None,
            "faber50":  faber50,
            "faber200": faber200,
            "mom1m":  round(mom1m, 2) if mom1m else None,
            "mom3m":  round(mom3m, 2) if mom3m else None,
            "mom1y":  round(mom1y, 2) if mom1y else None,
            "vol20":  round(vol20, 2) if vol20 else None,
            "maxdd":  round(maxdd, 2),
            "rsi":    round(rsi,   2) if rsi   else None,
            "score":  score,
            "ultra_score": ultra_score,
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def main():
    os.makedirs("data", exist_ok=True)
    print(f"🚀 RWM DataBot — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"📊 Fetching {len(TICKERS)} tickers...")

    results = {}
    errors  = []

    for i, (rwm, meta) in enumerate(TICKERS.items()):
        print(f"  [{i+1:02d}/{len(TICKERS)}] {rwm:8s} ({YF_MAP.get(rwm, rwm+'.DE')})", end=" ")
        data = fetch_ticker(rwm)
        results[rwm] = {**meta, **data}
        if data["ok"]:
            print(f"✓ € {data['price']:.2f}  score:{data.get('score','?')}/4")
        else:
            print(f"✗ {data.get('error','?')}")
            errors.append(rwm)

    # Market summary
    ok_count    = sum(1 for v in results.values() if v.get("ok"))
    long_count  = sum(1 for v in results.values() if v.get("ok") and v.get("score",0) >= 3)
    flat_count  = sum(1 for v in results.values() if v.get("ok") and v.get("score",0) == 0)
    avg_score   = round(
        sum(v.get("score",0) for v in results.values() if v.get("ok")) / max(ok_count,1), 2
    )

    output = {
        "meta": {
            "updated":    datetime.datetime.utcnow().isoformat() + "Z",
            "updated_it": datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
            "total":      len(TICKERS),
            "ok":         ok_count,
            "errors":     errors,
            "avg_score":  avg_score,
            "long":       long_count,
            "flat":       flat_count,
            "regime":     "BULL" if avg_score >= 3 else "BEAR" if avg_score <= 1 else "NEUTRO",
        },
        "data": results,
    }

    path = "data/market_data.json"
    with open(path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Salvato {path}")
    print(f"   OK:{ok_count}/{len(TICKERS)} · Errori:{len(errors)} · Score medio:{avg_score} · Regime:{output['meta']['regime']}")
    if errors:
        print(f"   Errori: {', '.join(errors)}")

if __name__ == "__main__":
    main()
