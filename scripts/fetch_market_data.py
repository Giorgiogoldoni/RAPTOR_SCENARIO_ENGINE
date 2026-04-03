#!/usr/bin/env python3
"""
RWM Market Data Fetcher
Scarica indicatori macro principali e scrive data/market_data.json
Usato dal workflow fetch_data.yml
"""
import json, os, datetime
import yfinance as yf
import numpy as np

TICKERS = {
    "SP500":   {"ticker": "^GSPC",     "n": "S&P 500"},
    "STOXX":   {"ticker": "^STOXX50E", "n": "Euro Stoxx 50"},
    "FTSEMIB": {"ticker": "FTSEMIB.MI","n": "FTSE MIB"},
    "DAX":     {"ticker": "^GDAXI",    "n": "DAX"},
    "VIX":     {"ticker": "^VIX",      "n": "VIX"},
    "US10Y":   {"ticker": "^TNX",      "n": "US 10Y Yield"},
    "EURUSD":  {"ticker": "EURUSD=X",  "n": "EUR/USD"},
    "GOLD":    {"ticker": "GC=F",      "n": "Gold"},
    "CRUDE":   {"ticker": "CL=F",      "n": "WTI Crude"},
    "BITCOIN": {"ticker": "BTC-EUR",   "n": "Bitcoin EUR"},
}

def safe_float(v):
    try:
        f = float(v)
        return None if (f != f or abs(f) == float('inf')) else f
    except:
        return None

def fetch_one(key, info):
    try:
        tk = yf.Ticker(info["ticker"])
        hist = tk.history(period="1mo")
        if hist.empty or len(hist) < 2:
            return {"ok": False, "error": "No data"}
        closes = [safe_float(v) for v in hist["Close"].tolist()]
        closes = [v for v in closes if v is not None]
        if len(closes) < 2:
            return {"ok": False, "error": "Dati insufficienti"}
        price = closes[-1]
        prev  = closes[-6] if len(closes) >= 6 else closes[0]
        chg   = (price - prev) / prev * 100 if prev else 0
        return {
            "ok": True,
            "price": round(price, 4),
            "chg_week": round(chg, 2),
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:60]}

def main():
    os.makedirs("data", exist_ok=True)
    print(f"[RWM DataFetch] Start — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

    results = {}
    for key, info in TICKERS.items():
        print(f"  {key:10s}", end=" ")
        data = fetch_one(key, info)
        results[key] = {**info, **data}
        if data["ok"]:
            print(f"✓  {data['price']:.4f}  Δ1w: {data.get('chg_week',0):+.1f}%")
        else:
            print(f"✗  {data.get('error','?')}")

    output = {
        "meta": {
            "updated":    datetime.datetime.utcnow().isoformat() + "Z",
            "updated_it": datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
            "source":     "Yahoo Finance",
        },
        "data": results,
    }

    with open("data/market_data.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    ok = sum(1 for v in results.values() if v.get("ok"))
    print(f"\n[RWM DataFetch] Done — {ok}/{len(TICKERS)} OK")
    print(f"  Salvato: data/market_data.json")

if __name__ == "__main__":
    main()
