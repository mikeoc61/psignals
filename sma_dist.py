#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "yfinance>=0.2.50",
#     "pandas>=2.0",
# ]
# ///

import sys
import yfinance as yf
import pandas as pd
import json

def sma_distances(ticker, windows=(20, 50, 200)):
    df = yf.download(ticker, period='2y', auto_adjust=True, progress=False)
    close = df['Close'].squeeze()
    out = {'ticker': ticker, 'price': round(float(close.iloc[-1]), 2)}
    for n in windows:
        sma = close.rolling(n).mean()
        std = close.rolling(n).std()
        out[f'sma{n}'] = round(float(sma.iloc[-1]), 2)
        out[f'dist{n}_pct'] = round(float(close.iloc[-1] / sma.iloc[-1] - 1) * 100, 2)
        out[f'z{n}'] = round(float((close.iloc[-1] - sma.iloc[-1]) / std.iloc[-1]), 2)
    return out

if __name__ == '__main__':
    tickers = sys.argv[1:] or ['FBTC']
    results = [sma_distances(t) for t in tickers]
    print(json.dumps(results))
