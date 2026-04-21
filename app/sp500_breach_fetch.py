# Required imports
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='sp500_breach_check.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# API Key for Financial Modeling Prep
API_KEY = "6sCr4LvlHAXvZbbWta9nugrs5X6qoKwH"

# Load S&P 500 tickers from Wikipedia
# Load S&P 500 tickers from Wikipedia
try:
    import requests

    wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}  # Wikipedia blocks non-browser requests
    response = requests.get(wiki_url, headers=headers)
    response.raise_for_status()

    tables = pd.read_html(response.text)
    sp500_table = tables[0]
    tickers_sp500 = sp500_table['Symbol'].tolist()
    tickers_sp500 = [ticker.replace('.', '-') for ticker in tickers_sp500]

    print(f"Loaded {len(tickers_sp500)} S&P 500 tickers.")
except Exception as e:
    print("Failed to load ticker list:", e)
    tickers_sp500 = []


# Function to fetch and process data for a given stock ticker
def process_sp500_stock(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={API_KEY}"
        response = requests.get(url)
        data = response.json()

        if 'historical' not in data:
            logging.warning(f"No data found for {ticker}")
            return None

        df = pd.DataFrame(data['historical'])
        df = df[['date', 'close', 'volume']]
        df.rename(columns={'date': 'Date', 'close': 'Close Price', 'volume': 'Volume'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', ascending=False, inplace=True)
        df = df[df['Date'] >= '2020-01-01']

        if df.empty:
            logging.warning(f"No recent data for {ticker}")
            return None

        df['Price Change'] = df['Close Price'].pct_change(-1) * 100
        df['EMA7'] = df['Close Price'].iloc[::-1].ewm(span=7, adjust=False).mean().iloc[::-1]
        df['EMA30'] = df['Close Price'].iloc[::-1].ewm(span=30, adjust=False).mean().iloc[::-1]
        df['EMA90'] = df['Close Price'].iloc[::-1].ewm(span=90, adjust=False).mean().iloc[::-1]
        df['EMA180'] = df['Close Price'].iloc[::-1].ewm(span=180, adjust=False).mean().iloc[::-1]

        def price_gap(price, ema):
            return (price - ema) / ema if price > ema else (price - ema) / price

        df['Price/EMA7 Gap'] = df.apply(lambda row: price_gap(row['Close Price'], row['EMA7']), axis=1)
        df['Price/EMA30 Gap'] = df.apply(lambda row: price_gap(row['Close Price'], row['EMA30']), axis=1)
        df['Price/EMA90 Gap'] = df.apply(lambda row: price_gap(row['Close Price'], row['EMA90']), axis=1)
        df['Price/EMA180 Gap'] = df.apply(lambda row: price_gap(row['Close Price'], row['EMA180']), axis=1)

        df['Weighted Avg Gap'] = (
            0.25 * df['Price/EMA7 Gap'] +
            0.25 * df['Price/EMA30 Gap'] +
            0.25 * df['Price/EMA90 Gap'] +
            0.25 * df['Price/EMA180 Gap']
        )

        # --- Correct Forward Return Calculation ---
        df_sorted = df.sort_values(by='Date', ascending=True).reset_index(drop=True)
        for days in [5, 10, 20]:
            df_sorted[f'+{days}d Return'] = df_sorted['Close Price'].pct_change(periods=days).shift(-days) * 100
        df = pd.merge(df, df_sorted[['Date', '+5d Return', '+10d Return', '+20d Return']], on='Date', how='left')

        gaps = df['Weighted Avg Gap']
        thresholds = {
            "WA - 3σ": np.percentile(gaps, 99.865),
            "WA - 2σ": np.percentile(gaps, 97.725),
            "WA - 90th pct": np.percentile(gaps, 90),
            "WA - 10th pct": np.percentile(gaps, 10),
            "WA - (-2σ)": np.percentile(gaps, 2.275),
            "WA - (-3σ)": np.percentile(gaps, 0.135)
        }

        def classify_wa_breach(gap):
            if gap > thresholds["WA - 3σ"]:
                return "WA - 3σ"
            elif gap > thresholds["WA - 2σ"]:
                return "WA - 2σ"
            elif gap > thresholds["WA - 90th pct"]:
                return "WA - 90th pct"
            elif gap < thresholds["WA - (-3σ)"]:
                return "WA - (-3σ)"
            elif gap < thresholds["WA - (-2σ)"]:
                return "WA - (-2σ)"
            elif gap < thresholds["WA - 10th pct"]:
                return "WA - 10th pct"
            return "NA"

        most_recent_row = df.iloc[0]
        return {
            'Ticker': ticker,
            'Date': most_recent_row['Date'].strftime('%Y-%m-%d'),
            'Weighted Avg Gap': most_recent_row['Weighted Avg Gap'],
            'Breach Category': classify_wa_breach(most_recent_row['Weighted Avg Gap']),
            '+5d Return': most_recent_row['+5d Return'],
            '+10d Return': most_recent_row['+10d Return'],
            '+20d Return': most_recent_row['+20d Return']
        }

    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")
        return None

# Track results
breach_summary = []

# Process all S&P 500 tickers
for ticker in tqdm(tickers_sp500[:10], desc="Processing first 10 tickers"):
    result = process_sp500_stock(ticker)
    if result is not None:
        breach_summary.append(result)
        if len(breach_summary) <= 5:
            print(pd.DataFrame(breach_summary).tail(1))

# Output results
breach_df = pd.DataFrame(breach_summary)
today = datetime.today().strftime('%Y-%m-%d')
breach_df.to_excel(f'SP500_WeightedGap_Latest_Report_{today}.xlsx', index=False)

print(f"✅ Done. Report saved as SP500_WeightedGap_Latest_Report_{today}.xlsx")

breach_df.to_csv('data/SP500_Breach_Report_Latest.csv', index=False)
print("✅ CSV updated for dashboard")