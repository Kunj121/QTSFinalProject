import requests
import pandas as pd
import time
import json
from dotenv import load_dotenv


import os
load_dotenv()
# Your CoinAPI API Key
API_KEY= os.getenv("coinapi")
def get_crypto_data(
    crypto="BTC",
    quote_currency="USDT",
    exchange="BINANCE",
    start_date="2019-02-25T00:00:00",
    end_date="2025-02-25T00:00:00",
    period_id="1DAY"
):
    """
    Fetch cryptocurrency historical OHLCV data from CoinAPI.

    Parameters:
    -----------
    crypto : str
        The cryptocurrency symbol (e.g., 'BTC', 'ETH', 'SOL')
    quote_currency : str
        The quote currency (e.g., 'USDT', 'USD', 'BUSD')
    exchange : str
        The exchange to fetch data from (e.g., 'BINANCE', 'COINBASE', 'KRAKEN')
    start_date : str
        Start date in ISO format (YYYY-MM-DDTHH:MM:SS)
    end_date : str
        End date in ISO format (YYYY-MM-DDTHH:MM:SS)
    period_id : str
        Time period of the candles (e.g., '1DAY', '1HRS', '15MIN')

    Returns:
    --------
    pandas.DataFrame or None
        DataFrame containing the OHLCV data, or None if data retrieval failed
    """
    # Construct the symbol ID based on the parameters
    symbol = f"{exchange}_SPOT_{crypto}_{quote_currency}"

    url = f"https://rest.coinapi.io/v1/ohlcv/{symbol}/history"
    headers = {"X-CoinAPI-Key": API_KEY}

    params = {
        "period_id": period_id,
        "time_start": start_date,
        "time_end": end_date,
        "limit": 10000  # Max records per request
    }

    all_data = []
    max_retries = 5

    print(f"Fetching {crypto}/{quote_currency} data from {exchange} with {period_id} candles")
    print(f"Symbol ID: {symbol}")
    print(f"Date range: {start_date} to {end_date}")

    while True:
        retry_count = 0
        response = None

        # Retry loop for handling rate limits
        while retry_count < max_retries:
            response = requests.get(url, headers=headers, params=params)

            # If we get a rate limit error (429)
            if response.status_code == 429:
                # Try to get the retry-after value
                retry_after = 15  # Default to 15 seconds

                try:
                    # Try to parse JSON response for retry-after info
                    error_data = response.json()
                    if "Retry-After" in error_data:
                        retry_after = int(error_data["Retry-After"])
                except:
                    # If can't parse JSON, try to get from headers
                    if "Retry-After" in response.headers:
                        retry_after = int(response.headers["Retry-After"])

                print(f"Rate limit hit. Waiting {retry_after} seconds before retrying...")
                time.sleep(retry_after + 1)  # Add 1 second buffer
                retry_count += 1
                continue

            # If we get any other error or success, break out of retry loop
            break

        # If we've exhausted our retries and still have rate limit issue
        if retry_count >= max_retries and response.status_code == 429:
            print(f"Still hitting rate limits after {max_retries} retries. Aborting.")
            break

        # Regular error handling
        if response.status_code != 200:
            print(f"Error: Status code {response.status_code}")
            print(f"Response: {response.text}")

            # If symbol not found, try alternative quote currencies
            if response.status_code == 404 or (response.text and "invalid or does not exist" in response.text):
                return None
            break

        # JSON parsing
        try:
            if response.text:
                data = response.json()
            else:
                print("Empty response received")
                break
        except Exception as e:
            print(f"JSON parsing error: {e}")
            print(f"Response content: {response.text[:200]}...")
            break

        # API error checking
        if isinstance(data, dict) and "error" in data:
            print("Error:", data["error"])
            break

        # Empty data check
        if not data:
            print("No more data available")
            break

        # Successfully got data
        df = pd.DataFrame(data)
        all_data.append(df)

        print(f"Retrieved {len(df)} records")

        # Update for next batch
        if len(df) > 0:
            last_timestamp = df["time_period_end"].iloc[-1]
            params["time_start"] = last_timestamp
            # Wait between successful requests too, to avoid hitting limits
            time.sleep(5)
        else:
            break

    # Process results
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        full_df["time_period_start"] = pd.to_datetime(full_df["time_period_start"])
        full_df.set_index("time_period_start", inplace=True)
        return full_df
    else:
        return None


def fetch_with_fallbacks(
        crypto="BTC",
        start_date="2019-02-25T00:00:00",
        end_date="2025-02-25T00:00:00",
        period_id="1DAY"
):
    """
    Attempt to fetch cryptocurrency data with fallbacks for quote currencies and exchanges.

    Parameters:
    -----------
    crypto : str
        The cryptocurrency symbol (e.g., 'BTC', 'ETH', 'SOL')
    start_date : str
        Start date in ISO format (YYYY-MM-DDTHH:MM:SS)
    end_date : str
        End date in ISO format (YYYY-MM-DDTHH:MM:SS)
    period_id : str
        Time period of the candles (e.g., '1DAY', '1HRS', '15MIN')

    Returns:
    --------
    pandas.DataFrame or None
        DataFrame containing the OHLCV data, or None if all attempts failed
    """

    # Try different quote currencies on Binance first
    quote_currencies = ["USDT", "USD", "BUSD"]
    for quote in quote_currencies:
        df = get_crypto_data(
            crypto=crypto,
            quote_currency=quote,
            exchange="BINANCE",
            start_date=start_date,
            end_date=end_date,
            period_id=period_id
        )

        if df is not None and not df.empty:
            print(f"Success with {crypto}/{quote} on BINANCE!")
            print(f"Total records: {len(df)}")
            print(f"Date range: {df.index.min()} to {df.index.max()}")
            return df

    # If Binance failed, try other major exchanges
    exchanges = ["COINBASE", "KRAKEN", "BITSTAMP", "KUCOIN"]
    for exchange in exchanges:
        for quote in quote_currencies:
            df = get_crypto_data(
                crypto=crypto,
                quote_currency=quote,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                period_id=period_id
            )

            if df is not None and not df.empty:
                print(f"Success with {crypto}/{quote} on {exchange}!")
                print(f"Total records: {len(df)}")
                print(f"Date range: {df.index.min()} to {df.index.max()}")
                return df

    print(f"Failed to retrieve data for {crypto} from any exchange or quote currency.")
    return None

# Example usage:
# Fetch BTC data with 1-day candles (default)
# btc_daily = fetch_with_fallbacks("BTC")

# Fetch ETH data with 1-hour candles
# eth_hourly = fetch_with_fallbacks("ETH", period_id="1HRS")

# Fetch SOL data with 15-minute candles for a specific date range
# sol_15min = fetch_with_fallbacks("SOL", start_date="2023-01-01T00:00:00", end_date="2023-12-31T23:59:59", period_id="15MIN")


def read_crypto_data(crypto:list):
    results = []
    for crypto in crypto:
        crypto_data = pd.read_csv(f'data/hourly_crypto_data/{crypto}_hourly.csv')
        crypto_data['time_period_end'] = pd.to_datetime(crypto_data['time_period_end'], errors='coerce')
        crypto_data['time_open'] = pd.to_datetime(crypto_data['time_open'], errors='coerce')
        crypto_data['time_close'] = pd.to_datetime(crypto_data['time_close'], errors='coerce')

        crypto_data['time_period_end'] = crypto_data['time_period_end'].dt.strftime('%Y-%m-%d %H:%M')
        crypto_data['time_open'] = crypto_data['time_open'].dt.strftime('%Y-%m-%d %H:%M')
        crypto_data['time_close'] = crypto_data['time_close'].dt.strftime('%Y-%m-%d %H:%M')

        results.append(crypto_data)

    return tuple(results)


# Example:
# btc, eth, sol = read_crypto_data(['btc', 'eth', 'sol'])
