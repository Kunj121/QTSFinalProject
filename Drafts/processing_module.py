import pandas as pd 
import numpy as np

def clean_data(data, price_data=None, on_chain_data=None, set_index=True):
    if price_data and on_chain_data:
        raise ValueError('Invalid Parameter Values: Both price_data or on_chain Data cannot be True')
    elif price_data:
        prices = data.copy()
        if prices.isna().any().any():
            print('you have nans here')
            return prices 
        
        if set_index:
            prices['time_period_end'] = pd.to_datetime(prices['time_period_end'])
            prices = prices.set_index('time_period_end') 
        else:
            prices.index = pd.to_datetime(prices.index)

        prices['time_open'] = pd.to_datetime(prices['time_open'])
        prices['time_close'] = pd.to_datetime(prices['time_close'])
        return prices
    
    elif on_chain_data:
        metrics = data.copy()
        if metrics.isna().any().any():
            print('you have nans here')
            return metrics 
        
        if set_index:
            metrics['hour'] = pd.to_datetime(metrics['hour'])
            metrics = metrics.set_index('hour')
        else:
            metrics.index = pd.to_datetime(metrics.index)
        return metrics
    else:
        raise ValueError('Invalid Parameter Values: price_data or on_chain Data must be True')
    

def check_missing_hours(df):
    """
    Check if the DataFrame's datetime index skips any hourly datapoints.
    
    Parameters:
        df (pd.DataFrame): DataFrame with a DatetimeIndex.
        
    Returns:
        missing (pd.DatetimeIndex): The missing hourly timestamps.
    """
    # Ensure the index is a DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be a DatetimeIndex.")
    
    # Create an expected date_range from the minimum to the maximum timestamp at hourly frequency
    expected_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='H')
    
    # Determine which timestamps are missing
    missing = expected_range.difference(df.index)
    return missing

def preprocess_data(data, price_data=None, on_chain_data=None, set_index=True):
    if price_data and on_chain_data:
        raise ValueError('Invalid Parameter Values. Both price_data and on_chain_data cannot both be True')

    elif (not price_data) and (not on_chain_data):
        raise ValueError('Invalid Parameter Values. Both price_data and on_chain_data cannot both be False')
    
    else:
        # Create the target df and merge on correct dates. Then forward fill the na values
        if price_data:
            df = clean_data(data.copy(), price_data=True, set_index=True) if set_index else clean_data(data.copy(), price_data=True, set_index=False)
            target_df = pd.DataFrame(0, columns=[0], index=pd.date_range(start=df.index.min(), end=df.index.max(), freq='H'))
        
        elif on_chain_data:
            df = clean_data(data.copy(), on_chain_data=True, set_index=True) if set_index else clean_data(data.copy(), on_chain_data=True, set_index=False)
            target_df = pd.DataFrame(0, columns=[0], index=pd.date_range(start=df.index.min(), end=df.index.max(), freq='H'))

        target_df = target_df.join(df, how='left').drop(0, axis=1)
        target_df = target_df.fillna(method='ffill')


        return target_df
    
def round_hours(staking_dat):
    staking_data = staking_dat.copy()
    staking_data['createdAt'] = pd.to_datetime(
    staking_data['createdAt'],
    format='mixed',
    utc=True,
    dayfirst=False)
    staking_data['createdAt_rounded'] = (staking_data['createdAt'] + pd.Timedelta(microseconds=1)).dt.ceil('H')

    return staking_data
    
def process_staking_data(raw_staking_data):

    raw_data = round_hours(raw_staking_data)
    token_dfs = {}

    for token in raw_data['symbol'].unique():
        # Filter data for the current token
        token_data = raw_data[raw_data['symbol'] == token].copy()
        
        # Pivot the data so that: index is the createdAt timestamp, columns are the metricKey values, and values are the defaultValue
        token_pivot = token_data.pivot_table(index='createdAt_rounded', columns='metricKey', values='defaultValue', aggfunc='first')
        token_pivot.sort_index(inplace=True)
        token_dfs[token] = token_pivot
    
    return token_dfs

def construct_forward_backward_dfs(features_df, prices_df, forward_size, backward_size):
    # Index of the dfs should be time_period_end

    # Aggregate the features
    features = features_df.copy()
    features['EMAcross_agg'] = features['EMAcross'].ewm(span=backward_size, adjust=False).mean()
    features['bollinger_agg'] = features['bollinger'].ewm(span=backward_size, adjust=False).mean()
    features['RSI_agg'] = features['RSI'].ewm(span=backward_size, adjust=False).mean()
    features['fib_23_agg'] = features['fib_23'].ewm(span=backward_size, adjust=False).mean()
    features['fib_38_agg'] = features['fib_38'].ewm(span=backward_size, adjust=False).mean()
    features['fib_50_agg'] = features['fib_50'].ewm(span=backward_size, adjust=False).mean()
    features['fib_61_agg'] = features['fib_61'].ewm(span=backward_size, adjust=False).mean()
    features['fib_78_agg'] = features['fib_78'].ewm(span=backward_size, adjust=False).mean()

    # Aggregate the normalized prices 
    prices = prices_df.copy()
    prices = prices.shift(forward_size) 

    if forward_size > backward_size:
        features = features.iloc[forward_size:]
        prices = prices.iloc[forward_size:]
    else:
        features = features.iloc[backward_size:]
        prices = prices.iloc[backward_size:]
    
    return features, prices