import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input, LeakyReLU, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import LabelEncoder, StandardScaler
import matplotlib.pyplot as plt
from collections import Counter
from sklearn.utils.class_weight import compute_class_weight
from imblearn.over_sampling import SMOTE
from sklearn.metrics import classification_report, confusion_matrix, balanced_accuracy_score

def prep_data(crypto = 'aval'):

    # ==================================================
    # Step 1. Load Data
    # ==================================================
    aval_prices    = pd.read_parquet('../data/price_data/processed/aval_processed_data.parquet')
    avax1_features = pd.read_parquet('../data/on_chain_data/processed/avax1_chain_processed.parquet').drop(columns=['average_difficulty'])
    avax2_features = pd.read_parquet('../data/on_chain_data/processed/avax2_chain_processed.parquet').drop(columns=['average_base_fee_per_gas','average_difficulty'])
    avax_staking   = pd.read_parquet('../data/on_chain_data/processed/avax_staking_data2.parquet')
    avax_tech      = pd.read_parquet('../data/data_segmented_tech/avax_data.parquet')

    # ==================================================
    # Step 2. Convert Indices to DateTime
    # ==================================================
    avax_staking.index   = pd.to_datetime(avax_staking.index)
    avax1_features.index = pd.to_datetime(avax1_features.index)
    avax2_features.index = pd.to_datetime(avax2_features.index)

    aval_prices['time_close'] = pd.to_datetime(aval_prices['time_close'])
    aval_prices['time_close'] = aval_prices['time_close'].dt.tz_localize('UTC')
    aval_prices.set_index('time_close', inplace=True)
    aval_prices.index = aval_prices.index + pd.Timedelta(minutes=1)

    avax_tech['time_close'] = pd.to_datetime(avax_tech['time_close'])
    avax_tech['time_close'] = avax_tech['time_close'].dt.round('min')
    avax_tech.set_index('time_close', inplace=True)

    # ==================================================
    # Step 3. Combine On-Chain Features
    # ==================================================
    df_combined = pd.concat([avax1_features, avax2_features]).sort_index()

    # ==================================================
    # Step 4. Align Staking Data with Combined Features
    # ==================================================
    common_start = max(avax_staking.index.min(), df_combined.index.min())
    common_end   = min(avax_staking.index.max(), df_combined.index.max())

    df1_aligned         = avax_staking.loc[common_start:common_end]
    df_combined_aligned = df_combined.loc[common_start:common_end]

    avax_features = pd.merge(
        df1_aligned,
        df_combined_aligned,
        left_index=True,
        right_index=True,
        how='inner'
    )

    # ==================================================
    # Step 6. Clean and Align Price Data
    # ==================================================
    global_start = max(
        avax_features.index.min(),
        aval_prices.index.min(),
        avax_tech.index.min()
    )
    global_end = min(
        avax_features.index.max(),
        aval_prices.index.max(),
        avax_tech.index.max()
    )

    avax_features_aligned = avax_features.loc[global_start:global_end]
    aval_prices_aligned   = aval_prices.loc[global_start:global_end]

    # ==================================================
    # Step 7. Process and Align Technical Indicator Data
    # ==================================================
    avax_tech_aligned = avax_tech.loc[global_start:global_end].copy()

    # Drop duplicates in price & tech data
    aval_prices_aligned = aval_prices_aligned[~aval_prices_aligned.index.duplicated(keep='first')]
    avax_tech_aligned   = avax_tech_aligned[~avax_tech_aligned.index.duplicated(keep='first')]

    # Reindex tech data using the nearest timestamps from price data
    avax_tech_aligned = avax_tech_aligned.reindex(aval_prices_aligned.index, method='nearest')

    # Select relevant tech columns
    tech_cols = ['fib_23', 'fib_38', 'fib_50', 'fib_61', 'fib_78', 'bollinger', 'EMAcross', 'RSI']
    avax_tech_selected = avax_tech_aligned[tech_cols].rename(columns=lambda x: "tech_" + x)

    # ==================================================
    # Step 8. Join Price Data with On-Chain Features
    # ==================================================
    data = avax_features_aligned.join(
        aval_prices_aligned[['price_close']],
        how='inner'
    )

    # ==================================================
    # Step 9. Join Tech Features to Main Data
    # ==================================================
    data = data.join(avax_tech_selected, how='left')

    return data



def add_more_indicators(data):
    data['price_pct_change'] = data['price_close'].pct_change()
    data['price_momentum_5'] = data['price_close'].pct_change(5)
    data['price_momentum_10'] = data['price_close'].pct_change(10)

    # Add volatility indicators
    data['price_volatility_5'] = data['price_close'].rolling(5).std() / data['price_close'].rolling(5).mean()
    data['price_volatility_10'] = data['price_close'].rolling(10).std() / data['price_close'].rolling(10).mean()

    # Add rate of change indicators
    data['roc_5'] = (data['price_close'] - data['price_close'].shift(5)) / data['price_close'].shift(5) * 100
    data['roc_10'] = (data['price_close'] - data['price_close'].shift(10)) / data['price_close'].shift(10) * 100

    return data



def apply_splits(data):
    # ==================================================
    # Step 11. Define Lag Columns
    # ==================================================
    # Exclude target-related columns from lagging
    all_lag_cols = list(data.columns.difference(['price_close', 'future_price_diff', 'Signal']))

    # Apply lag to all feature columns
    data[all_lag_cols] = data[all_lag_cols].shift(1)

    # ==================================================
    # Step 12. Generate Forward-Looking Target
    # ==================================================
    data['future_price_diff'] = data['price_close'].shift(-1) - data['price_close']

    # Define the function for signals with adjustable threshold
    def get_signal(diff, threshold):
        if diff > threshold:
            return 'Buy'
        elif diff < -threshold:
            return 'Sell'
        else:
            return 'Hold'

    # ==================================================
    # Step 13. Prepare Data for Modeling
    # ==================================================
    # Sort by date and drop rows with missing values
    data.sort_index(inplace=True)
    data.dropna(inplace=True)

    # Split into training and test sets (70% train, 30% test)
    split_index = int(0.7 * len(data))
    train_data = data.iloc[:split_index]
    test_data = data.iloc[split_index:]

    # Calculate threshold using training data - reduced to create more Buy/Sell signals
    threshold = train_data['future_price_diff'].std() * 0.25

    # Generate signals across the entire dataset based on the training threshold
    data['Signal'] = data['future_price_diff'].apply(lambda x: get_signal(x, threshold))

    # Define features and target
    X = data[all_lag_cols].values
    y = data['Signal'].values

    # Split into train and test sets
    X_train = X[:split_index]
    y_train = y[:split_index]
    X_test = X[split_index:]
    y_test = y[split_index:]

    return X_train, X_test, y_train, y_test, all_lag_cols
