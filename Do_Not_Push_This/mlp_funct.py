#########################################
# Step 1. Imports
#########################################
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, LabelEncoder
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input, LeakyReLU
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.utils import to_categorical
import tensorflow as tf
import arch
from arch.univariate import ConstantMean, GARCH, Normal
import keras_tuner as kt
from imblearn.over_sampling import RandomOverSampler
import keras.backend as K
import sys




sys.path.append('../Drafts')
from processing_module import round_hours


def prep_data(crypto='aval'):
    """
    Prepare and align cryptocurrency data from various sources.

    Parameters:
    -----------
    crypto : str, default='aval'
        Cryptocurrency ticker symbol (lowercase)

    Returns:
    --------
    pandas.DataFrame
        Combined and aligned dataset with price, on-chain, and technical features
    """
    # ==================================================
    # Step 1. Load Data
    # ==================================================
    # Note: Some filenames use 'aval' while others use 'avax' - maintaining this pattern
    prices_file = f'../data/price_data/processed/{crypto}_processed_data.parquet'
    features1_file = f'../data/on_chain_data/processed/avax1_chain_processed.parquet'  # Using avax1 as in original
    features2_file = f'../data/on_chain_data/processed/avax2_chain_processed.parquet'  # Using avax2 as in original
    staking_file = f'../data/on_chain_data/processed/avax_staking_data2.parquet'  # Using avax as in original
    tech_file = f'../data/data_segmented_tech/avax_data.parquet'  # Using avax as in original

    prices = pd.read_parquet(prices_file)
    features1 = pd.read_parquet(features1_file).drop(columns=['average_difficulty'])
    features2 = pd.read_parquet(features2_file).drop(columns=['average_base_fee_per_gas', 'average_difficulty'])
    staking = pd.read_parquet(staking_file)
    tech = pd.read_parquet(tech_file)

    # ==================================================
    # Step 2. Convert Indices to DateTime
    # ==================================================
    staking.index = pd.to_datetime(staking.index)
    features1.index = pd.to_datetime(features1.index)
    features2.index = pd.to_datetime(features2.index)

    prices['time_close'] = pd.to_datetime(prices['time_close'])
    prices['time_close'] = prices['time_close'].dt.tz_localize('UTC')
    prices.set_index('time_close', inplace=True)
    prices.index = prices.index + pd.Timedelta(minutes=1)

    tech['time_close'] = pd.to_datetime(tech['time_close'])
    tech['time_close'] = tech['time_close'].dt.round('min')
    tech.set_index('time_close', inplace=True)

    # ==================================================
    # Step 3. Combine On-Chain Features
    # ==================================================
    df_combined = pd.concat([features1, features2]).sort_index()

    # ==================================================
    # Step 4. Align Staking Data with Combined Features
    # ==================================================
    common_start = max(staking.index.min(), df_combined.index.min())
    common_end = min(staking.index.max(), df_combined.index.max())

    df1_aligned = staking.loc[common_start:common_end]
    df_combined_aligned = df_combined.loc[common_start:common_end]

    # ==================================================
    # Step 5. Merge Staking and Combined Features
    # ==================================================
    crypto_features = pd.merge(
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
        crypto_features.index.min(),
        prices.index.min(),
        tech.index.min()
    )
    global_end = min(
        crypto_features.index.max(),
        prices.index.max(),
        tech.index.max()
    )

    crypto_features_aligned = crypto_features.loc[global_start:global_end]
    prices_aligned = prices.loc[global_start:global_end]

    # ==================================================
    # Step 7. Process and Align Technical Indicator Data
    # ==================================================
    tech_aligned = tech.loc[global_start:global_end].copy()

    # Drop duplicates in price & tech data
    prices_aligned = prices_aligned[~prices_aligned.index.duplicated(keep='first')]
    tech_aligned = tech_aligned[~tech_aligned.index.duplicated(keep='first')]

    # Reindex tech data using the nearest timestamps from price data
    tech_aligned = tech_aligned.reindex(prices_aligned.index, method='nearest')

    # Select relevant tech columns
    tech_cols = ['fib_23', 'fib_38', 'fib_50', 'fib_61', 'fib_78', 'bollinger', 'EMAcross', 'RSI']
    tech_selected = tech_aligned[tech_cols].rename(columns=lambda x: "tech_" + x)

    # ==================================================
    # Step 8. Join Price Data with On-Chain Features
    # ==================================================
    data = crypto_features_aligned.join(
        prices_aligned[['price_close']],
        how='inner'
    )

    # ==================================================
    # Step 9. Join Tech Features to Main Data
    # ==================================================
    data = data.join(tech_selected, how='left')

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



def model_run(data, threshold_multiplier):
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
    threshold = train_data['future_price_diff'].std() * threshold_multiplier

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




    #########################################
    # Step 4. Splitting data 70%/30%
    #########################################
    # Prepare features and target arrays
    accuracy_train = []
    accuracy_test = []

    # Use only the lag-adjusted feature columns for training
    X = data[all_lag_cols].values
    y = data['Signal'].values

    # Encode target labels
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(y)
    y_cat = to_categorical(integer_encoded)

    # Define the split index for 70% training data
    split_index = int(0.7 * len(X))

    # Split the data into training and testing sets
    X_train = X[:split_index]
    y_train = y_cat[:split_index]
    X_test = X[split_index:]
    y_test = y_cat[split_index:]

    scaler = StandardScaler()
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)

    # Calculate class weights
    from sklearn.utils.class_weight import compute_class_weight
    import numpy as np

    # Get the original labels (not one-hot encoded) for training set
    y_train_labels = label_encoder.inverse_transform(np.argmax(y_train, axis=1))

    # Compute class weights
    class_weights = compute_class_weight(
        class_weight='balanced',
        classes=np.unique(y_train_labels),
        y=y_train_labels
    )

    # Create a dictionary of class weights
    class_weight_dict = {i: weight for i, weight in enumerate(class_weights)}

    # Build the model
    model = Sequential([
        Input(shape=(X_train.shape[1],)),
        Dense(64),
        LeakyReLU(negative_slope=0.1),
        Dense(32),
        LeakyReLU(negative_slope=0.1),
        Dense(y_cat.shape[1], activation='softmax')
    ])
    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    # Train the model with class weights
    history = model.fit(
        X_train, y_train,
        epochs=50,
        batch_size=32,
        validation_data=(X_test, y_test),
        class_weight=class_weight_dict,
        verbose=0
    )

    # Evaluate the model on the test set
    score_2 = model.evaluate(X_train, y_train, verbose=0)
    scores = model.evaluate(X_test, y_test, verbose=0)
    print(f"Train accuracy: {score_2[1]*100:.2f}%")
    accuracy_train.append(score_2[1])
    print(f"Test accuracy: {scores[1]*100:.2f}%")
    accuracy_test.append(scores[1])
    best_model = model

    return best_model



def confusion_matrix(data):


    #########################################
    # Step 4. Splitting data 70%/30%
    #########################################
    # Prepare features and target arrays
    tech_cols = ['fib_23', 'fib_38', 'fib_50', 'fib_61', 'fib_78', 'bollinger', 'EMAcross', 'RSI']

    lag_cols = [
        'average_gas_limit', 'average_gas_used', 'average_size',
        'average_total_difficulty', 'active_validators', 'real_reward_rate',
        'staked_tokens', 'staking_ratio', 'total_staking_wallets'
    ]
    tech_cols_renamed = ["tech_" + col for col in tech_cols]
    all_lag_cols = lag_cols + tech_cols_renamed

    # Use only the lag-adjusted feature columns for training
    X = data[all_lag_cols].values
    y = data['Signal'].values

    # Encode target labels
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(y)
    y_cat = to_categorical(integer_encoded)

    # Define the split index for 70% training data
    split_index = int(0.7 * len(X))

    # Split the data into training and testing sets
    X_train = X[:split_index]
    y_train = y_cat[:split_index]
    X_test = X[split_index:]
    y_test = y_cat[split_index:]

    scaler = StandardScaler()
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)

    # Calculate class weights
    from sklearn.utils.class_weight import compute_class_weight
    import numpy as np

    # Get the original labels (not one-hot encoded) for training set
    y_train_labels = label_encoder.inverse_transform(np.argmax(y_train, axis=1))

    # Compute class weights
    class_weights = compute_class_weight(
        class_weight='balanced',
        classes=np.unique(y_train_labels),
        y=y_train_labels
    )

    # Create a dictionary of class weights
    class_weight_dict = {i: weight for i, weight in enumerate(class_weights)}

    # Build the model
    model = Sequential([
        Input(shape=(X_train.shape[1],)),
        Dense(64),
        LeakyReLU(negative_slope=0.1),
        Dense(32),
        LeakyReLU(negative_slope=0.1),
        Dense(y_cat.shape[1], activation='softmax')
    ])
    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    # Train the model with class weights
    history = model.fit(
        X_train, y_train,
        epochs=50,
        batch_size=32,
        validation_data=(X_test, y_test),
        class_weight=class_weight_dict,
        verbose=0
    )

    # Evaluate the model on the test set
    score_2 = model.evaluate(X_train, y_train, verbose=0)
    scores = model.evaluate(X_test, y_test, verbose=0)
    print(f"Train accuracy: {score_2[1] * 100:.2f}%")
    print(f"Test accuracy: {scores[1] * 100:.2f}%")
    best_model = model

    #########################################
    # Confusion Matrix
    #########################################
    from sklearn.metrics import confusion_matrix, classification_report
    import matplotlib.pyplot as plt
    import seaborn as sns

    # Get predictions
    y_pred = model.predict(X_test)
    y_pred_classes = np.argmax(y_pred, axis=1)
    y_test_classes = np.argmax(y_test, axis=1)

    # Get the original class names
    class_names = label_encoder.classes_

    # Create confusion matrix
    cm = confusion_matrix(y_test_classes, y_pred_classes)

    # Plot confusion matrix
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=class_names,
                yticklabels=class_names)
    plt.xlabel('Predicted')
    plt.ylabel('True')
    plt.title('Confusion Matrix')
    plt.tight_layout()
    plt.show()

    # Print classification report
    print("\nClassification Report:")
    print(classification_report(
        y_test_classes,
        y_pred_classes,
        target_names=class_names
    ))

def plot_strat(data, best_model):



    tech_cols = ['fib_23', 'fib_38', 'fib_50', 'fib_61', 'fib_78', 'bollinger', 'EMAcross', 'RSI']

    lag_cols = [
        'average_gas_limit', 'average_gas_used', 'average_size',
        'average_total_difficulty', 'active_validators', 'real_reward_rate',
        'staked_tokens', 'staking_ratio', 'total_staking_wallets'
    ]
    tech_cols_renamed = ["tech_" + col for col in tech_cols]
    all_lag_cols = lag_cols + tech_cols_renamed

    X = data[all_lag_cols].values
    y = data['Signal'].values

    # Encode target labels
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(y)
    y_cat = to_categorical(integer_encoded)

    # Define the split index for 70% training data
    split_index = int(0.7 * len(X))



    # Define test_data from the combined data split
    test_data = data.iloc[split_index:].copy()
    test_data['asset_return'] = test_data['price_close'].pct_change()
    test_data.dropna(inplace=True)

    # Prepare test features and scale them
    features_test_aligned = test_data[all_lag_cols].values

    scaler = StandardScaler()
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)


    X_test_aligned = scaler.transform(features_test_aligned)

    # Get prediction probabilities from the trained model
    y_pred_test_aligned = best_model.predict(X_test_aligned)

    # Define positions as the difference between probability of Buy (assumed index 0)
    # and probability of Sell (assumed index 2)
    positions_test_aligned = y_pred_test_aligned[:, 0] - y_pred_test_aligned[:, 2]

    # Compute the strategy returns: assume positions are applied with a one-period lag
    asset_returns_test = test_data['asset_return'].values.astype(np.float32)
    strategy_returns_test = np.roll(positions_test_aligned, shift=1) * asset_returns_test
    strategy_returns_test[0] = 0  # Set the first return to zero

    # Calculate cumulative returns and performance metrics
    cumulative_returns_test = np.cumprod(1 + strategy_returns_test) - 1
    total_pnl_percentage = cumulative_returns_test[-1]
    sharpe_ratio_test = (np.mean(strategy_returns_test) / np.std(strategy_returns_test)) * np.sqrt(365)
    initial_capital = 100000
    absolute_pnl_test = total_pnl_percentage * initial_capital

    print("\nTrading Performance on Test Data:")
    print("Total PnL (percentage): {:.2%}".format(total_pnl_percentage))
    print("Absolute PnL: ${:,.2f}".format(absolute_pnl_test))
    print("Annualized Sharpe Ratio: {:.4f}".format(sharpe_ratio_test))

    # Plot performance metrics
    dates = test_data.index
    cumulative_returns_series = pd.Series(cumulative_returns_test, index=dates)
    returns_series = pd.Series(strategy_returns_test, index=dates)

    window = 30
    rolling_sharpe = returns_series.rolling(window=window).apply(
        lambda r: (np.mean(r) / np.std(r) * np.sqrt(365)) if np.std(r) != 0 else 0,
        raw=True
    )

    plt.figure(figsize=(14, 10))

    plt.subplot(2, 1, 1)
    plt.plot(cumulative_returns_series.index, cumulative_returns_series, label='Cumulative Returns (PnL)')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Return')
    plt.title('Strategy Cumulative Returns Over Time')
    plt.legend()
    plt.grid(True)

    plt.subplot(2, 1, 2)
    plt.plot(rolling_sharpe.index, rolling_sharpe, label=f'{window}-Day Rolling Sharpe Ratio', color='orange')
    plt.xlabel('Date')
    plt.ylabel('Rolling Sharpe Ratio')
    plt.title(f'{window}-Day Rolling Sharpe Ratio Over Time')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.show()



def run_x_times(data, best_model, num_runs =10):



    tech_cols = ['fib_23', 'fib_38', 'fib_50', 'fib_61', 'fib_78', 'bollinger', 'EMAcross', 'RSI']

    lag_cols = [
        'average_gas_limit', 'average_gas_used', 'average_size',
        'average_total_difficulty', 'active_validators', 'real_reward_rate',
        'staked_tokens', 'staking_ratio', 'total_staking_wallets'
    ]
    tech_cols_renamed = ["tech_" + col for col in tech_cols]
    all_lag_cols = lag_cols + tech_cols_renamed


    # Define features and target
    X = data[all_lag_cols].values
    y = data['Signal'].values
    split_index = int(0.7 * len(X))


    # Split into train and test sets
    X_train = X[:split_index]
    y_train = y[:split_index]
    X_test = X[split_index:]
    y_test = y[split_index:]

    X = data[all_lag_cols].values
    y = data['Signal'].values

    scaler = StandardScaler()
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)

    # Encode target labels
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(y)
    y_cat = to_categorical(integer_encoded)

    # Define the split index for 70% training data
    split_index = int(0.7 * len(X))

    test_data = data.iloc[split_index:].copy()
    test_data['asset_return'] = test_data['price_close'].pct_change()
    test_data.dropna(inplace=True)

    #########################################
    # Step 9. Run the model 10 more times and show each PnL, abs PnL, and annualized Sharpe
    #########################################
    total_pnl_list = []
    absolute_pnl_list = []
    sharpe_ratio_list = []

    # List to store each newly trained model
    model_list = []

    for run in range(num_runs):
        # print(f"\nTraining run {run + 1}/{num_runs}")

        # Create a new instance of the same model architecture
        model_run = Sequential([
            Input(shape=(X_train.shape[1],)),
            Dense(64),
            LeakyReLU(negative_slope=0.1),
            Dense(32),
            LeakyReLU(negative_slope=0.1),
            Dense(y_cat.shape[1], activation='softmax')
        ])
        model_run.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

        # Train the model for 50 epochs
        model_run.fit(
            X_train, y_train,
            epochs=50,
            batch_size=32,
            validation_split=0.2,
            verbose=0
        )
        scores_1 = model_run.evaluate(X_test, y_test, verbose=0)
        scores_2 = model_run.evaluate(X_train, y_train, verbose=0)
        # print(f"Train accuracy: {scores_2[1] * 100:.2f}%")
        # print(f"Test accuracy: {scores_1[1] * 100:.2f}%")
        # Store this newly trained model in a list
        model_list.append(model_run)

        # Evaluate on the test set
        test_data_run = test_data.copy()
        test_data_run['asset_return'] = test_data_run['price_close'].pct_change()
        strategy_data = test_data_run.dropna().copy()

        features_test_aligned = strategy_data[all_lag_cols].values
        X_test_aligned = scaler.transform(features_test_aligned)

        y_pred_test_aligned = model_run.predict(X_test_aligned)

        # Define positions as the difference between probability of Buy (index 0) and Sell (index 2)
        positions_test_aligned = y_pred_test_aligned[:, 0] - y_pred_test_aligned[:, 2]

        asset_returns_test = strategy_data['asset_return'].values.astype(np.float32)
        strategy_returns_test = np.roll(positions_test_aligned, shift=1) * asset_returns_test
        strategy_returns_test[0] = 0  # adjust the first value

        cumulative_returns_test = np.cumprod(1 + strategy_returns_test) - 1
        total_pnl_percentage = cumulative_returns_test[-1]

        # Compute Sharpe ratio; handle zero standard deviation
        std_returns = np.std(strategy_returns_test)
        if std_returns != 0:
            sharpe_ratio_test = (np.mean(strategy_returns_test) / std_returns) * np.sqrt(365)
        else:
            sharpe_ratio_test = 0

        initial_capital = 100000
        absolute_pnl_test = total_pnl_percentage * initial_capital

        # print(f"Run {run + 1} Trading Performance:")
        # print("  Total PnL (percentage): {:.2%}".format(total_pnl_percentage))
        # print("  Absolute PnL: ${:,.2f}".format(absolute_pnl_test))
        # print("  Annualized Sharpe Ratio: {:.4f}".format(sharpe_ratio_test))

        total_pnl_list.append(total_pnl_percentage)
        absolute_pnl_list.append(absolute_pnl_test)
        sharpe_ratio_list.append(sharpe_ratio_test)

    # Compute and display average results over all runs
    avg_total_pnl = np.mean(total_pnl_list)
    avg_absolute_pnl = np.mean(absolute_pnl_list)
    avg_sharpe_ratio = np.mean(sharpe_ratio_list)

    # print("\nAverage Trading Performance over {} runs:".format(num_runs))
    # print("  Average Total PnL (percentage): {:.2%}".format(avg_total_pnl))
    # print("  Average Absolute PnL: ${:,.2f}".format(avg_absolute_pnl))
    # print("  Average Annualized Sharpe Ratio: {:.4f}".format(avg_sharpe_ratio))

    return avg_total_pnl, avg_absolute_pnl, avg_sharpe_ratio