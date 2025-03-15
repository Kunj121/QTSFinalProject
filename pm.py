import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def prep_data():

    avax_preds = pd.read_csv('Coin_Models/avax_orders.csv')
    near_preds = pd.read_csv('Coin_Models/near_orders.csv')
    sol_preds = pd.read_csv('Coin_Models/sol_orders.csv')
    dot_preds = pd.read_csv('Coin_Models/dot_orders.csv')
    matic_preds = pd.read_csv('Coin_Models/matic_orders.csv')

    avax_prices = pd.read_parquet('data/price_data/processed/aval_processed_data.parquet')
    dot_prices  = pd.read_parquet('Crypto_QTS_Data_Processed/dot_price_processed.parquet')
    matic_prices = pd.read_parquet('data/price_data/processed/matic_processed_data.parquet')
    near_prices  = pd.read_parquet('Crypto_QTS_Data_Processed/near_price_processed.parquet')
    sol_prices = pd.read_parquet('data/price_data/processed/sol_processed_data.parquet')




    dot_prices['time_period_end'] = dot_prices.index
    dot_prices.index = np.arange(len(dot_prices))

    near_prices['time_period_end'] = near_prices.index
    near_prices.index = np.arange(len(near_prices))


    # Define the split index for 70% training data
    split_index = len(avax_preds)
    avax_prices = avax_prices[-split_index:].reset_index().drop(columns=['index'])
    avax_preds['time'] = avax_prices['time_period_end']
    avax_preds = avax_preds[(avax_preds['time'] >= '2024-01-01') & (avax_preds['time'] <= '2024-09-09')].reset_index().drop(columns=['index'])
    avax_prices = avax_prices[(avax_prices['time_period_end'] >= '2024-01-01') & (avax_prices['time_period_end'] <= '2024-09-09')].reset_index().drop(columns=['index'])

    split_index = len(near_preds)
    near_prices = near_prices[-split_index:].reset_index().drop(columns=['index'])
    near_preds['time'] = near_prices['time_period_end']
    near_preds = near_preds[(near_preds['time'] >= '2024-01-01') & (near_preds['time'] <= '2024-09-09')].reset_index().drop(columns=['index'])
    near_prices = near_prices[(near_prices['time_period_end'] >= '2024-01-01') & (near_prices['time_period_end'] <= '2024-09-09')].reset_index().drop(columns=['index'])

    split_index = len(sol_preds)
    sol_prices = sol_prices[-split_index:].reset_index().drop(columns=['index'])
    sol_preds['time'] = sol_prices['time_period_end']
    sol_preds = sol_preds[(sol_preds['time'] >= '2024-01-01') & (sol_preds['time'] <= '2024-09-09')].reset_index().drop(columns=['index'])
    sol_prices = sol_prices[(sol_prices['time_period_end'] >= '2024-01-01') & (sol_prices['time_period_end'] <= '2024-09-09')].reset_index().drop(columns=['index'])

    split_index = len(dot_preds)
    dot_prices = dot_prices[-split_index:].reset_index().drop(columns=['index'])
    dot_preds['time'] = dot_prices['time_period_end']
    dot_preds = dot_preds[(dot_preds['time'] >= '2024-01-01') & (dot_preds['time'] <= '2024-09-09')].reset_index().drop(columns=['index'])
    dot_prices = dot_prices[(dot_prices['time_period_end'] >= '2024-01-01') & (dot_prices['time_period_end'] <= '2024-09-09')].reset_index().drop(columns=['index'])

    split_index = len(matic_preds)
    matic_prices = matic_prices[-split_index:].reset_index().drop(columns=['index'])
    matic_preds['time'] = matic_prices['time_period_end']
    matic_preds = matic_preds[(matic_preds['time'] >= '2024-01-01') & (matic_preds['time'] <= '2024-09-09')].reset_index().drop(columns=['index'])
    matic_prices = matic_prices[(matic_prices['time_period_end'] >= '2024-01-01') & (matic_prices['time_period_end'] <= '2024-09-09')].reset_index().drop(columns=['index'])




    dfs = {
        "AVAX": avax_preds,
        "NEAR": near_preds,
        "SOL": sol_preds,
        "DOT": dot_preds,
        "MATIC": matic_preds
    }

    price_dfs = {
        "AVAX": avax_prices,
        "DOT": dot_prices,
        "MATIC": matic_prices,
        "NEAR": near_prices,
        "SOL": sol_prices
    }



    avax_preds.drop(columns='Unnamed: 0', inplace=True)
    near_preds.drop(columns='Unnamed: 0', inplace=True)
    sol_preds.drop(columns='Unnamed: 0', inplace=True)
    dot_preds.drop(columns='Unnamed: 0', inplace=True)
    matic_preds.drop(columns='Unnamed: 0', inplace=True)


    return dfs, price_dfs






def calculate_investment_portfolio(dfs, price_dfs, starting_capital, portfolio_size, coin_names, stop_loss_pct=0.55, trading_fee_proportion = 0.001):
    """
    Calculates investment amounts and PNL for each coin, incorporating price data with stop-loss logic.
    """
    stop_loss_events = {coin: [] for coin in coin_names}
    max_investment_per_coin = starting_capital / portfolio_size
    coin_investments = {coin: 0 for coin in coin_names}
    coin_cash = {coin: max_investment_per_coin for coin in coin_names}
    coin_holdings = {coin: 0 for coin in coin_names}
    coin_values = {coin: 0 for coin in coin_names}
    coin_entry_price = {coin: None for coin in coin_names}  # Store initial buy price per coin

    all_results = {}


    stop_loss_pct /= 100  # Convert percentage to decimal

    for coin, df in dfs.items():
        results = []
        price_df = price_dfs[coin].copy()
        price_df = price_df.rename(columns={'time_period_end': 'time', 'price_close': 'Close'})
        price_df['time'] = pd.to_datetime(price_df['time'])
        df['time'] = pd.to_datetime(df['time'])
        price_df = price_df.set_index('time')
        df = df.set_index('time')

        for index, row in df.iterrows():
            prediction = row["Prediction"]
            prob_buy = row["Probability Buy"] / 100
            prob_sell = row["Probability Sell"] / 100

            # Ensure index exists in price_df; otherwise, forward-fill the last known price
            if index in price_df.index:
                current_price = price_df.loc[index, 'Close']
            else:
                current_price = price_df['Close'].iloc[price_df.index.get_loc(index, method='ffill')]

            investment_amount = 0
            stop_loss_triggered = False

            # Stop-loss check: If the price drops below stop-loss level, force sell
            if coin_holdings[coin] > 0 and coin_entry_price[coin] is not None:
                stop_loss_price = coin_entry_price[coin] * (1 - stop_loss_pct)
                if current_price < stop_loss_price:
                    stop_loss_events[coin].append(index)  # Store stop-loss event
                    sell_value = coin_holdings[coin] * current_price
                    investment_amount = -sell_value
                    coin_cash[coin] += sell_value
                    coin_holdings[coin] = 0  # Liquidate position
                    coin_entry_price[coin] = None  # Reset entry price
                    stop_loss_triggered = True
                    # print(f"Stop-loss triggered for {coin} at {index}. Sold holdings at {current_price}")

            # Only proceed with regular buy/sell logic if stop-loss wasn't triggered
            if not stop_loss_triggered:
                if prediction == 0:  # Buy
                    available_cash = coin_cash[coin]
                    if available_cash > 0:
                        investment_amount = min(available_cash, max_investment_per_coin * prob_buy)
                        trading_fee = trading_fee_proportion * investment_amount
                        coin_investments[coin] += investment_amount
                        coin_cash[coin] -= (investment_amount + trading_fee)
                        units_bought = investment_amount / current_price
                        coin_holdings[coin] += units_bought

                        # Set entry price if this is a new position or adjust for additional buys
                        if coin_entry_price[coin] is None:
                            coin_entry_price[coin] = current_price
                        else:
                            # Weighted average entry price for additional purchases
                            prev_value = (coin_holdings[coin] - units_bought) * coin_entry_price[coin]
                            new_value = units_bought * current_price
                            coin_entry_price[coin] = (prev_value + new_value) / coin_holdings[coin]


                elif prediction == 2:  # Sell
                    available_holdings = coin_holdings[coin]
                    if available_holdings > 0:
                        sell_value = min(available_holdings * current_price, max_investment_per_coin * prob_sell)
                        units_sold = min(available_holdings, sell_value / current_price)  # Ensure valid units_sold
                        investment_amount = -units_sold * current_price
                        trading_fee = trading_fee_proportion * abs(investment_amount)
                        coin_investments[coin] += investment_amount
                        coin_cash[coin] += (-investment_amount - trading_fee)  # Add cash from sale
                        coin_holdings[coin] -= units_sold

                        # Reset entry price if all holdings are sold
                        if coin_holdings[coin] == 0:
                            coin_entry_price[coin] = None

            # Update values
            coin_values[coin] = coin_holdings[coin] * current_price
            total_coin_value = sum(coin_values.values())
            total_cash = sum(coin_cash.values())
            total_portfolio_value = total_coin_value + total_cash

            results.append({
                "Prediction": prediction,
                "Probability Buy": row["Probability Buy"],
                "Probability Sell": row["Probability Sell"],
                "Investment": investment_amount,
                "Cash Holdings": coin_cash[coin],
                "Stock Holdings": coin_holdings[coin],
                "Coin Value": coin_values[coin],
                "Total Portfolio Value": total_portfolio_value,
                "Stop Loss Triggered": stop_loss_triggered  # Ensure this field exists
            })

        all_results[coin] = pd.DataFrame(results)

    stop_loss_events = {coin: pd.DataFrame({"Time": events}) for coin, events in stop_loss_events.items() if events}



    return all_results, stop_loss_events




import sys

def calculate_pnl(results, initial_capital):
    """Calculates PNL and cumulative capital."""
    pnl = []
    cumulative_capital = []

    first_coin = next(iter(results.values()))
    for index in range(len(first_coin)):
        portfolio_value = 0
        for coin_result in results.values():
            try:
              portfolio_value += coin_result.loc[index, "Total Portfolio Value"]
            except KeyError as e:
              display(coin_result)
              display(first_coin)
              display(index)
              sys.exit(1)
              break

        pnl.append(portfolio_value - initial_capital)
        cumulative_capital.append(portfolio_value)

    return pd.Series(pnl), pd.Series(cumulative_capital)

def calculate_pnl_2(results, initial_capital):
    """Calculates PNL and cumulative capital."""
    pnl = []
    cumulative_capital = []

    # Any coin works, so let's just pick the first coin’s DataFrame
    first_coin_df = next(iter(results.values()))

    for index in range(len(first_coin_df)):
        # Grab the total portfolio value from the first coin only
        portfolio_value = first_coin_df.loc[index, "Total Portfolio Value"]
        current_pnl = portfolio_value - initial_capital

        pnl.append(current_pnl)
        cumulative_capital.append(portfolio_value)

    return pd.Series(pnl), pd.Series(cumulative_capital)

    # coin_names = list(dfs.keys())
    # stop_loss_pct = .1
    # starting_capital = 100000
    # portfolio_size = len(coin_names)
    #
    # results = calculate_investment_portfolio(dfs, price_dfs, starting_capital, portfolio_size, coin_names)
    #
    # pnl, cumulative_capital = calculate_pnl(results, starting_capital)
    #
    # # Plot PNL and Cumulative Capital
    # plt.figure(figsize=(10, 5))
    # plt.plot(pnl)
    # plt.title("Profit and Loss (PNL) Over Time")
    # plt.xlabel("Time (Trades)")
    # plt.ylabel("PNL")
    # plt.grid(True)
    # plt.show()
import matplotlib.pyplot as plt

import seaborn as sns
def generate_seaborn_plots(results, pnl, cumulative_capital, price_dfs, stop_loss_events=None):
    """
    Generate enhanced visualizations using Seaborn for portfolio analysis.

    Parameters:
    results (dict): Dictionary of DataFrames with portfolio results for each coin
    pnl (pd.Series): Series containing profit and loss values
    cumulative_capital (pd.Series): Series containing cumulative capital values
    price_dfs (dict): Dictionary of DataFrames containing price data for each coin
    stop_loss_events (dict): Dictionary of DataFrames containing stop loss events for each coin
    """
    # Set the Seaborn theme and color palette
    sns.set_theme(style="darkgrid")

    # 1. Overall PNL and Cumulative Capital Plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # PNL plot with color gradient based on positive/negative values
    sns.lineplot(x=range(len(pnl)), y=pnl, ax=ax1, color='blue', linewidth=2)
    ax1.fill_between(range(len(pnl)), 0, pnl, where=(pnl > 0), color='green', alpha=0.3)
    ax1.fill_between(range(len(pnl)), 0, pnl, where=(pnl < 0), color='red', alpha=0.3)
    ax1.set_title("Profit and Loss (PNL) Over Time", fontsize=16)
    ax1.set_ylabel("PNL ($)", fontsize=12)
    ax1.axhline(y=0, color='black', linestyle='-', alpha=0.3)

    # Cumulative Capital plot
    sns.lineplot(x=range(len(cumulative_capital)), y=cumulative_capital, ax=ax2, color='purple', linewidth=2)
    ax2.set_title("Cumulative Capital Over Time", fontsize=16)
    ax2.set_xlabel("Time (Trades)", fontsize=12)
    ax2.set_ylabel("Capital ($)", fontsize=12)

    plt.tight_layout()
    plt.show()

    # 2. Coin Value Distribution Plot
    plt.figure(figsize=(12, 6))
    coin_data = []

    for coin, result_df in results.items():
        for idx, row in result_df.iterrows():
            coin_data.append({
                'Coin': coin,
                'Time': idx,
                'Value': row['Coin Value']
            })

    coin_df = pd.DataFrame(coin_data)

    # Check if we have time as datetime index
    use_pivot = False
    try:
        # Try to create a pivot table with time as index
        pivot_df = coin_df.pivot_table(index='Time', columns='Coin', values='Value', aggfunc='sum')
        use_pivot = True
    except:
        # If that fails, we'll use a different approach
        use_pivot = False

    if use_pivot:
        # Stack plot showing coin value distribution over time
        ax = pivot_df.plot.area(figsize=(12, 6), alpha=0.7, cmap='viridis')
        plt.title('Distribution of Portfolio Value Across Coins Over Time', fontsize=16)
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Value ($)', fontsize=12)
        plt.legend(title='Coins', bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        # Alternative approach using lineplot for each coin
        for coin in coin_df['Coin'].unique():
            coin_subset = coin_df[coin_df['Coin'] == coin]
            plt.plot(range(len(coin_subset)), coin_subset['Value'], label=coin)

        plt.title('Coin Values Over Time', fontsize=16)
        plt.xlabel('Time (Trades)', fontsize=12)
        plt.ylabel('Value ($)', fontsize=12)
        plt.legend(title='Coins')

    plt.tight_layout()
    plt.show()

    # # 3. Trade Signal Visualization
    # n_coins = len(results)
    # fig, axes = plt.subplots(n_coins, 1, figsize=(14, n_coins * 4), sharex=False)
    #
    # # Handle case with only one coin
    # if n_coins == 1:
    #     axes = [axes]
    #
    # for i, (coin, result_df) in enumerate(results.items()):
    #     ax = axes[i]
    #
    #     # Plot holdings over time
    #     sns.lineplot(x=range(len(result_df)), y=result_df['Coin Value'], ax=ax, color='blue', label='Coin Value')
    #
    #     # Mark buy and sell points
    #     buy_points = result_df[result_df['Prediction'] == 1].index
    #     sell_points = result_df[result_df['Prediction'] == 2].index
    #
    #     # Convert to position numbers if needed
    #     try:
    #         buy_positions = [result_df.index.get_loc(idx) for idx in buy_points]
    #         sell_positions = [result_df.index.get_loc(idx) for idx in sell_points]
    #     except:
    #         buy_positions = list(range(len(buy_points)))
    #         sell_positions = list(range(len(sell_points)))
    #
    #     for pos in buy_positions:
    #         ax.axvline(x=pos, color='green', alpha=0.3, linestyle='--')
    #
    #     for pos in sell_positions:
    #         ax.axvline(x=pos, color='red', alpha=0.3, linestyle='--')
    #
    #     # Mark stop loss events if available
    #     if stop_loss_events and coin in stop_loss_events:
    #         stop_loss_points = stop_loss_events[coin]["Time"].tolist()
    #         stop_loss_positions = [result_df.index.get_loc(idx) for idx in stop_loss_points if idx in result_df.index]
    #         for pos in stop_loss_positions:
    #             ax.axvline(x=pos, color='purple', linewidth=2, linestyle='-')
    #
    #     ax.set_title(f"{coin} Value and Trade Signals", fontsize=14)
    #     ax.set_ylabel("Value ($)", fontsize=12)
    #     ax.set_xlabel("Time (Trades)", fontsize=12)
    #
    #     # Create a twin axis for displaying trade actions
    #     ax2 = ax.twinx()
    #
    #     # Create markers for trade types
    #     trade_markers = []
    #     for idx, row in result_df.iterrows():
    #         pos = result_df.index.get_loc(idx)
    #
    #         if row['Stop Loss Triggered']:
    #             ax2.scatter(pos, 0, color='purple', s=100, marker='x', label='Stop Loss')
    #         elif row['Prediction'] == 1:  # Buy
    #             ax2.scatter(pos, 0, color='green', s=80, marker='^', label='Buy')
    #         elif row['Prediction'] == 2:  # Sell
    #             ax2.scatter(pos, 0, color='red', s=80, marker='v', label='Sell')
    #
    #     # Remove duplicate legend entries
    #     handles, labels = ax2.get_legend_handles_labels()
    #     by_label = dict(zip(labels, handles))
    #     ax2.legend(by_label.values(), by_label.keys(), loc='upper right')
    #
    #     # Hide y-axis for the twin axis
    #     ax2.set_yticks([])
    #
    # plt.tight_layout()
    # plt.show()
def display_end_pnl_by_coin(results, starting_capital, plot = True):
    """
    Calculate and display the final PNL for each coin in the portfolio.

    Parameters:
    results (dict): Dictionary of DataFrames with portfolio results for each coin
    starting_capital (float): Initial investment amount

    Returns:
    pd.DataFrame: DataFrame containing end PNL for each coin
    """
    # Calculate initial investment per coin
    coin_names = list(results.keys())
    initial_investment_per_coin = starting_capital / len(coin_names)

    # Prepare data for final PNL calculation
    end_pnl_data = []

    for coin, result_df in results.items():
        # Get the last row for final values
        final_row = result_df.iloc[-1]

        # Calculate end value (cash + holdings) ensuring numeric conversion
        end_value = float(final_row['Cash Holdings']) + float(final_row['Coin Value'])

        # Calculate PNL
        coin_pnl = end_value - initial_investment_per_coin
        coin_pnl_percentage = (coin_pnl / initial_investment_per_coin) * 100

        end_pnl_data.append({
            'Coin': coin,
            'Initial Investment': initial_investment_per_coin,
            'Final Value': end_value,
            'PNL': coin_pnl,
            'PNL (%)': coin_pnl_percentage
        })

    # Create DataFrame and sort by PNL
    end_pnl_df = pd.DataFrame(end_pnl_data)
    end_pnl_df = end_pnl_df.sort_values('PNL', ascending=False)

    # Calculate total portfolio PNL
    total_end_value = sum(float(row['Final Value']) for row in end_pnl_data)
    total_pnl = total_end_value - starting_capital
    total_pnl_percentage = (total_pnl / starting_capital) * 100

    # Add total row
    end_pnl_df.loc[len(end_pnl_df)] = {
        'Coin': 'TOTAL',
        'Initial Investment': starting_capital,
        'Final Value': total_end_value,
        'PNL': total_pnl,
        'PNL (%)': total_pnl_percentage
    }

    # Format the DataFrame for display
    formatted_df = end_pnl_df.copy()
    formatted_df['Initial Investment'] = formatted_df['Initial Investment'].map('${:,.2f}'.format)
    formatted_df['Final Value'] = formatted_df['Final Value'].map('${:,.2f}'.format)
    formatted_df['PNL'] = formatted_df['PNL'].map('${:,.2f}'.format)
    formatted_df['PNL (%)'] = formatted_df['PNL (%)'].map('{:,.2f}%'.format)

    if plot:

        """
        Create a bar chart visualization of the end PNL for each coin.
        """
        # Create a copy without the TOTAL row for plotting
        plot_df = end_pnl_df[end_pnl_df['Coin'] != 'TOTAL'].copy()

        # Convert PNL strings back to numeric for plotting
        plot_df['PNL_numeric'] = end_pnl_df['PNL'].astype(float)

        # Set up the plot
        plt.figure(figsize=(12, 6))

        # Create bars with different colors based on positive/negative PNL
        colors = ['green' if x >= 0 else 'red' for x in plot_df['PNL_numeric']]

        # Create the bar chart
        ax = sns.barplot(x='Coin', y='PNL_numeric', data=plot_df, palette=colors)

        # Add value labels on top of each bar
        for i, p in enumerate(ax.patches):
            height = p.get_height()
            if height < 0:
                ax.text(p.get_x() + p.get_width()/2., height - 1000,
                       plot_df['PNL'].iloc[i],
                       ha="center", va="top", color='white')
            else:
                ax.text(p.get_x() + p.get_width()/2., height + 100,
                       plot_df['PNL'].iloc[i],
                       ha="center", va="bottom")

        # Customize the plot
        plt.title('End PNL by Coin', fontsize=16)
        plt.xlabel('Coin', fontsize=12)
        plt.ylabel('Profit/Loss ($)', fontsize=12)
        plt.axhline(y=0, color='black', linestyle='-')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.show()

        return formatted_df