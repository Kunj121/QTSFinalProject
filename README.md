# Multilayer Perceptron Trading Strategy on Proof of Stake Cryptocurrencies

## Overview
This research explores using a Multi-Layer Perceptron (MLP) model to generate buy, hold, and sell signals for Proof of Stake cryptocurrencies on an hourly timeframe. The model integrates on-chain data, staking metrics, and technical indicators to predict market movements across different market phases.

## Data
The study analyzes five cryptocurrencies:
- Avalanche (AVAX)
- Polygon (MATIC)
- Solana (SOL)
- Near (NEAR)
- Polkadot (DOT)

Data sources include:
- Hourly price data from Binance via CoinAPI
- On-chain analytics from Dune API
- Staking data from StakingRewards
- Date range: 2021-2025

## Methodology

### PELT (Pruned Exact Linear Time) Algorithm
- Detects structural changes in price trends
- Normalizes data within each detected segment
- Reduces noise and improves model robustness

### Technical Indicators
- Bollinger Bands (24-hour lookback)
- Exponential Moving Averages (20/50-period)
- Relative Strength Index
- Time-based features (hour, day, month)

### Signal Generation
- Price difference threshold at half the standard deviation
- Buy: When price difference exceeds positive threshold
- Sell: When price difference falls below negative threshold
- Hold: When price difference is within thresholds

### Neural Network Architecture
- Input layer matching feature count
- Two hidden layers (64 and 32 neurons) with LeakyReLU activation
- Output layer with softmax activation for 3-class prediction
- Adam optimizer with 50 epochs and batch size of 32

## Portfolio Allocation
- Initial capital: $100,000 (no leverage)
- Transaction costs: 50 basis points per trade
- Equal 20% allocation per token at equilibrium
- Position sizing based on softmax probability strength
- Risk control via stoploss parameters

## Results
- Strategy outperforms Fama-French market factors (R² < 1%)
- Individual coin performance:
  - AVAX: 2.18 Sharpe ratio, negative BTC correlation
  - MATIC: 1.87 Sharpe ratio, negative BTC correlation
  - NEAR: 1.06 Sharpe ratio, 13% returns
  - DOT: 0.31 Sharpe ratio, low volatility
  - SOL: 1.73 Sharpe ratio, 45% returns
- Aggregate portfolio: 8% annualized returns, 31% Sharpe ratio, -0.22 max drawdown

## Limitations & Future Work
- Data inconsistencies across cryptocurrencies
- Single model per coin rather than stacked approach
- Potential for deeper/wider MLP architecture
- Classification approach limits magnitude prediction
- PELT segments may change retroactively
- No leverage utilized

## Authors
Togay Atmaca, Jordan Cassella, Matthew Haimes, Kunj Shah, Yan Song Zhao

*This research was partially funded through Robinhood trivia event winnings and support from the Financial Mathematics program.*
