# 📈 Financial Analysis Platform

A comprehensive Python-based platform for stock market analysis, correlation networks, and portfolio insights. Built with modular architecture for extensible financial analytics.

## 🚀 Features

### 📊 Core Analysis
- **Market Data Management**: Automated data fetching and updates
- **Market Cap Updates**: Real-time market capitalization updates via yFinance
- **Returns Calculation**: Efficient vectorized return computations
- **Performance Analytics**: Momentum, risk-adjusted returns, and sector analysis
- **Correlation Analysis**: Stock relationship mapping and network graphs

### 🔍 Advanced Analytics
- **Top/Bottom Performers**: Heap-based efficient ranking algorithms
- **Sharpe Ratio Analysis**: Risk-adjusted performance metrics
- **Correlation Networks**: Directed graphs based on market cap and correlations
- **Multi-period Analysis**: Flexible timeframe performance tracking

### 📈 Visualization
- **Static Network Plots**: Matplotlib-based network visualizations
- **Interactive Charts**: Plotly-powered interactive network exploration
- **Minimal Layouts**: Clean, publication-ready visualizations


## 🏗️ Architecture
```
financial-analysis-platform/
├── src/
│ ├── data/ # Data management layer
│ │ ├── asset_prices_manager.py
│ │ ├── company_data_manager.py
│ │ └── market_cap_updater.py
│ ├── database/ # Database connectivity
│ │ └── connection_manager.py
│ ├── analysis/ # Financial analytics
│ │ ├── correlation_calculator.py
│ │ ├── returns_calculator.py
│ │ ├── sharpe_ratio_calculator.py
│ │ └── stock_analyzer.py
│ └── network/ # Network analysis & visualization
│ ├── correlation_network.py
│ └── visualizer.py
├── examples/
│ └── demo_notebook.ipynb # Complete analysis pipeline
└── requirements.txt 
```

## 🛠️ Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/financial-analysis-platform.git
   cd financial-analysis-platform
   
2. **Install Dependencies**
    ```bash
   pip install -r requirements.txt
3. **Setup Database**
    ```bash
   - Ensure your SQLite database is in the examples/ folder
   - Update connection path in connection_manager.py if needed
## 📖 Usage
   ### Quick Start
   Open the Jupyter notebook for a complete analysis pipeline:
```
# examples/demo_notebook.ipynb
from src.data.company_data_manager import CompanyDataManager
from src.analysis.returns_calculator import ReturnsCalculator
from src.analysis.stock_analyzer import StockAnalyzer

# Initialize managers
company_mgr = CompanyDataManager()
returns_calc = ReturnsCalculator()
returns_df = returns_calc.calculate_returns()
analyzer = StockAnalyzer(returns_df)

# Get market overview
tickers = company_mgr.get_ordered_tickers()
sectors = company_mgr.get_sector_dict()

# Performance analysis
top_stocks = analyzer.topk_stocks(30, 10)  # Top 10 over 30 days
```

### Key Analysis Examples
   Market Cap Update
```
# Update market caps to latest closing prices
from src.data.market_cap_updater import MarketCapUpdater

updater = MarketCapUpdater()
tickers = company_mgr.get_ordered_tickers()

# Update all market caps with 1-second delay between requests
updater.update_market_caps(tickers, delay=1)

# Verify updated market caps
updated_market_caps = company_mgr.get_market_cap_dict()
print(f"Updated market caps for {len(updated_market_caps)} companies")

```

   Market Overview
```# Sector distribution, market caps, company info
sector_ticker_dict = company_mgr.get_sector_dict()
market_cap_dict = company_mgr.get_market_cap_dict()
```
Performance Tracking

```
 #Momentum strategy
top_momentum = analyzer.topk_stocks(30, 10)
worst_performers = analyzer.leastk_stocks(30, 10)

# Multi-period analysis
performance = analyzer.analyze_multiple_periods("FDS", [10, 30, 50])
```
   Risk-Adjusted Returns
```
from src.analysis.sharpe_ratio_calculator import SharpeRatioCalculator

sharpe_calc = SharpeRatioCalculator(returns_df, tickers)
top_sharpe = sharpe_calc.get_top_sharpe_stocks(10)
```
Correlation Networks

```
from src.analysis.correlation_calculator import CorrelationCalculator
from src.network.correlation_network import CorrelationNetworkGraph
from src.network.visualizer import NetworkVisualizer

# Build correlation network
corr_calc = CorrelationCalculator(returns_df)
corr_matrix = corr_calc.calculate_correlation_matrix(45)
network = CorrelationNetworkGraph(corr_matrix, tickers, threshold=0.83)

# Visualize
visualizer = NetworkVisualizer(network)
visualizer.plot_networkx()
visualizer.plot_interactive()
```

## 📊 Sample Outputs
```
The platform generates:

- Market Overview: Sector distributions, market caps, company metadata

- Performance Metrics: Momentum rankings, Sharpe ratios, multi-period returns

- Correlation Matrices: Stock relationship analysis

- Network Visualizations:

- Static network plots (yellow nodes, black edges)

- Interactive Plotly charts

- Minimal clean layouts

```
## 🎯 Use Cases
- Portfolio Optimization: Identify diversification opportunities with current market caps

- Sector Analysis: Understand industry correlations with up-to-date valuations

- Risk Management: Monitor stock relationships with latest market data

- Investment Research: Systematic analysis with real-time market capitalization

- Academic Research: Financial network analysis with current valuation data

## 🔧 Configuration
- Database: SQLite with configurable connection paths

- Data Sources: yFinance integration for real-time market data and market caps

- Update Frequency: Configurable delays to avoid rate limiting

- Thresholds: Adjustable correlation thresholds (default: 0.70-0.83)

- Timeframes: Flexible analysis periods (10-365 days)


