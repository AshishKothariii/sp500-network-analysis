import heapq
import pandas as pd
import logging
from database.connection_manager import ConnectionManager

class StockAnalyzer:
    def __init__(self, returns_df):  # returns_df is needed for analysis methods
        self.conn = ConnectionManager.get_connection()
        self.returns_df = returns_df
        self.asset_prices_df = None
        self.logger = logging.getLogger(__name__)
        self._load_asset_prices()

    def _load_asset_prices(self):
        """Load the latest asset prices from the database"""
        query = "SELECT * FROM asset_prices ORDER BY date DESC LIMIT 1"
        self.asset_prices_df = pd.read_sql_query(query, self.conn)
        self.logger.debug("Loaded latest asset prices from database")

    def get_stock_price_history(self, ticker, n_days):
        """
        Get the last n days of prices for a specific stock from asset_prices table
    
        Parameters:
        ticker (str): Stock ticker symbol
        n_days (int): Number of most recent days to get prices for
    
        Returns:
        pd.DataFrame: DataFrame with columns ['date', 'price'] for the last n days
        """
        # Check if ticker exists in asset_prices table (independent of returns_df)
        check_query = "SELECT * FROM asset_prices LIMIT 1"
        try:
            sample = pd.read_sql_query(check_query, self.conn)
            
            if ticker not in sample.columns:
                self.logger.error(f"Ticker {ticker} not found in asset_prices table")
                return None
        except Exception as e:
            self.logger.error(f"Error checking asset_prices table: {e}")
            return None

        # Get the last n days of price data
        query = """
        SELECT date, {} as price 
        FROM asset_prices 
        ORDER BY date DESC 
        LIMIT ?
        """.format(f'"{ticker}"')
    
        price_data = pd.read_sql_query(query, self.conn, params=[n_days])
    
        if price_data.empty:
            self.logger.warning(f"No price data found for {ticker}")
            return None
    
        # Return in chronological order (oldest first)
        price_data = price_data.sort_values('date').reset_index(drop=True)
    
        self.logger.info(f"Retrieved {len(price_data)} days of price data for {ticker}")
        return price_data

    def get_stock_price(self, ticker):
        """
        Get the last updated price for a specific stock

        Parameters:
        ticker (str): Stock ticker symbol

        Returns:
        float: Last price of the stock, or None if not found
        """
        if self.asset_prices_df is None or self.asset_prices_df.empty:
            self.logger.error("No asset price data available")
            return None

        if ticker not in self.asset_prices_df.columns:
            self.logger.error(f"Ticker {ticker} not found in asset prices")
            return None

        price = self.asset_prices_df[ticker].iloc[0]
        self.logger.info(f"{ticker} last price: {price:.2f}")
        return price

    def topk_stocks(self, n_days, k_stocks):
        """
        Returns k stocks with highest increase over last n days using heaps
        Note: tail of returns_df represents latest dates

        Parameters:
        n_days (int): Number of most recent days to analyze
        k_stocks (int): Number of top stocks to return

        Returns:
        list: List of tuples (ticker, total_return) for top k stocks
        """
        if n_days > len(self.returns_df):
            raise ValueError(f"Requested {n_days} days but only {len(self.returns_df)} available")

        # Get last n days of returns (most recent at the tail)
        recent_returns = self.returns_df.tail(n_days)

        # Calculate total returns for each stock over n days
        total_returns = recent_returns.sum()

        # Use min-heap to get top k stocks efficiently
        min_heap = []

        for ticker, total_return in total_returns.items():
            if len(min_heap) < k_stocks:
                heapq.heappush(min_heap, (total_return, ticker))
            else:
                # If current return is greater than smallest in heap, replace it
                if total_return > min_heap[0][0]:
                    heapq.heappushpop(min_heap, (total_return, ticker))

        # Convert to sorted list (highest first)
        top_stocks = sorted(min_heap, reverse=True)

        self.logger.info(f"Top {k_stocks} stocks over last {n_days} days (most recent):")
        for i, (return_val, ticker) in enumerate(top_stocks, 1):
            self.logger.info(f"  {i}. {ticker}: {return_val:.2f}%")

        return top_stocks

    def leastk_stocks(self, n_days, k_stocks):
        """
        Returns k stocks with highest fall (lowest returns) over last n days using heaps
        Note: tail of returns_df represents latest dates

        Parameters:
        n_days (int): Number of most recent days to analyze
        k_stocks (int): Number of bottom stocks to return

        Returns:
        list: List of tuples (ticker, total_return) for bottom k stocks
        """
        if n_days > len(self.returns_df):
            raise ValueError(f"Requested {n_days} days but only {len(self.returns_df)} available")

        # Get last n days of returns (most recent at the tail)
        recent_returns = self.returns_df.tail(n_days)

        # Calculate total returns for each stock over n days
        total_returns = recent_returns.sum()

        # Use max-heap to get bottom k stocks efficiently
        max_heap = []

        for ticker, total_return in total_returns.items():
            # We use negative returns to convert max-heap to min-heap behavior
            if len(max_heap) < k_stocks:
                heapq.heappush(max_heap, (-total_return, ticker))
            else:
                # If current return is smaller (more negative) than largest negative in heap, replace it
                if total_return < -max_heap[0][0]:
                    heapq.heappushpop(max_heap, (-total_return, ticker))

        # Convert back to original values and sort (lowest first)
        bottom_stocks = [(-return_val, ticker) for return_val, ticker in max_heap]
        bottom_stocks.sort()  # Sort by return value (ascending)

        self.logger.info(f"Bottom {k_stocks} stocks over last {n_days} days (most recent):")
        for i, (return_val, ticker) in enumerate(bottom_stocks, 1):
            self.logger.info(f"  {i}. {ticker}: {return_val:.2f}%")

        return bottom_stocks

    def get_stock_performance(self, ticker, n_days):
        """
        Get performance of a specific stock over last n days
        Note: tail of returns_df represents latest dates

        Parameters:
        ticker (str): Stock ticker symbol
        n_days (int): Number of most recent days to analyze

        Returns:
        float: Total return over n days, or None if ticker not found
        """
        if n_days > len(self.returns_df):
            raise ValueError(f"Requested {n_days} days but only {len(self.returns_df)} available")

        if ticker not in self.returns_df.columns:
            self.logger.error(f"Ticker {ticker} not found in returns data")
            return None

        # Get last n days of returns for the specific stock (most recent at tail)
        recent_returns = self.returns_df[ticker].tail(n_days)
        total_return = recent_returns.sum()

        current_price = self.get_stock_price(ticker)

        self.logger.info(f"{ticker} performance over last {n_days} days (most recent):")
        self.logger.info(f"  Total Return: {total_return:.2f}%")
        if current_price:
            self.logger.info(f"  Current Price: ${current_price:.2f}")

        return total_return

    def get_recent_performance_range(self):
        """
        Get the date range of the most recent n days in returns_df

        Returns:
        tuple: (start_index, end_index, total_days_available)
        """
        total_days = len(self.returns_df)
        self.logger.info(f"Returns data available: {total_days} days")
        self.logger.info(f"   Index 0: Oldest data")
        self.logger.info(f"   Index {total_days-1}: Most recent data")
        return (0, total_days-1, total_days)

    def analyze_multiple_periods(self, ticker, periods=[7, 30, 90, 365]):
        """
        Analyze stock performance over multiple time periods

        Parameters:
        ticker (str): Stock ticker symbol
        periods (list): List of periods in days to analyze

        Returns:
        dict: Dictionary with period as key and return as value
        """
        results = {}

        self.logger.info(f"Multi-period analysis for {ticker}:")
        for period in periods:
            if period <= len(self.returns_df):
                return_val = self.get_stock_performance(ticker, period)
                results[period] = return_val
            else:
                self.logger.warning(f"{period} days: Not enough data (only {len(self.returns_df)} days available)")
                results[period] = None

        return results