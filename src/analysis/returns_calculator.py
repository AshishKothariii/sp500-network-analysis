import pandas as pd
import logging
from database.connection_manager import ConnectionManager

class ReturnsCalculator:
    def __init__(self):  # No conn parameter needed!
        self.conn = ConnectionManager.get_connection()  # Get connection automatically
        self.returns_df = None
        self.index_date_dict = {}
        self.logger = logging.getLogger(__name__)

    def calculate_returns(self):
        """Calculate returns from oldest to newest date in asset_prices - OPTIMIZED"""

        # Get ALL data from asset_prices in chronological order
        query = "SELECT * FROM asset_prices ORDER BY date ASC"
        price_data = pd.read_sql_query(query, self.conn)

        # Create index_date dictionary
        for i, date in enumerate(price_data['date']):
            self.index_date_dict[i] = date

        # Use pandas vectorized operations for speed
        price_values = price_data.drop('date', axis=1)

        # Calculate returns using pandas pct_change (much faster)
        # pct_change calculates (current - previous) / previous
        returns_values = price_values.pct_change() * 100

        # Remove first row (NaN) and keep the rest
        self.returns_df = returns_values.iloc[1:].reset_index(drop=True)

        # Fill NaN values with 0 (for cases where previous price was 0)
        self.returns_df = self.returns_df.fillna(0)

        self.logger.info(f"Calculated returns for {len(self.returns_df)} days")
        self.logger.info(f"Returns shape: {self.returns_df.shape}")
        self.logger.info(f"Date range: {price_data['date'].iloc[0]} to {price_data['date'].iloc[-1]}")

        return self.returns_df

    def get_index_date_mapping(self):
        """Return the index to date mapping"""
        self.logger.debug(f"Returning index-date mapping with {len(self.index_date_dict)} entries")
        return self.index_date_dict

    def get_returns_sample(self, num_rows=3, num_tickers=5):
        """Get sample of returns data"""
        if self.returns_df is None:
            self.logger.error("No returns calculated yet")
            return None

        sample = self.returns_df.iloc[:num_rows, :num_tickers]
        self.logger.info(f"Returns sample (first {num_rows} rows, first {num_tickers} tickers):")
        self.logger.info(f"\n{sample}")

        # Show corresponding dates
        self.logger.info("Corresponding dates:")
        for i in range(min(num_rows, len(self.index_date_dict) - 1)):
            self.logger.info(f"  Row {i}: {self.index_date_dict[i]} → {self.index_date_dict[i+1]}")

        return sample