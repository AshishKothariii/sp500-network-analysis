import logging
import pandas as pd
from database.connection_manager import ConnectionManager

class AssetPricesManager:
    def __init__(self, ordered_tickers):  # No conn parameter needed!
        self.conn = ConnectionManager.get_connection()  # Get connection automatically
        self.ordered_tickers = ordered_tickers
        self.logger = logging.getLogger(__name__)

    def _table_exists(self, table_name):
        """Check if table exists in database"""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = pd.read_sql_query(query, self.conn, params=[table_name])
        return len(result) > 0

    def _get_last_update_date(self):
        """Get the last date from asset_prices table"""
        if not self._table_exists('asset_prices'):
            return None

        query = "SELECT MAX(date) as last_date FROM asset_prices"
        result = pd.read_sql_query(query, self.conn)
        return result['last_date'].iloc[0] if result['last_date'].iloc[0] is not None else None

    def fill_asset_prices(self, target_date=None):
        """Fill asset_prices table with last 365 days data"""
        if target_date is None:
            target_date_query = "SELECT MAX(date) as last_date FROM stock_prices"
            target_date = pd.read_sql_query(target_date_query, self.conn)['last_date'].iloc[0]

        self.logger.info(f"Target date: {target_date}")

        dates_query = """
        WITH RankedDates AS (
            SELECT DISTINCT date,
                   ROW_NUMBER() OVER (ORDER BY date DESC) as rn
            FROM stock_prices
            WHERE date <= ?
        )
        SELECT date
        FROM RankedDates
        WHERE rn <= 365
        ORDER BY date ASC
        """
        dates = pd.read_sql_query(dates_query, self.conn, params=[target_date])
        dates_list = dates['date'].tolist()

        self.logger.info(f"Found {len(dates_list)} trading days")
        self.logger.info(f"Date range: {dates_list[0]} to {dates_list[-1]}")

        # Create table with proper columns: date, AAPL, MSFT, GOOGL, etc.
        column_definitions = ["date TEXT PRIMARY KEY"]
        for ticker in self.ordered_tickers:
            column_definitions.append(f'"{ticker}" REAL DEFAULT 0')

        create_table_sql = f"CREATE TABLE asset_prices ({', '.join(column_definitions)})"
        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)

        # Get close prices for all tickers and dates
        prices_query = """
        WITH Last365Dates AS (
            SELECT DISTINCT date
            FROM stock_prices
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT 365
        )
        SELECT sp.date, sp.ticker, sp.close
        FROM stock_prices sp
        WHERE sp.date IN (SELECT date FROM Last365Dates)
        AND sp.ticker IN ({})
        ORDER BY sp.date ASC, sp.ticker
        """.format(','.join(['?' for _ in self.ordered_tickers]))

        # Get the data
        params = [target_date] + self.ordered_tickers
        prices_data = pd.read_sql_query(prices_query, self.conn, params=params)

        # Pivot the data to wide format
        pivot_data = prices_data.pivot(index='date', columns='ticker', values='close')
        pivot_data = pivot_data.fillna(0)

        # Ensure we have all tickers in the right order
        for ticker in self.ordered_tickers:
            if ticker not in pivot_data.columns:
                pivot_data[ticker] = 0

        # Reorder columns to match ordered_tickers
        pivot_data = pivot_data[self.ordered_tickers]

        # Reset index and insert into asset_prices table
        pivot_data = pivot_data.reset_index()

        # Insert each row into the table
        cursor = self.conn.cursor()
        for _, row in pivot_data.iterrows():
            date = row['date']
            values = [date] + [row[ticker] for ticker in self.ordered_tickers]
            placeholders = ','.join(['?'] * len(values))
            insert_sql = f"INSERT INTO asset_prices VALUES ({placeholders})"
            cursor.execute(insert_sql, values)

        self.conn.commit()
        self.logger.info(f"Filled asset_prices table with {len(pivot_data)} rows")

    def update_asset_prices(self, target_date=None):
        """Update asset_prices table from last update date to target_date"""
        if target_date is None:
            target_date_query = "SELECT MAX(date) as last_date FROM stock_prices"
            target_date = pd.read_sql_query(target_date_query, self.conn)['last_date'].iloc[0]

        # Check if asset_prices table exists
        if not self._table_exists('asset_prices'):
            self.logger.error("asset_prices table doesn't exist. Run fill_asset_prices() first.")
            return

        # Get last update date from asset_prices
        last_update_date = self._get_last_update_date()
        if last_update_date is None:
            self.logger.error("No data found in asset_prices table. Run fill_asset_prices() first.")
            return

        self.logger.info(f"Last update date: {last_update_date}")
        self.logger.info(f"Target date: {target_date}")

        # If already up to date
        if last_update_date >= target_date:
            self.logger.info("asset_prices table is already up to date")
            return

        # Get dates to update (from last_update_date + 1 day to target_date)
        dates_query = """
        SELECT DISTINCT date
        FROM stock_prices
        WHERE date > ? AND date <= ?
        ORDER BY date ASC
        """
        dates_to_update = pd.read_sql_query(dates_query, self.conn, params=[last_update_date, target_date])
        dates_list = dates_to_update['date'].tolist()

        if not dates_list:
            self.logger.info("No new dates to update")
            return

        self.logger.info(f"Updating {len(dates_list)} dates: {dates_list[0]} to {dates_list[-1]}")

        # Get close prices for all tickers and new dates
        prices_query = """
        SELECT sp.date, sp.ticker, sp.close
        FROM stock_prices sp
        WHERE sp.date IN ({})
        AND sp.ticker IN ({})
        ORDER BY sp.date ASC, sp.ticker
        """.format(','.join(['?' for _ in dates_list]), ','.join(['?' for _ in self.ordered_tickers]))

        # Get the data
        params = dates_list + self.ordered_tickers
        prices_data = pd.read_sql_query(prices_query, self.conn, params=params)

        # Pivot the data to wide format
        pivot_data = prices_data.pivot(index='date', columns='ticker', values='close')
        pivot_data = pivot_data.fillna(0)

        # Ensure we have all tickers in the right order
        for ticker in self.ordered_tickers:
            if ticker not in pivot_data.columns:
                pivot_data[ticker] = 0

        # Reorder columns to match ordered_tickers
        pivot_data = pivot_data[self.ordered_tickers]

        # Reset index and insert into asset_prices table
        pivot_data = pivot_data.reset_index()

        # Insert each row into the table
        cursor = self.conn.cursor()
        for _, row in pivot_data.iterrows():
            date = row['date']
            values = [date] + [row[ticker] for ticker in self.ordered_tickers]
            placeholders = ','.join(['?'] * len(values))
            insert_sql = f"INSERT INTO asset_prices VALUES ({placeholders})"
            cursor.execute(insert_sql, values)

        self.conn.commit()
        self.logger.info(f"Updated asset_prices table with {len(pivot_data)} new rows")