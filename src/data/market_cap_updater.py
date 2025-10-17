import logging
import yfinance as yf
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
from database.connection_manager import ConnectionManager

class MarketCapUpdater:
    def __init__(self):  # No conn parameter needed!
        self.conn = ConnectionManager.get_connection()  # Get connection automatically
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger(__name__)

    def get_market_cap(self, ticker):
        """Get market cap for a single ticker using yfinance"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            market_cap = info.get('marketCap')
            return market_cap
        except Exception as e:
            self.logger.error(f"Error getting market cap for {ticker}: {e}")
            return None

    def update_market_cap(self, ticker, market_cap, date):
        """Update market cap for a single ticker in market_caps table for specific date"""
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO market_caps (ticker, date, market_cap) VALUES (?, ?, ?)",
                (ticker, date, market_cap)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error updating market cap for {ticker} on {date}: {e}")
            return False

    def get_last_updated_date(self):
        """Get the most recent date from market_caps table"""
        try:
            self.cursor.execute("SELECT MAX(date) FROM market_caps")
            result = self.cursor.fetchone()
            return result[0] if result[0] else None
        except Exception as e:
            self.logger.error(f"Error getting last updated date: {e}")
            return None

    def is_market_closed_today(self):
        """Check if US market is closed for the day (after 5:00 PM EST)"""
        now = datetime.now()
        est_time = now - timedelta(hours=5)  # Convert to EST
        
        # Check if it's after 5:00 PM EST
        if est_time.hour >= 17:
            return True
        
        # Check if it's weekend
        if est_time.weekday() >= 5:  # Saturday or Sunday
            return True
            
        return False

    def get_trading_dates_since_last_update(self, end_date=None):
        """Get list of trading dates since last update until end_date"""
        last_date = self.get_last_updated_date()
        
        if last_date:
            # Convert string date to datetime
            last_date_dt = datetime.strptime(last_date, '%Y-%m-%d')
            start_date = last_date_dt + timedelta(days=1)
        else:
            # If no previous data, start from 30 days ago
            start_date = datetime.now() - timedelta(days=30)
        
        # Use provided end_date or current date
        if end_date:
            if isinstance(end_date, str):
                end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_date_dt = end_date
        else:
            end_date_dt = datetime.now()
        
        # Get trading days using yfinance (using SPY as reference)
        try:
            spy = yf.download('SPY', start=start_date.strftime('%Y-%m-%d'), 
                            end=end_date_dt.strftime('%Y-%m-%d'), progress=False)
            trading_dates = [date.strftime('%Y-%m-%d') for date in spy.index]
            return trading_dates
        except Exception as e:
            self.logger.error(f"Error getting trading dates: {e}")
            # Fallback: generate business days
            date_range = pd.bdate_range(start=start_date, end=end_date_dt)
            return [date.strftime('%Y-%m-%d') for date in date_range]

    def update_market_caps(self, tickers, end_date=None, delay=1):
        """Update market caps for new trading days since last update until end_date
        
        Parameters:
        tickers (list): List of ticker symbols to update
        end_date (str/datetime, optional): Update market caps until this date (format: 'YYYY-MM-DD')
        delay (int): Delay between API calls to avoid rate limiting
        """
        if not tickers:
            self.logger.warning("No tickers provided to update")
            return

        # Get dates that need updating
        dates_to_update = self.get_trading_dates_since_last_update(end_date)
        
        if not dates_to_update:
            self.logger.info("No new trading days to update")
            return

        self.logger.info(f"Updating market caps for {len(dates_to_update)} trading days: {dates_to_update}")

        updated_count = 0
        failed_count = 0

        # For each date, update market caps for all tickers
        for date in dates_to_update:
            self.logger.info(f"Updating market caps for date: {date}")
            
            date_updated = 0
            date_failed = 0
            
            for i, ticker in enumerate(tickers, 1):
                self.logger.info(f"Processing {i}/{len(tickers)}: {ticker} for {date}")

                market_cap = self.get_market_cap(ticker)

                if market_cap:
                    success = self.update_market_cap(ticker, market_cap, date)
                    if success:
                        self.logger.info(f"Updated {ticker} on {date}: Market Cap = {market_cap:,.0f}")
                        updated_count += 1
                        date_updated += 1
                    else:
                        self.logger.error(f"Failed to update {ticker} on {date} in database")
                        failed_count += 1
                        date_failed += 1
                else:
                    self.logger.warning(f"No market cap data for {ticker} on {date}")
                    failed_count += 1
                    date_failed += 1

                # Delay to avoid rate limiting
                sleep(delay)
            
            self.logger.info(f"Date {date} completed: {date_updated} successful, {date_failed} failed")

        # Commit all changes
        self.conn.commit()
        self.logger.info(f"Update completed: {updated_count} successful, {failed_count} failed across {len(dates_to_update)} dates")

    def create_market_caps_table(self):
        """Create market_caps table if it doesn't exist"""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_caps (
                    ticker TEXT,
                    date TEXT,
                    market_cap REAL,
                    PRIMARY KEY (ticker, date)
                )
            """)
            self.conn.commit()
            self.logger.info("market_caps table created or already exists")
        except Exception as e:
            self.logger.error(f"Error creating market_caps table: {e}")