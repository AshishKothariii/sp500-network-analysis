import logging
import pandas as pd
import heapq
from database.connection_manager import ConnectionManager

class CompanyDataManager:
    def __init__(self):  # No conn parameter needed!
        self.conn = ConnectionManager.get_connection()  # Get connection automatically
        self.market_cap_dict = {}
        self.ticker_industry_dict = {}
        self.ticker_sector_dict = {}
        self.sector_tickers_dict = {}
        self.ticker_name_dict = {}
        self.ordered_tickers = []
        self.logger = logging.getLogger(__name__)
        self._load_data()

    def _load_data(self):
        """Load data once during initialization"""
        try:
            df = pd.read_sql_query("SELECT ticker, market_cap, sector, industry, name FROM company_info", self.conn)
            self.logger.info(f"Loaded {len(df)} companies from database")

            # Build all dictionaries
            for index, row in df.iterrows():
                ticker = row['ticker']

                # Market cap dictionary
                if pd.notna(row['market_cap']):
                    self.market_cap_dict[ticker] = row['market_cap']

                # Industry dictionary
                if pd.notna(row['industry']):
                    self.ticker_industry_dict[ticker] = row['industry']

                # Sector dictionary (ticker -> sector)
                if pd.notna(row['sector']):
                    self.ticker_sector_dict[ticker] = row['sector']

                # Sector dictionary (sector -> list of tickers)
                if pd.notna(row['sector']):
                    sector = row['sector']
                    if sector not in self.sector_tickers_dict:
                        self.sector_tickers_dict[sector] = []
                    self.sector_tickers_dict[sector].append(ticker)

                # Name dictionary
                if pd.notna(row['name']):
                    self.ticker_name_dict[ticker] = row['name']

            # Get the order from asset_prices table columns (excluding 'date')
            try:
                sample = pd.read_sql_query("SELECT * FROM asset_prices LIMIT 1", self.conn)
                asset_price_columns = [col for col in sample.columns if col != 'date']
                # Set ordered_tickers to match asset_prices column order
                self.ordered_tickers = asset_price_columns
                self.logger.info(f"Found {len(self.ordered_tickers)} ordered tickers from asset_prices table")
            except Exception as e:
                self.logger.warning(f"Could not load asset_prices table: {e}")
                # Fallback: order by market cap if asset_prices doesn't exist
                self.ordered_tickers = sorted(self.market_cap_dict.keys(), 
                                            key=lambda x: self.market_cap_dict.get(x, 0), 
                                            reverse=True)
                self.logger.info(f"Using market cap order: {len(self.ordered_tickers)} tickers")
            
            self.logger.info(f"Successfully built data structures: {len(self.market_cap_dict)} market caps, {len(self.ordered_tickers)} ordered tickers, {len(self.sector_tickers_dict)} sectors")

        except Exception as e:
            self.logger.error(f"Error loading data from database: {e}")
            raise

    def get_ordered_tickers(self):
        """Return tickers in the same order as asset_prices table columns"""
        return self.ordered_tickers

    def get_market_cap_dict(self):
        """Return dictionary {ticker: market_cap}"""
        self.logger.debug(f"Returning market cap dict with {len(self.market_cap_dict)} entries")
        return self.market_cap_dict

    def get_industry_dict(self):
        """Return dictionary {ticker: industry}"""
        self.logger.debug(f"Returning industry dict with {len(self.ticker_industry_dict)} entries")
        return self.ticker_industry_dict

    def get_sector_dict(self):
        """Return dictionary {ticker: sector}"""
        self.logger.debug(f"Returning sector dict with {len(self.ticker_sector_dict)} entries")
        return self.ticker_sector_dict

    def get_sector_tickers_dict(self):
        """Return dictionary {sector: [list of tickers]}"""
        self.logger.debug(f"Returning sector tickers dict with {len(self.sector_tickers_dict)} sectors")
        return self.sector_tickers_dict

    def get_ticker_name_dict(self):
        """Return dictionary {ticker: company_name}"""
        self.logger.debug(f"Returning ticker name dict with {len(self.ticker_name_dict)} entries")
        return self.ticker_name_dict