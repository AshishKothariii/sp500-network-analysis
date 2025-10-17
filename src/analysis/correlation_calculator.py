import pandas as pd
import numpy as np
import logging
import heapq

class CorrelationCalculator:
    def __init__(self, returns_df):
        self.returns_df = returns_df
        self.logger = logging.getLogger(__name__)

    def calculate_correlation_matrix(self, n_days):
        """
        Calculate correlation matrix for the last n trading days

        Parameters:
        n_days (int): Number of most recent trading days to use

        Returns:
        pd.DataFrame: Correlation matrix of shape (num_tickers, num_tickers)
        """
        if n_days > len(self.returns_df):
            raise ValueError(f"Requested {n_days} days but only {len(self.returns_df)} available")

        # Get last n days of returns data
        last_n_returns = self.returns_df.tail(n_days)

        # Calculate correlation matrix
        correlation_matrix = last_n_returns.corr()

        self.logger.info(f"Calculated correlation matrix for last {n_days} days")
        self.logger.info(f"Correlation matrix shape: {correlation_matrix.shape}")
        self.logger.info(f"Using returns from index {len(self.returns_df)-n_days} to {len(self.returns_df)-1}")

        return correlation_matrix

    def get_topk_similar_stocks(self, correlation_matrix, ticker, k=5):
        """
        Find top k most similar stocks to the given ticker based on correlation

        Parameters:
        correlation_matrix (pd.DataFrame): Correlation matrix
        ticker (str): Target stock ticker
        k (int): Number of similar stocks to return

        Returns:
        list: List of tuples (ticker, correlation_score) sorted by similarity
        """
        if ticker not in correlation_matrix.columns:
            raise ValueError(f"Ticker {ticker} not found in correlation matrix")

        # Get correlations with the target ticker
        correlations = correlation_matrix[ticker]
        
        # Remove the ticker itself (correlation = 1.0)
        correlations = correlations.drop(ticker)
        
        # Get top k most correlated stocks
        topk_similar = correlations.nlargest(k)
        
        self.logger.info(f"Top {k} similar stocks to {ticker}:")
        for similar_ticker, corr in topk_similar.items():
            self.logger.info(f"  {similar_ticker}: {corr:.3f}")
            
        return list(topk_similar.items())

    def get_leastk_similar_stocks(self, correlation_matrix, ticker, k=5):
        """
        Find top k least similar (most negatively correlated) stocks to the given ticker

        Parameters:
        correlation_matrix (pd.DataFrame): Correlation matrix
        ticker (str): Target stock ticker
        k (int): Number of dissimilar stocks to return

        Returns:
        list: List of tuples (ticker, correlation_score) sorted by dissimilarity
        """
        if ticker not in correlation_matrix.columns:
            raise ValueError(f"Ticker {ticker} not found in correlation matrix")

        # Get correlations with the target ticker
        correlations = correlation_matrix[ticker]
        
        # Remove the ticker itself (correlation = 1.0)
        correlations = correlations.drop(ticker)
        
        # Get top k least correlated (most negative) stocks
        leastk_similar = correlations.nsmallest(k)
        
        self.logger.info(f"Top {k} least similar stocks to {ticker}:")
        for similar_ticker, corr in leastk_similar.items():
            self.logger.info(f"  {similar_ticker}: {corr:.3f}")
            
        return list(leastk_similar.items())

    def get_similar_stocks_threshold(self, correlation_matrix, ticker, threshold=0.7):
        """
        Find all stocks with correlation above a certain threshold

        Parameters:
        correlation_matrix (pd.DataFrame): Correlation matrix
        ticker (str): Target stock ticker
        threshold (float): Correlation threshold (0.0 to 1.0)

        Returns:
        list: List of tuples (ticker, correlation_score) above threshold
        """
        if ticker not in correlation_matrix.columns:
            raise ValueError(f"Ticker {ticker} not found in correlation matrix")

        correlations = correlation_matrix[ticker].drop(ticker)
        similar_stocks = correlations[correlations >= threshold].sort_values(ascending=False)
        
        self.logger.info(f"Stocks with correlation >= {threshold} to {ticker}: {len(similar_stocks)} found")
        return list(similar_stocks.items())

    def get_dissimilar_stocks_threshold(self, correlation_matrix, ticker, threshold=-0.3):
        """
        Find all stocks with correlation below a certain threshold (for diversification)

        Parameters:
        correlation_matrix (pd.DataFrame): Correlation matrix
        ticker (str): Target stock ticker
        threshold (float): Correlation threshold (-1.0 to 0.0)

        Returns:
        list: List of tuples (ticker, correlation_score) below threshold
        """
        if ticker not in correlation_matrix.columns:
            raise ValueError(f"Ticker {ticker} not found in correlation matrix")

        correlations = correlation_matrix[ticker].drop(ticker)
        dissimilar_stocks = correlations[correlations <= threshold].sort_values()
        
        self.logger.info(f"Stocks with correlation <= {threshold} to {ticker}: {len(dissimilar_stocks)} found")
        return list(dissimilar_stocks.items())