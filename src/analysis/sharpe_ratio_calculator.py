import logging
import numpy as np
import pandas as pd
from typing import List

class SharpeRatioCalculator:
    def __init__(self, returns_df: pd.DataFrame, ordered_tickers: List[str]):
        self.returns_df = returns_df
        self.ordered_tickers = ordered_tickers
        self.logger = logging.getLogger(__name__)

    def calculate_sharpe_ratios(self,
                              risk_free_rate: float = 0.04,
                              annualization_factor: int = 252) -> List[float]:
        """
        Calculate Sharpe ratios for all stocks in ordered_tickers

        Returns Sharpe ratios in exact same order as ordered_tickers
        """
        self.logger.info(f"Calculating Sharpe ratios for {len(self.ordered_tickers)} stocks")

        sharpe_ratios = []
        daily_risk_free = risk_free_rate / annualization_factor

        for ticker in self.ordered_tickers:
            returns = self.returns_df[ticker].dropna()

            # Calculate annualized metrics
            annualized_return = returns.mean() * annualization_factor
            annualized_volatility = returns.std() * np.sqrt(annualization_factor)

            if annualized_volatility == 0:
                sharpe_ratio = 0.0
            else:
                sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility

            sharpe_ratios.append(sharpe_ratio)

        self.logger.info(f"Successfully calculated {len(sharpe_ratios)} Sharpe ratios")
        return sharpe_ratios

    def get_sharpe_ranking(self, risk_free_rate: float = 0.04) -> List[tuple]:
        """
        Get stocks ranked by Sharpe ratio (highest to lowest)

        Returns: List of (ticker, sharpe_ratio) tuples
        """
        sharpe_ratios = self.calculate_sharpe_ratios(risk_free_rate)

        # Combine with tickers and sort
        ranked_stocks = list(zip(self.ordered_tickers, sharpe_ratios))
        ranked_stocks.sort(key=lambda x: x[1], reverse=True)

        return ranked_stocks

    def get_top_sharpe_stocks(self, top_k: int = 10, risk_free_rate: float = 0.04) -> List[tuple]:
        """
        Get top K stocks by Sharpe ratio
        """
        ranked_stocks = self.get_sharpe_ranking(risk_free_rate)
        return ranked_stocks[:top_k]
