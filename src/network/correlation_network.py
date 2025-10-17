import pandas as pd
from typing import List, Dict, Set
import heapq
import networkx as nx
import matplotlib.pyplot as plt

class CorrelationNetworkGraph:
    def __init__(self, correlation_matrix: pd.DataFrame, ordered_tickers: List[str], threshold: float = 0.70):
        self.correlation_matrix = correlation_matrix
        self.ordered_tickers = ordered_tickers
        self.threshold = threshold
        self.graph = {}  # adjacency list: {source: {target: weight}}
        self._build_graph()

    def _build_graph(self):
        """Build directed graph where edges go from large-cap to small-cap"""
        self.graph = {ticker: {} for ticker in self.ordered_tickers}

        for i, ticker_i in enumerate(self.ordered_tickers):
            for j, ticker_j in enumerate(self.ordered_tickers):
                if i < j:  # ticker_i has higher market cap than ticker_j
                    corr = self.correlation_matrix.iloc[i, j]
                    if abs(corr) > self.threshold:
                        self.graph[ticker_i][ticker_j] = abs(corr)

    def get_similar_stocks(self, tickers: List[str], n: int = 10) -> Dict[str, List[Dict]]:
        """Get n most similar stocks for each ticker in the list"""
        results = {}

        for ticker in tickers:
            if ticker not in self.graph:
                results[ticker] = []
                continue

            # Priority queue: (-correlation_strength, stock) for max-heap behavior
            heap = []
            visited = set([ticker])

            # Add direct outgoing connections (stocks this ticker influences)
            for neighbor, weight in self.graph[ticker].items():
                if neighbor not in visited:
                    heapq.heappush(heap, (-weight, neighbor))
                    visited.add(neighbor)

            # Add direct incoming connections (stocks that influence this ticker)
            for source_ticker, neighbors in self.graph.items():
                if ticker in neighbors and source_ticker not in visited:
                    weight = neighbors[ticker]
                    heapq.heappush(heap, (-weight, source_ticker))
                    visited.add(source_ticker)

            # Get top n similar stocks for this ticker
            similar_stocks = []
            while heap and len(similar_stocks) < n:
                neg_weight, stock = heapq.heappop(heap)
                similar_stocks.append({
                    'ticker': stock,
                    'correlation_strength': -neg_weight  # Convert back to positive
                })

            results[ticker] = similar_stocks

        return results

    def get_common_similar_stocks(self, tickers: List[str], n: int = 10) -> List[Dict]:
        """Find stocks that are similar to ALL tickers in the input list"""
        if not tickers:
            return []

        # Get similar stocks for each ticker
        all_similar = self.get_similar_stocks(tickers, n=len(self.ordered_tickers))

        # Find intersection of similar stocks across all input tickers
        common_stocks = set()
        first_ticker = tickers[0]

        if all_similar[first_ticker]:
            # Start with stocks similar to the first ticker
            common_stocks = {stock['ticker'] for stock in all_similar[first_ticker]}

            # Find intersection with other tickers
            for ticker in tickers[1:]:
                if all_similar[ticker]:
                    ticker_similar = {stock['ticker'] for stock in all_similar[ticker]}
                    common_stocks = common_stocks.intersection(ticker_similar)

        # Calculate average correlation strength for common stocks
        common_with_strength = []
        for stock in common_stocks:
            total_strength = 0
            for ticker in tickers:
                # Find correlation strength for each input ticker
                strength = 0
                if stock in self.graph[ticker]:
                    strength = self.graph[ticker][stock]
                else:
                    for source, neighbors in self.graph.items():
                        if source == stock and ticker in neighbors:
                            strength = neighbors[ticker]
                            break
                total_strength += strength

            avg_strength = total_strength / len(tickers)
            common_with_strength.append({
                'ticker': stock,
                'avg_correlation_strength': avg_strength
            })

        # Return top n common stocks by average correlation strength
        return sorted(common_with_strength, key=lambda x: x['avg_correlation_strength'], reverse=True)[:n]

    def get_graph_info(self) -> Dict:
        """Get basic graph information"""
        total_edges = sum(len(neighbors) for neighbors in self.graph.values())
        return {
            'nodes': len(self.ordered_tickers),
            'edges': total_edges,
            'threshold': self.threshold
        }

   

