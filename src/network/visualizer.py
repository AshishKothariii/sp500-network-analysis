import networkx as nx
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from typing import Dict, Set, List
import pandas as pd

class NetworkVisualizer:
    def __init__(self, correlation_graph):
        """
        Initialize with a CorrelationNetworkGraph instance
        
        Parameters:
        correlation_graph: Instance of CorrelationNetworkGraph
        """
        self.correlation_graph = correlation_graph
        self.G = None
        self.connected_nodes = set()
        self._build_connected_graph()
    
    def _build_connected_graph(self):
        """Build a graph containing only nodes with connections"""
        self.G = nx.DiGraph()
        self.connected_nodes = set()
        
        # Find all nodes with at least one connection
        for source, targets in self.correlation_graph.graph.items():
            if targets:  # Source has outgoing connections
                self.connected_nodes.add(source)
                self.connected_nodes.update(targets.keys())
        
        # Build graph only with connected nodes
        for source, targets in self.correlation_graph.graph.items():
            if source in self.connected_nodes:
                for target, weight in targets.items():
                    if target in self.connected_nodes:
                        self.G.add_edge(source, target, weight=weight)
        
        print(f"NetworkVisualizer: {len(self.G.nodes())} connected nodes, {len(self.G.edges())} edges")
    
    def plot_networkx(self, figsize=(14, 10), title="Stock Correlation Network"):
        """
        Plot using NetworkX and Matplotlib
        
        Parameters:
        figsize: Figure size tuple
        title: Plot title
        """
        if len(self.G.nodes()) == 0:
            print("No connected nodes to visualize")
            return
        
        plt.figure(figsize=figsize)
        pos = nx.spring_layout(self.G, k=1, iterations=50)
        
        # Draw YELLOW nodes
        nx.draw_networkx_nodes(self.G, pos, 
                              node_size=400, 
                              node_color='yellow',
                              edgecolors='black',
                              linewidths=1.5,
                              alpha=0.9)
        
        # Draw BLACK edges with thickness based on correlation strength
        edges = self.G.edges()
        weights = [self.G[u][v]['weight'] * 4 for u, v in edges]
        
        nx.draw_networkx_edges(self.G, pos, 
                              edge_color='black', 
                              width=weights, 
                              alpha=0.7, 
                              arrows=True, 
                              arrowsize=20,
                              arrowstyle='->')
        
        # Draw labels
        nx.draw_networkx_labels(self.G, pos, 
                               font_size=9, 
                               font_weight='bold',
                               font_color='darkred')
        
        plt.title(f"{title}\nThreshold: {self.correlation_graph.threshold} | Connected Nodes: {len(self.G.nodes())}", 
                  fontsize=16, pad=20)
        plt.axis('off')
        plt.tight_layout()
        plt.show()
        
        return self.G
    
    def plot_interactive(self, title="Stock Correlation Network"):
        """
        Create interactive plot using Plotly
        
        Parameters:
        title: Plot title
        """
        if len(self.G.nodes()) == 0:
            print("No connected nodes to visualize")
            return
        
        # Get layout
        pos = nx.spring_layout(self.G, k=1, iterations=50)
        
        # Extract node positions
        node_x = [pos[node][0] for node in self.G.nodes()]
        node_y = [pos[node][1] for node in self.G.nodes()]
        
        # Create BLACK edge traces
        edge_traces = []
        for edge in self.G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            
            edge_trace = go.Scatter(
                x=[x0, x1, None], y=[y0, y1, None],
                line=dict(width=self.G[edge[0]][edge[1]]['weight'] * 6, color='black'),
                hoverinfo='none',
                mode='lines')
            edge_traces.append(edge_trace)
        
        # Create YELLOW node trace with hover info
        node_hover_text = []
        node_labels = []
        for node in self.G.nodes():
            # Count connections
            out_degree = self.G.out_degree(node)
            in_degree = self.G.in_degree(node)
            node_hover_text.append(
                f"<b>{node}</b><br>"
                f"Outgoing: {out_degree}<br>"
                f"Incoming: {in_degree}<br>"
                f"Total: {out_degree + in_degree}"
            )
            node_labels.append(node)
        
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            hovertext=node_hover_text,
            text=node_labels,
            marker=dict(
                size=25,
                color='yellow',
                line=dict(width=2, color='black')
            ),
            textposition="middle center",
            textfont=dict(size=10, color='black')
        )
        
        # Create figure
        fig = go.Figure(data=edge_traces + [node_trace])
        
        # Update layout properly
        fig.update_layout(
            title=dict(
                text=f'<b>{title}</b><br>Threshold: {self.correlation_graph.threshold} | Connected: {len(self.G.nodes())}',
                font=dict(size=16)
            ),
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=80),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        
        fig.show()
        return fig
    
    def plot_minimal(self, figsize=(12, 8)):
        """
        Create a minimal clean visualization
        
        Parameters:
        figsize: Figure size tuple
        """
        if len(self.G.nodes()) == 0:
            print("No connected nodes to visualize")
            return
        
        plt.figure(figsize=figsize)
        pos = nx.spring_layout(self.G)
        
        # Yellow nodes with black borders
        nx.draw_networkx_nodes(self.G, pos, 
                              node_color='yellow', 
                              node_size=300, 
                              edgecolors='black', 
                              linewidths=2)
        
        # Black edges
        nx.draw_networkx_edges(self.G, pos, 
                              edge_color='black', 
                              arrows=True, 
                              arrowsize=15)
        
        # Labels
        nx.draw_networkx_labels(self.G, pos, 
                               font_size=8, 
                               font_weight='bold')
        
        plt.title(f"Connected Stocks Network\n"
                  f"{len(self.G.nodes())} stocks | {len(self.G.edges())} connections | "
                  f"Threshold: {self.correlation_graph.threshold}")
        plt.axis('off')
        plt.tight_layout()
        plt.show()
    
    def get_network_stats(self) -> Dict:
        """Get statistics about the connected network"""
        if len(self.G.nodes()) == 0:
            return {}
        
        return {
            'total_nodes': len(self.correlation_graph.ordered_tickers),
            'connected_nodes': len(self.G.nodes()),
            'edges': len(self.G.edges()),
            'threshold': self.correlation_graph.threshold,
            'connected_node_list': list(self.G.nodes()),
            'average_degree': sum(dict(self.G.degree()).values()) / len(self.G.nodes())
        }
    
    def get_highly_connected_stocks(self, min_connections=3) -> List[str]:
        """Get stocks with minimum number of connections"""
        if len(self.G.nodes()) == 0:
            return []
        
        highly_connected = []
        for node in self.G.nodes():
            total_connections = self.G.out_degree(node) + self.G.in_degree(node)
            if total_connections >= min_connections:
                highly_connected.append((node, total_connections))
        
        return sorted(highly_connected, key=lambda x: x[1], reverse=True)