from typing import List, Dict, Optional
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
from io import StringIO
import logging
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class StockScore:
    """Data class to store stock scoring information."""
    ticker: str
    score: float

class SP500Scraper:
    """Class responsible for scraping S&P 500 data."""
    
    def __init__(self, url: str = 'https://www.slickcharts.com/sp500'):
        self.url = url
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def get_tickers(self) -> List[str]:
        """Retrieve list of S&P 500 tickers from web.
        
        Returns:
            List[str]: List of stock tickers
        """
        try:
            request = requests.get(self.url, headers=self.headers)
            request.raise_for_status()
            soup = bs(request.text, "lxml")
            stats = soup.find('table', class_='table table-hover table-borderless table-sm')
            
            if not stats:
                raise ValueError("Table not found in webpage")
            
            df = pd.read_html(StringIO(str(stats)))[0]
            return df['Symbol'].tolist()
        
        except Exception as e:
            logger.error(f"Error fetching S&P 500 tickers: {e}")
            return []

class StockAnalyzer:
    """Class for analyzing stock metrics and calculating scores."""

    @staticmethod
    def calculate_score(ticker: str, index: int, total: int) -> Optional[StockScore]:
        """Calculate score for a given stock based on financial metrics.
        
        Args:
            ticker: Stock symbol
            index: Current processing index
            total: Total number of stocks to process
            
        Returns:
            Optional[StockScore]: Stock score if calculation successful, None otherwise
        """
        logger.info(f"Processing {ticker} ({index + 1}/{total})")
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            metrics = {
                'trailingPE': info.get('trailingPE'),
                'returnOnEquity': info.get('returnOnEquity'),
                'debtToEquity': info.get('debtToEquity'),
                'dividendYield': info.get('dividendYield')
            }

            if any(value is None for value in metrics.values()):
                logger.warning(f"Missing metrics for {ticker}")
                return None

            score = (
                (-metrics['trailingPE'] * 10) +
                (metrics['returnOnEquity'] * 10) -
                (metrics['debtToEquity'] / 10) +
                (metrics['dividendYield'] * 100)
            )
            
            return StockScore(ticker=ticker, score=score)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            return None

class SP500Analyzer:
    """Main class for analyzing S&P 500 stocks."""
    
    def __init__(self):
        self.scraper = SP500Scraper()
        self.analyzer = StockAnalyzer()

    def run_analysis(self) -> Optional[pd.DataFrame]:
        """Execute full analysis of S&P 500 stocks.
        
        Returns:
            Optional[pd.DataFrame]: Top 10 stocks by score if successful, None otherwise
        """
        start_time = time.time()
        companies = self.scraper.get_tickers()
        
        if not companies:
            logger.error("Failed to retrieve company list")
            return None

        scores = []
        for index, ticker in enumerate(companies):
            score_result = self.analyzer.calculate_score(ticker, index, len(companies))
            if score_result:
                scores.append({
                    'Ticker': score_result.ticker,
                    'Score': score_result.score
                })

        if not scores:
            logger.warning("No valid scores calculated")
            return None

        return self._prepare_results(scores, start_time)

    def _prepare_results(self, scores: List[Dict], start_time: float) -> pd.DataFrame:
        """Prepare and format analysis results.
        
        Args:
            scores: List of calculated stock scores
            start_time: Analysis start timestamp
            
        Returns:
            pd.DataFrame: Formatted top 10 results
        """
        scores_df = pd.DataFrame(scores)
        scores_df['Score'] = scores_df['Score'].astype(float)
        scores_df = scores_df.sort_values(by='Score', ascending=False).reset_index(drop=True)
        scores_df['Rank'] = scores_df.index + 1
        
        top_10 = scores_df[['Rank', 'Ticker']].head(10)
        
        logger.info("\nTop 10 Companies by Ranking:")
        logger.info(f"\n{top_10.to_string(index=False)}")
        logger.info(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
        
        return top_10

def main():
    """Main entry point of the program."""
    analyzer = SP500Analyzer()
    return analyzer.run_analysis()

if __name__ == "__main__":
    top_10 = main()