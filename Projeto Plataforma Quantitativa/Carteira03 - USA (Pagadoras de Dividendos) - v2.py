from typing import List, Dict, Optional, TypedDict
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
from io import StringIO
import logging
from dataclasses import dataclass
import lxml
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DividendResult:
    """Data class to store dividend analysis results."""
    ticker: str
    status: str
    name: Optional[str] = None
    sector: Optional[str] = None
    current_price: Optional[float] = None
    dividend_yield: Optional[float] = None
    consecutive_years: Optional[int] = None
    missing_data: List[str] = None

    def __post_init__(self):
        if self.missing_data is None:
            self.missing_data = []

class SP500Scraper:
    """Class responsible for scraping S&P 500 data."""
    
    def __init__(self, url: str = 'https://www.slickcharts.com/sp500'):
        self.url = url
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def get_tickers(self) -> List[str]:
        """Retrieve list of S&P 500 tickers from web."""
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

class DividendAnalyzer:
    """Class for analyzing dividend metrics."""

    @staticmethod
    def get_stock_info(stock: yf.Ticker) -> Optional[StockInfo]:
        """Extract required stock information."""
        try:
            info = stock.info
            return StockInfo(
                dividend_yield=info.get("dividendYield", 0) * 100,
                current_price=info.get("currentPrice", 0),
                short_name=info.get("shortName", "N/A"),
                sector=info.get("sector", "N/A")
            )
        except Exception as e:
            logger.error(f"Error getting stock info: {e}")
            return None

    @staticmethod
    def calculate_consecutive_years(history: pd.DataFrame) -> int:
        """Calculate consecutive years of dividend payments."""
        dividends = history['Dividends']
        if dividends.empty:
            return 0
            
        years = dividends.index.year
        unique_years = sorted(set(years))
        consecutive_years = 1
        
        for i in range(1, len(unique_years)):
            if unique_years[i] == unique_years[i - 1] + 1:
                consecutive_years += 1
            else:
                break
                
        return consecutive_years

class StockAnalyzer:
    """Main class for analyzing stocks using dividend metrics."""
    
    def analyze_stock(self, ticker: str, index: int, total: int) -> DividendResult:
        """Analyze a single stock's dividend history and metrics."""
        logger.info(f"Processing {ticker} ({index + 1}/{total})")
        
        try:
            stock = yf.Ticker(ticker)
            stock_info = DividendAnalyzer.get_stock_info(stock)
            
            if not stock_info:
                return DividendResult(
                    ticker=ticker,
                    status='Excluída',
                    missing_data=['Dados básicos não disponíveis']
                )

            # Verify required data
            if stock_info['dividend_yield'] == 0 or stock_info['current_price'] == 0:
                return DividendResult(
                    ticker=ticker,
                    status='Excluída',
                    missing_data=['Dados insuficientes de dividendos']
                )

            # Get dividend history
            history = stock.history(period="max")
            consecutive_years = DividendAnalyzer.calculate_consecutive_years(history)
            
            return DividendResult(
                ticker=ticker,
                status='Incluída',
                name=stock_info['short_name'],
                sector=stock_info['sector'],
                current_price=stock_info['current_price'],
                dividend_yield=round(stock_info['dividend_yield'], 2),
                consecutive_years=consecutive_years
            )

        except Exception as e:
            return DividendResult(
                ticker=ticker,
                status='Excluída',
                missing_data=[str(e)]
            )

class ResultsProcessor:
    """Class for processing and presenting analysis results."""
    
    @staticmethod
    def prepare_rankings(results: List[DividendResult]) -> Optional[pd.DataFrame]:
        """Process results and create rankings."""
        included = [r for r in results if r.status == 'Incluída']
        
        if not included:
            logger.warning("No companies had sufficient data for analysis")
            return None
            
        df = pd.DataFrame(included)
        
        # Calculate rankings
        df['Yield_Rank'] = df['dividend_yield'].rank(ascending=False)
        df['Years_Rank'] = df['consecutive_years'].rank(ascending=False)
        df['Combined_Rank'] = (df['Yield_Rank'] + df['Years_Rank']) / 2
        
        # Sort and add final ranking
        df = df.sort_values('Combined_Rank').reset_index(drop=True)
        df['Final_Rank'] = df.index + 1
        
        return df

    @staticmethod
    def print_results(results: List[DividendResult], rankings_df: Optional[pd.DataFrame], 
                     execution_time: float) -> None:
        """Print analysis results and statistics."""
        if rankings_df is not None:
            logger.info("\nTop 10 Companies by Dividend Model:")
            display_df = rankings_df[['Final_Rank', 'ticker', 'dividend_yield', 'consecutive_years']].head(10)
            display_df.columns = ['Final_Rank', 'Ticker', 'Dividend Yield (%)', 'Anos de Dividendos Consecutivos']
            logger.info(display_df.to_string(index=False))

        excluded = [r for r in results if r.status == 'Excluída']
        
        logger.info("\nProcessing Statistics:")
        logger.info(f"Total companies analyzed: {len(results)}")
        logger.info(f"Companies included: {len(results) - len(excluded)}")
        logger.info(f"Companies excluded: {len(excluded)}")
        
        logger.info("\nExcluded Companies Details:")
        for result in excluded:
            logger.info(f"{result.ticker}: Missing data - {result.missing_data}")
            
        logger.info(f"\nTotal execution time: {execution_time:.2f} seconds")

class DividendAnalysis:
    """Main class orchestrating the entire analysis process."""
    
    def __init__(self):
        self.scraper = SP500Scraper()
        self.analyzer = StockAnalyzer()
        self.processor = ResultsProcessor()

    def run_analysis(self) -> Optional[pd.DataFrame]:
        """Execute complete dividend analysis."""
        start_time = time.time()
        
        # Get companies list
        companies = self.scraper.get_tickers()
        if not companies:
            logger.error("Failed to retrieve company list")
            return None
            
        # Analyze all companies
        results = [
            self.analyzer.analyze_stock(ticker, idx, len(companies))
            for idx, ticker in enumerate(companies)
        ]
        
        # Process results
        rankings_df = self.processor.prepare_rankings(results)
        
        # Print results
        self.processor.print_results(
            results, 
            rankings_df,
            time.time() - start_time
        )
        
        if rankings_df is not None:
            return rankings_df[['Final_Rank', 'ticker', 'dividend_yield', 'consecutive_years']].head(10)
        return None

def main() -> Optional[pd.DataFrame]:
    """Main entry point of the program."""
    analyzer = DividendAnalysis()
    return analyzer.run_analysis()

if __name__ == "__main__":
    top_10 = main()