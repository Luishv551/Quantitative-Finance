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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockData(TypedDict):
    """Type definition for stock financial data."""
    EBIT: float
    Total_Assets: float
    Current_Assets: float
    Current_Liabilities: float
    Market_Cap: float
    Total_Debt: float
    Total_Cash: float

@dataclass
class StockResult:
    """Data class to store stock analysis results."""
    ticker: str
    status: str
    roc: Optional[float] = None
    earnings_yield: Optional[float] = None
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

class MagicFormulaCalculator:
    """Class for calculating Magic Formula metrics."""

    @staticmethod
    def get_financial_data(stock: yf.Ticker) -> Optional[StockData]:
        """Extract required financial data from stock information."""
        try:
            income_stmt = stock.income_stmt
            balance_sheet = stock.balance_sheet
            info = stock.info

            if income_stmt.empty or balance_sheet.empty:
                return None

            latest_income = income_stmt.iloc[:, 0]
            latest_balance = balance_sheet.iloc[:, 0]

            return StockData(
                EBIT=latest_income.get('EBIT'),
                Total_Assets=latest_balance.get('Total Assets'),
                Current_Assets=latest_balance.get('Current Assets'),
                Current_Liabilities=latest_balance.get('Current Liabilities'),
                Market_Cap=info.get('marketCap'),
                Total_Debt=info.get('totalDebt'),
                Total_Cash=info.get('totalCash')
            )
        except Exception as e:
            logger.error(f"Error getting financial data: {e}")
            return None

    @staticmethod
    def calculate_metrics(data: StockData) -> tuple[float, float]:
        """Calculate ROC and Earnings Yield from financial data."""
        net_fixed_assets = data['Total_Assets'] - data['Current_Assets']
        working_capital = data['Current_Assets'] - data['Current_Liabilities']
        enterprise_value = data['Market_Cap'] + data['Total_Debt'] - data['Total_Cash']
        
        denominator = working_capital + net_fixed_assets
        if denominator == 0 or enterprise_value == 0:
            raise ValueError("Division by zero in calculations")
            
        roc = data['EBIT'] / denominator
        earnings_yield = data['EBIT'] / enterprise_value
        
        return roc, earnings_yield

class StockAnalyzer:
    """Main class for analyzing stocks using Magic Formula."""
    
    def analyze_stock(self, ticker: str, index: int, total: int) -> StockResult:
        """Analyze a single stock using Magic Formula methodology."""
        logger.info(f"Processing {ticker} ({index + 1}/{total})")
        
        try:
            stock = yf.Ticker(ticker)
            financial_data = MagicFormulaCalculator.get_financial_data(stock)
            
            if not financial_data:
                return StockResult(
                    ticker=ticker,
                    status='Excluída',
                    missing_data=['Dados financeiros não disponíveis']
                )

            # Check for missing data
            missing = [k for k, v in financial_data.items() if v is None]
            if missing:
                return StockResult(
                    ticker=ticker,
                    status='Excluída',
                    missing_data=missing
                )

            roc, earnings_yield = MagicFormulaCalculator.calculate_metrics(financial_data)
            
            return StockResult(
                ticker=ticker,
                status='Incluída',
                roc=roc,
                earnings_yield=earnings_yield
            )

        except Exception as e:
            return StockResult(
                ticker=ticker,
                status='Excluída',
                missing_data=[str(e)]
            )

class ResultsProcessor:
    """Class for processing and presenting analysis results."""
    
    @staticmethod
    def prepare_rankings(results: List[StockResult]) -> Optional[pd.DataFrame]:
        """Process results and create rankings."""
        included = [r for r in results if r.status == 'Incluída']
        
        if not included:
            logger.warning("No companies had sufficient data for analysis")
            return None
            
        df = pd.DataFrame(included)
        
        # Calculate rankings
        df['ROC_Rank'] = df['roc'].rank(ascending=False)
        df['EY_Rank'] = df['earnings_yield'].rank(ascending=False)
        df['Combined_Rank'] = (df['ROC_Rank'] + df['EY_Rank']) / 2
        
        # Sort and add final ranking
        df = df.sort_values('Combined_Rank').reset_index(drop=True)
        df['Final_Rank'] = df.index + 1
        
        return df

    @staticmethod
    def print_results(results: List[StockResult], rankings_df: Optional[pd.DataFrame], 
                     execution_time: float) -> None:
        """Print analysis results and statistics."""
        if rankings_df is not None:
            logger.info("\nTop 10 Companies by Magic Formula:")
            logger.info(rankings_df[['Final_Rank', 'ticker']].head(10).to_string(index=False))

        excluded = [r for r in results if r.status == 'Excluída']
        
        logger.info("\nProcessing Statistics:")
        logger.info(f"Total companies analyzed: {len(results)}")
        logger.info(f"Companies included: {len(results) - len(excluded)}")
        logger.info(f"Companies excluded: {len(excluded)}")
        
        logger.info("\nExcluded Companies Details:")
        for result in excluded:
            logger.info(f"{result.ticker}: Missing data - {result.missing_data}")
            
        logger.info(f"\nTotal execution time: {execution_time:.2f} seconds")

class MagicFormulaAnalysis:
    """Main class orchestrating the entire analysis process."""
    
    def __init__(self):
        self.scraper = SP500Scraper()
        self.analyzer = StockAnalyzer()
        self.processor = ResultsProcessor()

    def run_analysis(self) -> Optional[pd.DataFrame]:
        """Execute complete Magic Formula analysis."""
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
        
        return rankings_df[['Final_Rank', 'ticker']].head(10) if rankings_df is not None else None

def main() -> Optional[pd.DataFrame]:
    """Main entry point of the program."""
    analyzer = MagicFormulaAnalysis()
    return analyzer.run_analysis()

if __name__ == "__main__":
    top_10 = main()