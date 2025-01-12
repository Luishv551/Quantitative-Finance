import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
from io import StringIO
import lxml

def get_spy():
    """Dataframe of all tickers in S&P 500"""
    url = 'https://www.slickcharts.com/sp500'
    request = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = bs(request.text, "lxml")
    stats = soup.find('table', class_='table table-hover table-borderless table-sm')
    
    df = pd.read_html(StringIO(str(stats)))[0]
    return df['Symbol'].tolist()

def calculate_magic_formula(ticker, index, total):
    try:
        print(f"Processando {ticker} ({index + 1}/{total})...")
        stock = yf.Ticker(ticker)
        
        # Pegando demonstrações financeiras
        income_stmt = stock.income_stmt
        balance_sheet = stock.balance_sheet
        
        if income_stmt.empty or balance_sheet.empty:
            return {'Ticker': ticker, 'Status': 'Excluída', 'Missing': ['Dados financeiros não disponíveis']}
        
        # Pegando os últimos dados disponíveis (primeira coluna)
        latest_income = income_stmt.iloc[:, 0]
        latest_balance = balance_sheet.iloc[:, 0]
        info = stock.info
        
        # Obter todos os dados necessários
        required_data = {
            'EBIT': latest_income.get('EBIT'),
            'Total Assets': latest_balance.get('Total Assets'),
            'Current Assets': latest_balance.get('Current Assets'),
            'Current Liabilities': latest_balance.get('Current Liabilities'),
            'Market Cap': info.get('marketCap'),
            'Total Debt': info.get('totalDebt'),
            'Total Cash': info.get('totalCash')
        }
        
        # Verificar se há dados faltantes
        missing_data = {k: v for k, v in required_data.items() if v is None}
        if missing_data:
            return {'Ticker': ticker, 'Status': 'Excluída', 'Missing': list(missing_data.keys())}
            
        # Cálculos com os dados disponíveis
        net_fixed_assets = required_data['Total Assets'] - required_data['Current Assets']
        working_capital = required_data['Current Assets'] - required_data['Current Liabilities']
        enterprise_value = required_data['Market Cap'] + required_data['Total Debt'] - required_data['Total Cash']
        
        # Verificar divisão por zero
        if (working_capital + net_fixed_assets) == 0 or enterprise_value == 0:
            return {'Ticker': ticker, 'Status': 'Excluída', 'Missing': ['Divisão por zero nos cálculos']}

        # Calcular os componentes da Magic Formula
        roc = required_data['EBIT'] / (working_capital + net_fixed_assets)
        earnings_yield = required_data['EBIT'] / enterprise_value

        return {
            'Ticker': ticker,
            'Status': 'Incluída',
            'ROC': roc,
            'EarningsYield': earnings_yield
        }

    except Exception as e:
        return {'Ticker': ticker, 'Status': 'Excluída', 'Missing': [str(e)]}

def main():
    start_time = time.time()
    companies = get_spy()
    all_results = []

    for index, ticker in enumerate(companies):
        result = calculate_magic_formula(ticker, index, len(companies))
        all_results.append(result)

    # Separar empresas incluídas e excluídas
    included = [r for r in all_results if r['Status'] == 'Incluída']
    excluded = [r for r in all_results if r['Status'] == 'Excluída']

    if len(included) > 0:
        # Criar DataFrame com as empresas incluídas
        scores_df = pd.DataFrame(included)
        
        # Rankear separadamente ROC e Earnings Yield
        scores_df['ROC_Rank'] = scores_df['ROC'].rank(ascending=False)
        scores_df['EY_Rank'] = scores_df['EarningsYield'].rank(ascending=False)
        
        # Combinar os rankings
        scores_df['Combined_Rank'] = (scores_df['ROC_Rank'] + scores_df['EY_Rank']) / 2
        
        # Ordenar pelo ranking combinado
        scores_df = scores_df.sort_values('Combined_Rank').reset_index(drop=True)
        scores_df['Final_Rank'] = scores_df.index + 1
        
        # Exibir Top 10
        print("\nTop 10 Empresas pela Magic Formula:")
        print(scores_df[['Final_Rank', 'Ticker']].head(10).to_string(index=False))
        
        # Exibir estatísticas das exclusões
        print(f"\nEstatísticas de processamento:")
        print(f"Total de empresas analisadas: {len(all_results)}")
        print(f"Empresas incluídas: {len(included)}")
        print(f"Empresas excluídas: {len(excluded)}")
        
        # Exibir detalhes das exclusões
        print("\nEmpresas excluídas da análise:")
        for company in excluded:
            print(f"{company['Ticker']}: Dados faltantes - {company['Missing']}")

        end_time = time.time()
        print(f"\nTempo total de execução: {end_time - start_time:.2f} segundos")
        return scores_df[['Final_Rank', 'Ticker']].head(10)
    else:
        print("Nenhuma empresa teve dados suficientes para análise.")
        return None

if __name__ == "__main__":
    top_10 = main()