import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from io import StringIO
import lxml
import time
from datetime import datetime

def get_spy():
    """Dataframe of all tickers in S&P 500"""
    url = 'https://www.slickcharts.com/sp500'
    request = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = bs(request.text, "lxml")
    stats = soup.find('table', class_='table table-hover table-borderless table-sm')
    
    df = pd.read_html(StringIO(str(stats)))[0]
    return df['Symbol'].tolist()

def calculate_yield_and_dividend_years(ticker, index, total):
    try:
        print(f"Processando {ticker} ({index + 1}/{total})...")
        stock = yf.Ticker(ticker)
        
        # Obter informações principais
        info = stock.info
        dividend_yield = info.get("dividendYield", 0) * 100
        current_price = info.get("currentPrice", 0)
        short_name = info.get("shortName", "N/A")
        setor = info.get("sector", "N/A")
        
        # Obter histórico de dividendos
        historico = stock.history(period="max")
        dividendos = historico['Dividends']
        if dividendos.empty or dividend_yield == 0 or current_price == 0:
            return {'Ticker': ticker, 'Status': 'Excluída', 'Missing': ['Dados insuficientes']}
        
        # Calcular anos consecutivos de pagamento de dividendos
        anos = dividendos.index.year
        anos_unicos = sorted(set(anos))
        anos_consecutivos = 1
        for i in range(1, len(anos_unicos)):
            if anos_unicos[i] == anos_unicos[i - 1] + 1:
                anos_consecutivos += 1
            else:
                break  # Se houver uma lacuna, interrompa o contador
        
        return {
            'Ticker': ticker,
            'Status': 'Incluída',
            'Nome': short_name,
            'Setor': setor,
            'Preço Atual (USD)': current_price,
            'Dividend Yield (%)': round(dividend_yield, 2),
            'Anos de Dividendos Consecutivos': anos_consecutivos
        }

    except Exception as e:
        return {'Ticker': ticker, 'Status': 'Excluída', 'Missing': [str(e)]}

def main():
    start_time = time.time()
    companies = get_spy()
    all_results = []

    for index, ticker in enumerate(companies):
        result = calculate_yield_and_dividend_years(ticker, index, len(companies))
        all_results.append(result)

    # Separar empresas incluídas e excluídas
    included = [r for r in all_results if r['Status'] == 'Incluída']
    excluded = [r for r in all_results if r['Status'] == 'Excluída']

    if len(included) > 0:
        # Criar DataFrame com as empresas incluídas
        scores_df = pd.DataFrame(included)
        
        # Criar um modelo baseado em Dividend Yield e Anos de Dividendos Consecutivos
        scores_df['Yield_Rank'] = scores_df['Dividend Yield (%)'].rank(ascending=False)
        scores_df['Years_Rank'] = scores_df['Anos de Dividendos Consecutivos'].rank(ascending=False)
        scores_df['Combined_Rank'] = (scores_df['Yield_Rank'] + scores_df['Years_Rank']) / 2
        
        # Ordenar pelo ranking combinado
        scores_df = scores_df.sort_values('Combined_Rank').reset_index(drop=True)
        scores_df['Final_Rank'] = scores_df.index + 1
        
        # Exibir Top 10
        print("\nTop 10 Empresas pelo Modelo (Yield e Anos de Dividendos):")
        print(scores_df[['Final_Rank', 'Ticker', 'Dividend Yield (%)', 'Anos de Dividendos Consecutivos']].head(10).to_string(index=False))
        
        # Exibir estatísticas das exclusões
        print(f"\nEstatísticas de processamento:")
        print(f"Total de empresas analisadas: {len(all_results)}")
        print(f"Empresas incluídas: {len(included)}")
        print(f"Empresas excluídas: {len(excluded)}")
        
        # Exibir detalhes das exclusões
        print("\nEmpresas excluídas da análise:")
        for company in excluded:
            print(f"{company['Ticker']}: Motivo - {company['Missing']}")

        end_time = time.time()
        print(f"\nTempo total de execução: {end_time - start_time:.2f} segundos")
        return scores_df[['Final_Rank', 'Ticker', 'Dividend Yield (%)', 'Anos de Dividendos Consecutivos']].head(10)
    else:
        print("Nenhuma empresa teve dados suficientes para análise.")
        return None

if __name__ == "__main__":
    top_10 = main()
