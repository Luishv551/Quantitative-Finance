import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
from io import StringIO

# Função para obter a lista completa do S&P 500
def get_spy():
    """Dataframe of all tickers in S&P 500"""
    url = 'https://www.slickcharts.com/sp500'
    request = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = bs(request.text, "lxml")
    stats = soup.find('table', class_='table table-hover table-borderless table-sm')
    
    df = pd.read_html(StringIO(str(stats)))[0]

    return df['Symbol'].tolist()  # Retorna apenas a lista de tickers

# Função para calcular o score com base nos indicadores
def calculate_score(ticker, index, total):
    try:
        print(f"Processando {ticker} ({index + 1}/{total})...")  # Log de progresso
        stock = yf.Ticker(ticker)
        info = stock.info

        # Obter os indicadores
        pe_ratio = info.get('trailingPE')
        roe = info.get('returnOnEquity')
        debt_to_equity = info.get('debtToEquity')
        dividend_yield = info.get('dividendYield')

        # Verificar se todos os indicadores estão disponíveis
        if None in [pe_ratio, roe, debt_to_equity, dividend_yield]:
            return None

        # Calcular o score
        score = (-pe_ratio * 10) + (roe * 10) - (debt_to_equity / 10) + (dividend_yield * 100)
        return score
    except Exception as e:
        print(f"Erro ao processar {ticker}: {e}")
        return None

# Calcular scores para todas as empresas
def main():
    start_time = time.time()  # Medir tempo total de execução

    # Obter a lista completa de empresas do S&P 500
    companies = get_spy()
    scores = []

    for index, ticker in enumerate(companies):
        score = calculate_score(ticker, index, len(companies))
        if score is not None:
            scores.append({'Ticker': ticker, 'Score': score})

    # Criar DataFrame e garantir que a coluna 'Score' seja numérica
    if len(scores) > 0:  # Verificar se há scores calculados
        scores_df = pd.DataFrame(scores)
        scores_df['Score'] = scores_df['Score'].astype(float)

        # Ordenar pelo maior score e resetar o índice
        scores_df = scores_df.sort_values(by='Score', ascending=False).reset_index(drop=True)

        # Adicionar ranking
        scores_df['Rank'] = scores_df.index + 1

        # Exibir apenas o Top 10 com o ranking e o ticker
        top_10 = scores_df[['Rank', 'Ticker']].head(10)
        print("\nTop 10 Empresas por Ranking:")
        print(top_10.to_string(index=False))

        end_time = time.time()
        print(f"\nTempo total de execução: {end_time - start_time:.2f} segundos")
        return top_10
    else:
        print("Nenhum score foi calculado. Verifique os tickers ou os dados financeiros.")
        return None

if __name__ == "__main__":
    top_10 = main()
