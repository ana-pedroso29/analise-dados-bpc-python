# data_pipeline.py
import requests
import zipfile
import io
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

def baixar_bpc(ano, mes):
    """Baixa os dados do BPC para um ano/mês específico."""
    print(f"--- Tentando baixar dados para {mes}/{ano}... ---")
    mes_formatado = str(mes).zfill(2)
    url_direta = f"https://www.portaltransparencia.gov.br/download-de-dados/bpc/{ano}{mes_formatado}"

    try:
        response_zip = requests.get(url_direta, timeout=300)
        response_zip.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response_zip.content)) as z:
            csv_file_name = [name for name in z.namelist() if name.lower().endswith('.csv')][0]
            with z.open(csv_file_name) as f:
                df_bruto = pd.read_csv(f, encoding='ISO-8859-1', sep=';', decimal=',', engine='python', on_bad_lines='skip')
                print(f"Sucesso! Dados de {mes}/{ano} baixados e lidos.")
                return df_bruto
    except Exception as e:
        print(f"AVISO: Falha ao baixar dados de {mes}/{ano}. Motivo: {e}")
        return None

def tratar_e_agregar(df_bruto):
    """Trata e agrega um dataframe de dados brutos do BPC."""
    if df_bruto is None:
        return None
    
    df_bruto.rename(columns={
        'CÓDIGO MUNICÍPIO SIAFI': 'IBGE_COD',
        'NOME MUNICÍPIO': 'MUNICIPIO',
        'UF': 'UF',
        'VALOR PARCELA': 'VALOR_BRUTO'
    }, inplace=True)
    
    # Seleciona apenas as colunas que vamos usar para otimizar a memória
    colunas_necessarias = ['IBGE_COD', 'MUNICIPIO', 'UF', 'VALOR_BRUTO']
    df_bruto = df_bruto[colunas_necessarias]

    df_bruto['IBGE_COD'] = df_bruto['IBGE_COD'].astype(str).str.zfill(7)
    df_bruto['VALOR_BRUTO'] = pd.to_numeric(df_bruto['VALOR_BRUTO'], errors='coerce')
    df_bruto.dropna(subset=['VALOR_BRUTO'], inplace=True)

    df_agregado = df_bruto.groupby(['IBGE_COD', 'MUNICIPIO', 'UF']).agg(
        total_beneficios=('VALOR_BRUTO', 'count'),
        soma_valor=('VALOR_BRUTO', 'sum'),
        media_valor=('VALOR_BRUTO', 'mean')
    ).reset_index()
    return df_agregado

def detectar_outliers(df_agregado):
    """Aplica o Isolation Forest no dataframe agregado."""
    if df_agregado is None or df_agregado.empty:
        return None
        
    features = ['total_beneficios', 'soma_valor']
    X = np.log1p(df_agregado[features])
    
    model = IsolationForest(contamination=0.01, random_state=42)
    model.fit(X)
    predicoes = model.predict(X)
    
    df_agregado['is_outlier'] = ['INCONSISTÊNCIA' if p == -1 else 'NORMAL' for p in predicoes]
    return df_agregado