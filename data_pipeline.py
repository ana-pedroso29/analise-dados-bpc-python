# data_pipeline.py

import hashlib
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

def tratar_dados_individuais(df_bruto):
    """
    Limpa, padroniza e anonimiza os dados individuais dos beneficiários.
    Esta função NÃO agrega os dados.
    """
    if df_bruto is None:
        return None

    print(">>> Iniciando Passo 2: Tratamento e Anonimização de Dados Individuais...")
    
    colunas_rename = {
        'CÓDIGO MUNICÍPIO SIAFI': 'IBGE_COD',
        'NOME MUNICÍPIO': 'MUNICIPIO',
        'UF': 'UF',
        'VALOR PARCELA': 'VALOR_BRUTO',
        'CPF BENEFICIÁRIO': 'CPF_BENEFICIARIO',
        
    }
    df_bruto.rename(columns=colunas_rename, inplace=True)

    def hash_cpf(cpf):
        cpf_str = str(cpf).encode('utf-8')
        return hashlib.sha256(cpf_str).hexdigest()

    df_bruto['BENEFICIARIO_ID_HASH'] = df_bruto['CPF_BENEFICIARIO'].apply(hash_cpf)
    df_bruto['REPRESENTANTE_ID_HASH'] = df_bruto['CPF_REPRESENTANTE'].apply(hash_cpf)
    
    df_bruto['IBGE_COD'] = df_bruto['IBGE_COD'].astype(str).str.zfill(7)
    df_bruto['VALOR_BRUTO'] = pd.to_numeric(df_bruto['VALOR_BRUTO'], errors='coerce')
    df_bruto.dropna(subset=['VALOR_BRUTO'], inplace=True)
    
    colunas_finais = [
        'IBGE_COD', 'MUNICIPIO', 'UF', 'VALOR_BRUTO', 
        'BENEFICIARIO_ID_HASH', 'REPRESENTANTE_ID_HASH'
    ]
    
    df_tratado = df_bruto[colunas_finais]

    print("Tratamento e anonimização concluídos.")
    return df_tratado

def analisar_representantes(df_tratado, limiar=10):
    """Encontra representantes legais com um número de beneficiários acima de um limiar."""
    print("--- Executando Análise de Representantes Legais...")
    contagem_representantes = df_tratado.groupby('REPRESENTANTE_ID_HASH')['BENEFICIARIO_ID_HASH'].nunique().reset_index()
    contagem_representantes.rename(columns={'BENEFICIARIO_ID_HASH': 'QTD_BENEFICIARIOS'}, inplace=True)
    
    representantes_suspeitos = contagem_representantes[contagem_representantes['QTD_BENEFICIARIOS'] > limiar]
    representantes_suspeitos = representantes_suspeitos.sort_values(by='QTD_BENEFICIARIOS', ascending=False)
    
    print(f"Análise concluída. {len(representantes_suspeitos)} representantes com mais de {limiar} beneficiários encontrados.")
    return representantes_suspeitos

def analisar_duplicados(df_tratado):
    """Encontra beneficiários que aparecem mais de uma vez no mesmo arquivo."""
    print("--- Executando Análise de Beneficiários Duplicados...")
    contagem_duplicados = df_tratado['BENEFICIARIO_ID_HASH'].value_counts()
    
    beneficiarios_duplicados = contagem_duplicados[contagem_duplicados > 1].reset_index()
    beneficiarios_duplicados.columns = ['BENEFICIARIO_ID_HASH', 'QTD_APARICOES']
    
    print(f"Análise concluída. {len(beneficiarios_duplicados)} possíveis beneficiários duplicados encontrados.")
    return beneficiarios_duplicados

def tratar_e_agregar(df_bruto):
    """
    Limpa, anonimiza, e agrega os dados por município,
    calculando tanto o total de pagamentos quanto o total de pessoas únicas.
    """
    if df_bruto is None:
        return None

    print(">>> Iniciando Passo 2: Tratamento, Anonimização e Agregação...")

    # 1. Renomear colunas (incluindo as de PII que precisamos)
    colunas_rename = {
        'CÓDIGO MUNICÍPIO SIAFI': 'IBGE_COD',
        'NOME MUNICÍPIO': 'MUNICIPIO',
        'UF': 'UF',
        'VALOR PARCELA': 'VALOR_BRUTO',
        'CPF BENEFICIÁRIO': 'CPF_BENEFICIARIO' # Coluna que precisamos para contar pessoas
    }
    df_bruto.rename(columns=colunas_rename, inplace=True)

    # 2. Função interna de Anonimização (Hashing)
    def hash_cpf(cpf):
        cpf_str = str(cpf).encode('utf-8')
        return hashlib.sha256(cpf_str).hexdigest()

    # 3. Aplicar Anonimização
    # Verifica se a coluna de CPF existe antes de tentar usá-la
    if 'CPF_BENEFICIARIO' in df_bruto.columns:
        df_bruto['BENEFICIARIO_ID_HASH'] = df_bruto['CPF_BENEFICIARIO'].apply(hash_cpf)
    else:
        print("AVISO: Coluna 'CPF_BENEFICIARIO' não encontrada. A contagem de 'total_pessoas' será baseada em índices.")
        # Cria uma coluna de fallback para a agregação 'nunique' não falhar
        df_bruto['BENEFICIARIO_ID_HASH'] = df_bruto.index

    # 4. Limpeza (como antes)
    df_bruto['IBGE_COD'] = df_bruto['IBGE_COD'].astype(str).str.zfill(7)
    df_bruto['VALOR_BRUTO'] = pd.to_numeric(df_bruto['VALOR_BRUTO'], errors='coerce')
    df_bruto.dropna(subset=['VALOR_BRUTO'], inplace=True)

    # 5. Agregação (A GRANDE MUDANÇA)
    print("Agregando dados por município...")
    df_agregado = df_bruto.groupby(['IBGE_COD', 'MUNICIPIO', 'UF']).agg(
        total_pagamentos=('VALOR_BRUTO', 'count'),  # Renomeado para clareza
        soma_valor=('VALOR_BRUTO', 'sum'),
        media_valor=('VALOR_BRUTO', 'mean'),
        total_pessoas=('BENEFICIARIO_ID_HASH', 'nunique') # <-- A NOVA MÉTRICA
    ).reset_index()

    print("Tratamento e agregação concluídos.")
    return df_agregado

def detectar_outliers(df_agregado):
    """Aplica o Isolation Forest no dataframe agregado."""
    if df_agregado is None or df_agregado.empty:
        return None
        
    features = ['total_pagamentos', 'soma_valor']
    X = np.log1p(df_agregado[features])
    
    model = IsolationForest(contamination=0.01, random_state=42)
    model.fit(X)
    predicoes = model.predict(X)
    
    df_agregado['is_outlier'] = ['INCONSISTÊNCIA' if p == -1 else 'NORMAL' for p in predicoes]
    return df_agregado