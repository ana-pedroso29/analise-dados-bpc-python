# run_pipeline.py (versão corrigida com lógica de criação/anexação)

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from data_pipeline import baixar_bpc, tratar_e_agregar, detectar_outliers
from config import ANO_INICIAL

# --- CONFIGURAÇÃO DA CONEXÃO COM O BANCO DE DADOS ---
DB_USER = 'postgres'
DB_PASSWORD = 'Apap29**' # Sua senha
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'bpc_db'
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL) 

def executar_pipeline_incremental():
    """
    Verifica o último dado no DB e baixa/processa apenas os meses faltantes.
    Cria a tabela na primeira execução.
    """
    print("--- INICIANDO PIPELINE INCREMENTAL ---")
    
    ano_inicial_carga = ANO_INICIAL
    mes_inicial_carga = 1
    
    if_exists_action = 'replace'
    
    try:
        query = 'SELECT MAX("ANO" * 100 + "MES") as ultimo_periodo FROM bpc_analise'
        df_ultimo = pd.read_sql(query, engine)
        
        if not df_ultimo.empty and df_ultimo['ultimo_periodo'].iloc[0] is not None:
            ultimo_periodo = int(df_ultimo['ultimo_periodo'].iloc[0])
            ultimo_ano = ultimo_periodo // 100
            ultimo_mes = ultimo_periodo % 100
            
            print(f"Último dado encontrado no banco de dados: {ultimo_mes}/{ultimo_ano}")
            ano_inicial_carga, mes_inicial_carga = (ultimo_ano, ultimo_mes + 1) if ultimo_mes < 12 else (ultimo_ano + 1, 1)
            
            if_exists_action = 'append'
            
    except Exception:
        print("Tabela 'bpc_analise' não encontrada. Iniciando carga histórica completa.")

    print(f"Iniciando download a partir de: {mes_inicial_carga}/{ano_inicial_carga}")

    ano_atual = datetime.now().year
    mes_atual = datetime.now().month
    
    lista_de_dfs_mensais = []

    for ano in range(ano_inicial_carga, ano_atual + 1):
        start_month = mes_inicial_carga if ano == ano_inicial_carga else 1
        end_month = mes_atual if ano == ano_atual else 13
        
        for mes in range(start_month, end_month):
            df_bruto = baixar_bpc(ano, mes)
            if df_bruto is not None and not df_bruto.empty:
                df_agregado = tratar_e_agregar(df_bruto) 
                df_agregado['ANO'] = ano
                df_agregado['MES'] = mes
                lista_de_dfs_mensais.append(df_agregado)

    if not lista_de_dfs_mensais:
        print("Nenhum dado novo para processar. O banco de dados já está atualizado.")
        return

    print(f"\n>>> Consolidando {len(lista_de_dfs_mensais)} novo(s) mês(es) de dados...")
    df_novos_dados = pd.concat(lista_de_dfs_mensais, ignore_index=True)

    print(">>> Iniciando detecção de outliers nos novos dados...")
    df_final = detectar_outliers(df_novos_dados)

    print(f">>> Salvando dados na tabela 'bpc_analise' (Ação: {if_exists_action})...")
    df_final.to_sql(
        name='bpc_analise',
        con=engine,
        if_exists=if_exists_action,
        index=False
    )
    
    print("--- PIPELINE CONCLUÍDO COM SUCESSO! ---")

if __name__ == "__main__":
    executar_pipeline_incremental()