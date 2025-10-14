# run_pipeline.py
import pandas as pd
from datetime import datetime
from data_pipeline import baixar_bpc, tratar_e_agregar, detectar_outliers
from config import ANO_INICIAL, ARQUIVO_CONSOLIDADO, ARQUIVO_DE_LOG

def executar_carga_historica():
    """
    Baixa, trata e consolida TODOS os dados do BPC desde o ano inicial até hoje.
    ATENÇÃO: Este processo pode demorar MUITO tempo e consumir bastante internet.
    """
    print("--- INICIANDO CARGA HISTÓRICA COMPLETA ---")
    
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month
    
    lista_de_dfs_mensais = []

    # Loop por todos os anos desde o início
    for ano in range(ANO_INICIAL, ano_atual + 1):
        # Para o ano atual, só processa até o mês anterior
        limite_mes = mes_atual if ano == ano_atual else 13
        
        # Loop por todos os meses
        for mes in range(1, limite_mes):
            df_bruto = baixar_bpc(ano, mes)
            if df_bruto is not None and not df_bruto.empty:
                df_agregado = tratar_e_agregar(df_bruto)
                
                # Adiciona colunas de ano e mês para contexto
                df_agregado['ANO'] = ano
                df_agregado['MES'] = mes
                
                lista_de_dfs_mensais.append(df_agregado)

    if not lista_de_dfs_mensais:
        print("Nenhum dado foi baixado. Encerrando.")
        return

    print("\n>>> Consolidando todos os dados mensais...")
    df_consolidado = pd.concat(lista_de_dfs_mensais, ignore_index=True)

    print(">>> Iniciando detecção de outliers nos dados consolidados...")
    df_final = detectar_outliers(df_consolidado)

    print(f">>> Salvando arquivo final em '{ARQUIVO_CONSOLIDADO}'...")
    df_final.to_csv(ARQUIVO_CONSOLIDADO, index=False)
    
    # Salva o log do último mês processado com sucesso
    with open(ARQUIVO_DE_LOG, 'w') as f:
        f.write(f"{ano_atual}-{mes_atual-1}")

    print("--- CARGA HISTÓRICA CONCLUÍDA COM SUCESSO! ---")

if __name__ == "__main__":
    executar_carga_historica()