# run_monitor.py
import pandas as pd
from datetime import datetime
import os
from data_pipeline import baixar_bpc, tratar_e_agregar, detectar_outliers
from config import ARQUIVO_CONSOLIDADO, ARQUIVO_DE_LOG

def executar_atualizacao():
    """
    Verifica o último mês processado e baixa, processa e anexa novos dados.
    """
    print("--- INICIANDO ROTINA DE MONITORAMENTO E ATUALIZAÇÃO ---")
    
    # 1. Descobre o que precisa ser processado
    try:
        with open(ARQUIVO_DE_LOG, 'r') as f:
            ultimo_processado = f.read().strip()
            ano_log, mes_log = map(int, ultimo_processado.split('-'))
    except FileNotFoundError:
        print("AVISO: Arquivo de log não encontrado. Execute a carga histórica primeiro.")
        return

    # Calcula o próximo mês a ser verificado
    proximo_ano, proximo_mes = (ano_log, mes_log + 1) if mes_log < 12 else (ano_log + 1, 1)
    
    print(f"Último processado: {mes_log}/{ano_log}. Verificando por novos dados a partir de {proximo_mes}/{proximo_ano}...")

    # 2. Tenta baixar os novos dados
    df_novo_bruto = baixar_bpc(proximo_ano, proximo_mes)
    
    if df_novo_bruto is None or df_novo_bruto.empty:
        print("Nenhum dado novo encontrado para este período. Encerrando.")
        return

    # 3. Processa e anexa os novos dados
    print(">>> Novo dado detectado! Processando...")
    df_novo_agregado = tratar_e_agregar(df_novo_bruto)
    df_novo_agregado['ANO'] = proximo_ano
    df_novo_agregado['MES'] = proximo_mes
    
    print(">>> Anexando novos dados ao arquivo consolidado...")
    df_novo_agregado.to_csv(ARQUIVO_CONSOLIDADO, mode='a', header=not os.path.exists(ARQUIVO_CONSOLIDADO), index=False)

    print(">>> Re-executando a detecção de outliers em todo o conjunto de dados...")
    df_completo = pd.read_csv(ARQUIVO_CONSOLIDADO)
    df_final_atualizado = detectar_outliers(df_completo)
    df_final_atualizado.to_csv(ARQUIVO_CONSOLIDADO, index=False)

    # 4. Atualiza o log
    with open(ARQUIVO_DE_LOG, 'w') as f:
        f.write(f"{proximo_ano}-{proximo_mes}")
        
    print(f"--- ATUALIZAÇÃO PARA {proximo_mes}/{proximo_ano} CONCLUÍDA COM SUCESSO! ---")

if __name__ == "__main__":
    executar_atualizacao()