# app.py (versão com multi-seleção e gráfico comparativo)

import streamlit as st
import pandas as pd
import plotly.express as px
from config import ARQUIVO_CONSOLIDADO # Importa o nome do arquivo correto

# --- Configuração da Página ---
st.set_page_config(layout="wide")
st.title("🔎 BPC Insight: Análise Comparativa de Inconsistências")

# --- Carregamento dos Dados ---
@st.cache_data # Isso acelera o carregamento em execuções futuras
def carregar_dados():
    """
    Carrega os dados consolidados do pipeline.
    """
    try:
        df = pd.read_csv(ARQUIVO_CONSOLIDADO)
        return df
    except FileNotFoundError:
        return None

df_final = carregar_dados()

# --- Verificação e Exibição do Dashboard ---
if df_final is None:
    st.error(f"Arquivo '{ARQUIVO_CONSOLIDADO}' não encontrado. Por favor, execute o script 'run_pipeline.py' primeiro para gerar os dados.")
else:
    # --- BARRA LATERAL COM FILTROS ---
    st.sidebar.header("Filtros de Análise")
    
    # Filtro de Ano (multi-seleção)
    anos_disponiveis = sorted(df_final['ANO'].unique(), reverse=True)
    anos_selecionados = st.sidebar.multiselect("Selecione o(s) Ano(s):", options=anos_disponiveis, default=anos_disponiveis[0:1])
    
    # Filtro de Mês (multi-seleção)
    meses_disponiveis = sorted(df_final['MES'].unique())
    meses_selecionados = st.sidebar.multiselect("Selecione o(s) Mês(es):", options=meses_disponiveis, default=meses_disponiveis)

    # Filtro de Estado (UF) (multi-seleção)
    ufs_disponiveis = sorted(df_final['UF'].unique())
    ufs_selecionadas = st.sidebar.multiselect(
        "Selecione o(s) Estado(s) (UF):",
        options=ufs_disponiveis,
        default=ufs_disponiveis[:5] # Seleciona os 5 primeiros por padrão
    )
    
    # Filtro de Status (Outlier/Normal)
    status_selecionado = st.sidebar.radio(
        "Mostrar municípios:",
        options=['Todos', 'Apenas com Inconsistência', 'Apenas Normais'],
        index=0 # 'Todos' como padrão para a visão comparativa
    )

    # --- Aplicação dos Filtros ---
    df_filtrado = df_final[
        (df_final['ANO'].isin(anos_selecionados)) & 
        (df_final['MES'].isin(meses_selecionados)) &
        (df_final['UF'].isin(ufs_selecionadas))
    ]
    
    if status_selecionado == 'Apenas com Inconsistência':
        df_filtrado = df_filtrado[df_filtrado['is_outlier'] == 'INCONSISTÊNCIA']
    elif status_selecionado == 'Apenas Normais':
        df_filtrado = df_filtrado[df_filtrado['is_outlier'] == 'NORMAL']
    
    # --- PAINEL PRINCIPAL ---
    st.header(f"Análise Comparativa para o Período Selecionado")

    if df_filtrado.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
    else:
        # Métricas Chave do período
        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Municípios na Seleção", f"{df_filtrado['IBGE_COD'].nunique():,}")
        col2.metric("Valor Total Pago no Período", f"R$ {df_filtrado['soma_valor'].sum():,.2f}")
        col3.metric("Total de Inconsistências", f"{len(df_filtrado[df_filtrado['is_outlier'] == 'INCONSISTÊNCIA']):,}")

        # --- NOVO GRÁFICO COMPARATIVO POR ESTADO E ANO ---
        st.subheader("Comparativo de Valor Pago por Estado e Ano")
        
        # Agrupa os dados por UF e Ano para o gráfico
        df_grafico = df_filtrado.groupby(['UF', 'ANO'])['soma_valor'].sum().reset_index()

        fig_bar = px.bar(
            df_grafico,
            x="UF",
            y="soma_valor",
            color="ANO",
            barmode="group", # Cria barras agrupadas por ano
            title="Valor Total Pago (R$) por Estado e Ano",
            labels={'soma_valor': 'Soma do Valor Pago (R$)', 'UF': 'Estado', 'ANO': 'Ano'}
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # --- Tabela de Dados Detalhada ---
        st.subheader("Dados Detalhados dos Municípios")
        st.dataframe(df_filtrado.style.format({
            'soma_valor': 'R$ {:,.2f}',
            'media_valor': 'R$ {:,.2f}',
            'total_beneficios': '{:,}',
        }))