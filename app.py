# app.py (versão com "Total de Pessoas")

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --- CONFIGURAÇÃO DA CONEXÃO ---
DB_USER = 'postgres'
DB_PASSWORD = 'Apap29**' # Sua senha
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'bpc_db'
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL) 

# --- Configuração da Página ---
st.set_page_config(layout="wide")
st.title("📊 BPC Insight: Análise de Dados por Município")

# --- Carregamento dos Dados ---
@st.cache_data
def carregar_dados_do_db(query):
    try:
        df = pd.read_sql_query(query, con=engine)
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ou consultar o banco de dados: {e}")
        return None

# Carrega os dados para popular os filtros iniciais
df_filtros = carregar_dados_do_db("SELECT DISTINCT \"ANO\", \"MES\", \"UF\" FROM bpc_analise")

if df_filtros is None:
    st.error(f"A tabela 'bpc_analise' não foi encontrada. Por favor, execute o script 'run_pipeline.py' primeiro.")
else:
    # --- BARRA LATERAL COM FILTROS ---
    st.sidebar.header("Filtros de Análise")
    
    anos_selecionados = st.sidebar.multiselect("Ano(s):", options=sorted(df_filtros['ANO'].unique(), reverse=True), default=sorted(df_filtros['ANO'].unique(), reverse=True)[:1])
    meses_selecionados = st.sidebar.multiselect("Mês(es):", options=sorted(df_filtros['MES'].unique()), default=sorted(df_filtros['MES'].unique()))
    ufs_selecionadas = st.sidebar.multiselect("Estado(s) (UF):", options=sorted(df_filtros['UF'].unique()), default=sorted(df_filtros['UF'].unique())[:1])

    # --- Filtro de Município com "Todos" ---
    municipios_selecionados = []
    if ufs_selecionadas:
        query_municipios = f"""SELECT DISTINCT "MUNICIPIO" FROM bpc_analise WHERE "UF" IN ({str(ufs_selecionadas)[1:-1]})"""
        df_municipios = carregar_dados_do_db(query_municipios)
        opcoes_municipios = ['Todos os Municípios'] + sorted(df_municipios['MUNICIPIO'].unique())
        municipios_selecionados = st.sidebar.multiselect("Município(s):", options=opcoes_municipios, default=['Todos os Municípios'])

    # --- Monta a Query SQL com base nos filtros ---
    where_clauses = []
    if anos_selecionados: where_clauses.append(f"\"ANO\" IN ({str(anos_selecionados)[1:-1]})")
    if meses_selecionados: where_clauses.append(f"\"MES\" IN ({str(meses_selecionados)[1:-1]})")
    if ufs_selecionadas: where_clauses.append(f"\"UF\" IN ({str(ufs_selecionadas)[1:-1]})")

    if municipios_selecionados and 'Todos os Municípios' not in municipios_selecionados:
        where_clauses.append(f"\"MUNICIPIO\" IN ({str(municipios_selecionados)[1:-1]})")
    
    df_filtrado = None
    if where_clauses:
        query_principal = f"SELECT * FROM bpc_analise WHERE {' AND '.join(where_clauses)}"
        df_filtrado = carregar_dados_do_db(query_principal)

    # --- PAINEL PRINCIPAL ---
    st.header(f"Análise para o Período e Localidades Selecionadas")

    if df_filtrado is None or df_filtrado.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
    else:
        df_filtrado['DATA'] = pd.to_datetime(df_filtrado['ANO'].astype(str) + '-' + df_filtrado['MES'].astype(str) + '-01')
        
        mostrar_por_municipio = municipios_selecionados and 'Todos os Municípios' not in municipios_selecionados
        chave_agrupamento = ['DATA', 'MUNICIPIO'] if mostrar_por_municipio else ['DATA', 'UF']
        cor_grafico = 'MUNICIPIO' if mostrar_por_municipio else 'UF'
        
        # Agrega os dados para os gráficos, incluindo a nova métrica
        df_temporal = df_filtrado.groupby(chave_agrupamento).agg(
            total_pagamentos=('total_pagamentos', 'sum'),
            soma_valor=('soma_valor', 'sum'),
            total_pessoas=('total_pessoas', 'sum') # <-- NOVA MÉTRICA AQUI
        ).reset_index().sort_values(by='DATA')
        
        
        st.subheader("Evolução Mensal")

        # --- NOVO GRÁFICO: QUANTIDADE DE PESSOAS ---
        fig_pessoas = px.line(
            df_temporal, 
            x="DATA", 
            y="total_pessoas", 
            color=cor_grafico, 
            markers=True, 
            title="Evolução da Quantidade de Pessoas (Únicas)",
            labels={'DATA': 'Mês/Ano', 'total_pessoas': 'Total de Pessoas Únicas', cor_grafico: 'Localidade'}
        )
        st.plotly_chart(fig_pessoas, use_container_width=True)

        # --- Gráfico de Valor Pago (sem alteração) ---
        fig_valor = px.line(
            df_temporal, 
            x="DATA", 
            y="soma_valor", 
            color=cor_grafico, 
            markers=True, 
            title="Evolução do Valor Total Pago (R$)",
            labels={'DATA': 'Mês/Ano', 'soma_valor': 'Valor Total Pago (R$)', cor_grafico: 'Localidade'}
        )
        st.plotly_chart(fig_valor, use_container_width=True)