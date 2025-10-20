# BPC Insight: Análise de Dados do Benefício de Prestação Continuada

## 📖 Sobre o Projeto

Este projeto é uma plataforma de análise de dados desenvolvida em Python para extrair, tratar, analisar e visualizar os dados públicos do Benefício de Prestação Continuada (BPC).

O sistema utiliza um pipeline de dados automatizado para carregar os dados históricos e incrementais do Portal da Transparência diretamente em um banco de dados **PostgreSQL**. O dashboard interativo, construído com **Streamlit**, consome esses dados para permitir análises temporais e geográficas, incluindo a detecção de outliers com **Scikit-learn** e a contagem de beneficiários únicos.

## 🚀 Tecnologias Utilizadas

-   **Linguagem:** Python
-   **Banco de Dados:** PostgreSQL
-   **Conexão DB:** SQLAlchemy, Psycopg2
-   **Processamento de Dados:** Pandas
-   **Machine Learning:** Scikit-learn (Isolation Forest)
-   **Dashboard:** Streamlit
-   **Gráficos:** Plotly Express

---

## ⚙️ Pré-requisitos (Configuração Obrigatória)

Antes de executar, você precisa ter o **PostgreSQL** instalado e rodando.

1.  [Baixe e instale o PostgreSQL](https://www.postgresql.org/download/windows/).
2.  Durante a instalação, defina uma senha para o superusuário `postgres`.
3.  Abra o **pgAdmin 4** (ferramenta que vem com o PostgreSQL) e crie um novo banco de dados vazio chamado **`bpc_db`** com a codificação **`UTF8`**.

---

## 🏃 Como Executar o Projeto

### 1. Preparação do Ambiente

```bash
# Clone este repositório
git clone [https://github.com/SEU-USUARIO/SEU-REPOSITORIO.git](https://github.com/SEU-USUARIO/SEU-REPOSITORIO.git)
cd SEU-REPOSITORIO

# Crie e ative o ambiente virtual
python -m venv venv
venv\Scripts\activate

# Instale todas as dependências
pip install -r requirements.txt