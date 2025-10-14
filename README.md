# BPC Insight: Análise de Dados do Benefício de Prestação Continuada

## 📖 Sobre o Projeto

Este projeto é uma plataforma de análise de dados desenvolvida em Python para extrair, tratar, analisar e visualizar os dados públicos do Benefício de Prestação Continuada (BPC) do Portal da Transparência do Brasil.

O objetivo principal é automatizar a coleta de dados mensais e utilizar técnicas de Machine Learning (Isolation Forest) para identificar municípios com padrões de pagamento atípicos ou inconsistentes, servindo como uma ferramenta de apoio para gestores e pesquisadores.

## 🚀 Tecnologias Utilizadas

- **Python:** Linguagem principal do projeto.
- **Pandas:** Para manipulação e tratamento dos dados.
- **Scikit-learn:** Para a aplicação do modelo de detecção de outliers (Isolation Forest).
- **Streamlit:** Para a construção do dashboard web interativo.
- **Plotly Express:** Para a criação dos gráficos.
- **Requests:** Para realizar o download dos dados da web.

## ⚙️ Como Executar

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/SEU-USUARIO/SEU-REPOSITORIO.git](https://github.com/SEU-USUARIO/SEU-REPOSITORIO.git)
    cd SEU-REPOSITORIO
    ```

2.  **Crie e ative o ambiente virtual:**
    ```bash
    python -m venv venv
    venv\Scripts\activate
    ```

3.  **Instale as dependências:**
    (Crie um arquivo `requirements.txt` e adicione o nome das bibliotecas, uma por linha)
    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute o pipeline de dados:**
    (Este comando irá baixar e processar todos os dados históricos)
    ```bash
    python run_pipeline.py
    ```

5.  **Inicie o dashboard:**
    ```bash
    streamlit run app.py
    ```

## ✨ Funcionalidades

- Pipeline automatizado para carga histórica e atualizações mensais.
- Dashboard interativo com filtros por ano, mês e estado.
- Análise comparativa de gastos entre diferentes localidades e períodos.
- Detecção de anomalias com Machine Learning.