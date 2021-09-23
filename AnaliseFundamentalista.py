import streamlit as st
import pandas as pd
import csv

import plotly.express as px
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.graph_objects as go
#import plotly.offline as pyo
from pandas_datareader import data as pdr
import yfinance as yf


# SETTING PAGE CONFIG TO WIDE MODE
st.set_page_config(layout="wide")

# Read CSV Dados Financeiros

@st.cache(persist=True)
def readDadosFinanceiros(f):
    df = pd.read_csv(f, sep=';', encoding='Latin1', decimal=',')
    df.rec_liq = df.rec_liq.astype(int)
    df.lucro_liq = df.lucro_liq.astype(int)
    df.EBITDA = df.EBITDA.astype(int)
    df.caixa = df.caixa.astype(int)
    df.pl = df.pl.astype(int)
    df.div_total = df.div_total.astype(int)
    df = df.fillna('')
    return df

#financ = readDadosFinanceiros(f = r'C:\Users\Renato\Documents\_R Projetos\Projeto GetDFPData\DadosFinanceiros.csv')
financ = readDadosFinanceiros(f='https://raw.githubusercontent.com/renatosts/AnaliseFundamentalista/main/DadosFinanceiros.csv')

row1_1, row1_2 = st.columns([2,3])

with row1_1:
    #st.title('Análise Fundamentalista')
    # Prepara lista de empresas
    ticker_opcoes =  financ.segmento.str[:20] + ' - ' + financ.ticker.str[:4] + ' - ' + financ.nome
    ticker_opcoes = ticker_opcoes.drop_duplicates().sort_values()

    ticker_selecionado = st.selectbox('Selecione a empresa:', ticker_opcoes )
    ticker = ticker_selecionado.split(sep='-')[1].strip()
    empresa = ticker_selecionado.split(sep='-')[2].strip()

# FILTERING DATA
df = financ[financ.ticker.str.startswith(str.upper(ticker))]

df_aux = df[['ano', 'rec_liq', 'lucro_liq', 'margem_liq', 'EBITDA', 'div_liq', 'caixa', 'pl', 'div_total']]
df_aux.columns = ['Ano', 'Rec.Líq', 'Luc.Líq', 'Marg.Líq', 'EBITDA', 'Dív.Líq', 'Caixa', 'Patr.Líq', 'Dív.Total']
df_aux.reset_index(inplace=True, drop=True) 
df_aux = df_aux.set_index('Ano')

df_aux = df_aux.style.format('{:,}')

# EXIBE DATAFRAME
with row1_2:
    st.dataframe(df_aux)
    #st.table(df_aux)

with row1_1:
    st.write(f'{df.ticker.iloc[0]} - {df.pregao.iloc[0]}')
    st.write(f'IBovespa: {df.ibovespa.iloc[-1]} - {df.segmento.iloc[0]}')
    st.write(f'Governança: {df.listagem.iloc[0]}')
    site = df.site.iloc[0]
    if site[0:4] != 'http' and site != '':
        site = 'http://' + site
    st.write(site)


fig = make_subplots(rows=2, cols=2, 
                    shared_xaxes=True,
                    vertical_spacing=0.1,
                    specs=([[{'secondary_y': True}, {'secondary_y': True}],
                            [{'secondary_y': True}, {'secondary_y': True}]]))

fig.add_trace(
    go.Bar(x=df.ano, y=df.rec_liq, name='Receita Líquida', marker=dict(color="blue")),
    row=1, col=1)
fig.add_trace(
    go.Bar(x=df.ano, y=df.EBITDA, name='EBITDA', marker=dict(color="green")),
    row=1, col=1)

fig.add_trace(
    go.Bar(x=df.ano, y=df.lucro_liq, marker=dict(color="orange"), name='Lucro Líquido'), 
    secondary_y=False,
    row=1, col=2)
fig.add_trace(
    go.Scatter(x=df.ano, y=df.margem_liq, marker=dict(color="crimson"), name='Margem Líquida'), 
    secondary_y=True,
    row=1, col=2)

fig.add_trace(
    go.Bar(x=df.ano, y=df.div_liq, marker=dict(color="red"), showlegend=True, name='Dívida Líquida'),
    row=2, col=1)

fig.add_trace(
    go.Bar(x=df.ano, y=df.pl, name='Patr.Líq', marker=dict(color="yellow")),
    row=2, col=2)
fig.add_trace(
    go.Bar(x=df.ano, y=df.caixa, name='Caixa', marker=dict(color="cyan")),
    row=2, col=2)


fig.update_layout(barmode='overlay', separators = '.,',)

fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1,
    xanchor="right",
    x=1))


st.plotly_chart(fig, use_container_width=True)


# Cotações

ticker_b3 = financ.ticker[(financ.ticker.str.startswith(ticker))].iloc[0].split(sep=';')

for tck in ticker_b3:

    df = pdr.DataReader(f'{tck}.SA', data_source='yahoo', start=f'2010-01-01')
    
    #figd = make_subplots()

    fig = go.Figure(data=[
        go.Scatter(x=df.index, y=df["Adj Close"], marker=dict(color="darkgoldenrod"))])

    fig.update_layout(title=f'<b>{tck} (R$ {df["Adj Close"].iloc[-1]:,.2f})</b>')

    st.plotly_chart(fig)
