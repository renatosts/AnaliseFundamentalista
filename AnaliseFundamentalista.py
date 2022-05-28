import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from pandas_datareader import data as pdr
import yfinance as yf

# SETTING PAGE CONFIG TO WIDE MODE
st.set_page_config(
    layout='wide',
    page_icon='app.jpg',
    page_title='B3')

# Read CSV Dados Financeiros

@st.cache(persist=True)
def readDadosFinanceiros(f):
    df = pd.read_csv(f, sep=';', encoding='Latin1', decimal=',')
    df.receita_liq = df.receita_liq / 1000
    df.lucro_liq = df.lucro_liq / 1000
    df.EBITDA = df.EBITDA / 1000
    df.caixa = df.caixa / 1000
    df.patr_liq = df.patr_liq / 1000
    df.divida_total = df.divida_total / 1000
    df.acoes = df.acoes.fillna(0)
    df.acoes = (df.acoes / 1_000).astype(int)
    df.dt_ref = pd.to_datetime(df.dt_ref).dt.strftime('%d/%m/%Y')
    df = df.fillna('')
    return df

f ='https://raw.githubusercontent.com/renatosts/AnaliseFundamentalista/main/DadosFinanceiros.csv'
#f = r'C:\Users\Renato\Documents\_Projetos Github\CVM_Dados_Financeiros\DadosFinanceiros.csv'

financ = readDadosFinanceiros(f)

row1_1, row1_2 = st.columns([1.3, 3])

with row1_1:
    # Prepara lista de empresas
    ticker_opcoes =  financ.segmento.str[:20] + ' - ' + financ.ticker.str[:4] + ' - ' + financ.nome
    ticker_opcoes = ticker_opcoes.drop_duplicates().sort_values()

    ticker_selecionado = st.selectbox('Selecione a empresa:', ticker_opcoes )
    ticker = ticker_selecionado.split(sep='-')[1].strip()
    empresa = ticker_selecionado.split(sep='-')[2].strip()

# FILTERING DATA (limitando aos 12 últimos anos - tail)
df = financ[financ.ticker.str.startswith(str.upper(ticker))].tail(12).copy()
print(df)

qtd_acoes = df.acoes.iloc[0]
ult_dem = df.form.iloc[-1]
ult_dem_data = df.dt_ref.iloc[-1]

# Define para merge do cálculo do P/L diário
df['prox_ano'] = df.ano + 1

df_aux = df[['ano', 'form', 'receita_liq', 'lucro_liq', 'margem_liq', 'EBITDA', 'divida_liq', 'caixa', 'patr_liq', 'divida_total']]
df_aux = df_aux.tail(9)

df_aux.reset_index(inplace=True, drop=True) 
df_aux = df_aux.set_index('ano')

df_aux.columns = ['Dem', 'Rec.Líq', 'Luc.Líq', 'Marg.Líq', 'EBITDA', 'Dív.Líq', 'Caixa', 'Patr.Líq', 'Dív.Total']

df_aux = df_aux.style.format(thousands=".",
                             decimal = ",",
                             formatter={'Rec.Líq': '{:,.0f}',
                                        'Luc.Líq': '{:,.0f}',
                                        'Marg.Líq': '{:.1f}',
                                        'EBITDA': '{:,.0f}',
                                        'Caixa': '{:,.0f}',
                                        'Patr.Líq': '{:,.0f}',
                                        'Dív.Total': '{:,.0f}',
                                        'Dív.Líq': '{:.1f}'})


# EXIBE DATAFRAME
with row1_2:
    st.dataframe(df_aux)
    st.write('DFP: Demonstrações Financeiras Padronizadas (anual) / ITR: Informações Trimestrais')
    if ult_dem == 'ITR':
        st.write(f'ITR -> dados acumulados até {ult_dem_data}')

with row1_1:
    #st.write(f'{df.ticker.iloc[0]} - {df.pregao.iloc[0]}')
    st.write(f'{df.ticker.iloc[0]}')
    #st.write(f'IBovespa: {df.ibovespa.iloc[-1]} - {df.segmento.iloc[0]}')
    st.write(f'{df.segmento.iloc[0]}')
    st.write(f'Governança: {df.governanca.iloc[0]}')
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
    go.Bar(x=df.ano, y=df.receita_liq, name='Receita Líquida', marker=dict(color="blue")),
    row=1, col=1)
fig.add_trace(
    go.Bar(x=df.ano, y=df.EBITDA, name='EBITDA', marker=dict(color="green")),
    row=1, col=1)

fig.add_trace(
    go.Bar(x=df.ano, y=df.lucro_liq, marker=dict(color="yellow"), name='Lucro Líquido'), 
    secondary_y=False,
    row=1, col=2)
fig.add_trace(
    go.Scatter(x=df.ano, y=df.margem_liq, marker=dict(color="crimson"), name='Margem Líquida'), 
    secondary_y=True,
    row=1, col=2)

fig.add_trace(
    go.Bar(x=df.ano, y=df.divida_liq, marker=dict(color="red"), showlegend=True, name='Dívida Líquida'),
    row=2, col=1)

fig.add_trace(
    go.Bar(x=df.ano, y=df.patr_liq, name='Patr.Líq', marker=dict(color="purple")),
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

ticker_b3 = df.ticker[(df.ticker.str.startswith(ticker))].iloc[0].split(sep=',')

row1_1, row1_2 = st.columns([1, 1])


for tck in ticker_b3:

    try:
        df_b3 = pdr.DataReader(f'{tck}.SA', data_source='yahoo', start=f'2010-01-01')
    
        # Cálculo do P/L diário

        df_b3['Ano'] = df_b3.index.year
        df_b3['Data'] = df_b3.index
        df_b3 = df_b3.merge(df, how='left', left_on='Ano', right_on='prox_ano')
        df_b3['P/L'] = df_b3.Close / (df_b3.lucro_liq / qtd_acoes)

        # Limita intervalo do P/L entre -150 e 150
        df_b3.loc[df_b3['P/L'] > 150, 'P/L'] = 150
        df_b3.loc[df_b3['P/L'] < -150, 'P/L'] = -150

        df_pl_hist = df_b3.tail(500)

        var = (df_b3["Adj Close"].iloc[-1] / df_b3["Adj Close"].iloc[-2] - 1) * 100

        with row1_1:
            
            fig = go.Figure(data=[
                go.Scatter(x=df_b3["Data"], y=df_b3["Adj Close"], marker=dict(color="darkgoldenrod"))])
            fig.update_layout(title=f'<b>{tck}     R$ {df_b3["Adj Close"].iloc[-1]:,.2f}   <i> {var:,.2f} % </i></b>')

            st.plotly_chart(fig)

        with row1_2:
            
            fig = go.Figure(data=[
                go.Scatter(x=df_pl_hist["Data"], y=df_pl_hist["P/L"], marker=dict(color="green"))])
            fig.update_layout(title=f'<b>P/L Histórico Diário ({df_pl_hist["P/L"].iloc[-1]:,.2f})    (Ações: {qtd_acoes:,.0f})</b>')

            st.plotly_chart(fig)

    except:
        pass
