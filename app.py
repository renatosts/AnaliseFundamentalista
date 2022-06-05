from bs4 import BeautifulSoup
from datetime import datetime
from pandas_datareader import data as pdr
from plotly.subplots import make_subplots
from zipfile import ZipFile
import csv
import matplotlib.pyplot as plt
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import re
import requests as req
import streamlit as st
import yfinance as yf


# Read CSV Dados Financeiros

@st.cache(persist=True)
def readDadosFinanceiros(f):
    df = pd.read_csv(f, sep=';', encoding='Latin1', decimal=',')
    df.receita_liq = df.receita_liq / 1_000
    df.lucro_liq = df.lucro_liq / 1_000
    df.EBITDA = df.EBITDA / 1_000
    df.caixa = df.caixa / 1_000
    df.patr_liq = df.patr_liq / 1_000
    df.divida_total = df.divida_total / 1_000
    df.acoes = df.acoes.fillna(0)
    df.acoes = (df.acoes / 1_000).astype(int)
    df.dt_ref = pd.to_datetime(df.dt_ref).dt.strftime('%d/%m/%Y')
    df = df.fillna('')
    return df


def define_color(val):
    if val < 0:
        color = 'red'
    elif val > 0:
        color = 'green'
    else:
        color = 'gray'
    return 'color: %s' % color


def cria_cadastro():

    # Determina Cadastro
    cadastro = processa_base_cvm('FCA', 'geral')
    cadastro  = cadastro.groupby('CNPJ_Companhia')[['Codigo_CVM', 'Nome_Empresarial', 'Setor_Atividade', 'Pagina_Web']].last().reset_index()
    cadastro.columns = ['cnpj', 'cod_cvm', 'nome', 'segmento', 'site']
    cadastro.segmento = cadastro.segmento.str.replace('Emp. Adm. Part. - ', '', regex=False)
    cadastro.segmento = cadastro.segmento.str.replace('Emp. Adm. Part.-', '', regex=False)
    cadastro.nome = cadastro.nome.str.upper()

    # Determina tickers de negociação
    tickers = processa_base_cvm('FCA', 'valor_mobiliario')
    tickers = tickers[['CNPJ_Companhia', 'Codigo_Negociacao']].drop_duplicates().dropna()
    tickers = tickers[~tickers.Codigo_Negociacao.str.contains('*', regex=False)]
    tickers = tickers.groupby(['CNPJ_Companhia']).agg({'Codigo_Negociacao': ','.join}).reset_index()
    tickers.columns = ['cnpj', 'ticker']
    tickers.ticker = tickers.ticker.str.upper()

    cadastro = cadastro.merge(tickers, on='cnpj')


    # Quantidade de ações
    distribuicao_capital = processa_base_cvm('FRE', 'distribuicao_capital')
    distribuicao_capital = distribuicao_capital[distribuicao_capital['Quantidade_Total_Acoes_Circulacao'] > 0]
    distribuicao_capital = distribuicao_capital[distribuicao_capital['Percentual_Total_Acoes_Circulacao'] > 0]
    distribuicao_capital = distribuicao_capital.groupby('CNPJ_Companhia')[['Data_Referencia', 'Versao', 'Quantidade_Total_Acoes_Circulacao', 'Percentual_Total_Acoes_Circulacao']].last().reset_index()
    distribuicao_capital['acoes'] = (distribuicao_capital.Quantidade_Total_Acoes_Circulacao / (distribuicao_capital.Percentual_Total_Acoes_Circulacao / 100)).astype('int64')
    distribuicao_capital.columns = ['cnpj', 'dt_ref', 'versao', 'acoes_circul', 'free_float', 'acoes']
    distribuicao_capital = distribuicao_capital[['cnpj', 'acoes', 'free_float']]

    cadastro = cadastro.merge(distribuicao_capital, on='cnpj', how='left')
    cadastro['acoes'] = cadastro['acoes'].fillna(0).astype('int64')


    # Governança
    governanca = processa_base_cvm('FCA', 'valor_mobiliario')
    governanca = governanca.groupby('CNPJ_Companhia')[['Data_Referencia', 'Versao', 'Segmento']].last().reset_index()
    governanca = governanca[['CNPJ_Companhia', 'Segmento']]
    governanca.columns = ['cnpj', 'governanca']

    cadastro = cadastro.merge(governanca, on='cnpj', how='left')

    return cadastro


def download_arquivos_CVM(tipo):

    # Acessa site CVM para verificar arquivos anuais para download

    URL_CVM = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo}/DADOS/'

    # Verifica data do último download

    with open('controle_download.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            data_ultimo_download = row[0]

    # Acessa site CVM para verificar arquivos mensais para download

    try:

        resp = req.get(URL_CVM)

    except Exception as e:

        print(e)


    bs = BeautifulSoup(resp.text, 'html.parser')

    pre = bs.find('pre')

    nome = []
    for m in re.finditer(rf"{tipo.lower()}_cia_aberta_\w*.zip", pre.text):  
        nome.append(m.group(0).rstrip('.zip'))
    
    last_mod = []
    for m in re.finditer(r"\d{2}-\w{3}-\d{4} \w{2}:\w{2}", pre.text):  
        last_mod.append(m.group(0))

    # df contém a lista dos arquivos a serem baixados
    df = pd.DataFrame()
    df['nome'] = nome
    df['last_mod'] = last_mod
    df['ano'] = df.nome.str[15:19]
    df['last_mod'] = pd.to_datetime(df.last_mod)

    df = df[df['last_mod'] > data_ultimo_download]

    for arq in df['nome']:
         print('Download do arquivo:', arq)
         download_url(URL_CVM + arq + '.zip', dest_folder=rf'Base_CVM\{tipo}')


def download_url(url: str, dest_folder: str):


    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = url.split('/')[-1].replace(' ', '_')  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    r = req.get(url, stream=True)
    if r.ok:
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print('Download failed: status code {}\n{}'.format(r.status_code, r.text))


def processa_base_cvm(tipo, arquivo):

    primeiro_ano = datetime.today().year - 9

    df = pd.DataFrame()

    pasta = f'Base_CVM\\{tipo}\\'

    for filezip in os.listdir(pasta):

        ano = int(re.search('[0-9]+', filezip).group(0))

        if ano >= primeiro_ano:

            with ZipFile(pasta + filezip) as zip:

                nome_arq = f'{tipo.lower()}_cia_aberta_{arquivo}_{ano}.csv'
                with zip.open(nome_arq) as f:
                    temp = pd.read_csv(f, encoding='Latin-1', delimiter=';')
                    df = pd.concat([df, temp])

    return df


def processa_dados_financeiros(tipo):

    balanco_ativo = processa_base_cvm(tipo, 'BPA_con')
    balanco_passivo = processa_base_cvm(tipo, 'BPP_con')
    dre = processa_base_cvm(tipo, 'DRE_con')
    dra = processa_base_cvm(tipo, 'DRA_con')
    dfc_md = processa_base_cvm(tipo, 'DFC_MD_con')
    dfc_mi = processa_base_cvm(tipo, 'DFC_MI_con')

    df = pd.concat([balanco_ativo, balanco_passivo, dre, dra, dfc_md, dfc_mi])

    df = df[df.ORDEM_EXERC == 'ÚLTIMO']       
    df.VL_CONTA = df.VL_CONTA.astype(float)
    df['ano'] = df['DT_REFER'].str[:4].astype(int)

    df['tipo'] = tipo

    df.columns = ['cnpj', 'dt_ref', 'versao', 'nome', 'cod_cvm', 'grupo_dfp', 'moeda', 'escala_moeda',
                  'ordem_exerc', 'dt_fim_exerc', 'cod_conta', 'desc_conta', 'valor', 'sit_conta_fixa',
                  'dt_ini_exerc', 'ano', 'form']

    df.dt_ref = pd.to_datetime(df.dt_ref)

    return df


def cria_base_Dados_Financeiros():


    base_download_cvm = ['DFP', 'ITR', 'FRE', 'FCA']

    for tipo in base_download_cvm:
        print(tipo)
        download_arquivos_CVM(tipo)

    # Atualiza data do último download

    data_ultimo_download = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with open('controle_download.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([data_ultimo_download])

    # Gera cadastro

    cadastro = cria_cadastro()

    # Dados Financeiros

    # Processa DFP

    dfp = processa_dados_financeiros('DFP')

    ultimo_ano_dfp = dfp.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'dt_ref', 'ano']]
    ultimo_ano_dfp.columns = ['cod_cvm', 'ultimo_dfp_dt_ref', 'ultimo_dfp_ano']

    ano_anterior = dfp.ano.max() - 1

    empresas_ano_anterior = ultimo_ano_dfp[ultimo_ano_dfp.ultimo_dfp_ano >= ano_anterior]
    dfp = dfp[dfp.cod_cvm.isin(empresas_ano_anterior.cod_cvm)]

    # Processa ITR (deixa somente último ITR se houver)

    itr = processa_dados_financeiros('ITR')

    ultimo_itr = itr.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'ano', 'dt_ref', 'versao']]
    ultimo_itr = ultimo_itr.merge(ultimo_ano_dfp, on='cod_cvm', how='left')

    ultimo_itr = ultimo_itr[ultimo_itr.dt_ref > ultimo_itr.ultimo_dfp_dt_ref]
    ultimo_itr = ultimo_itr[((ultimo_itr.ultimo_dfp_dt_ref.isna()) & (ultimo_itr.ano > ano_anterior)) |
                            (ultimo_itr.ano > ano_anterior)]


    itr['chave'] = itr.cod_cvm.astype(str) + itr.dt_ref.dt.strftime('%Y-%m-%d') + itr.versao.astype(str)
    ultimo_itr['chave'] = ultimo_itr.cod_cvm.astype(str) + ultimo_itr.dt_ref.dt.strftime('%Y-%m-%d') + ultimo_itr.versao.astype(str)

    itr = itr[itr.chave.isin(ultimo_itr.chave)]

    del itr['chave']


    # Junta DFP com último ITR
    financ = pd.concat([dfp, itr])

    # Seleciona saldos de interesse

    contas_selec = ['1', '1.01.01', '1.01.02', '2.03', '3.01', '3.03',
                    '3.05', '3.11', '2.01.04', '2.02.01']

    # idx saldos
    idx_saldos = financ.cod_conta.isin(contas_selec)

    # idx deprec
    idx_deprec = (financ.cod_conta.str.startswith('6.01')
                ) & (
                financ.desc_conta.str.lower().str.contains('deprec|amortiz', regex=True))


    saldos = financ[idx_saldos]

    deprec = financ[idx_deprec]
    deprec = deprec.groupby(['form', 'cod_cvm', 'ano', 'dt_ref']).sum('valor').reset_index()
    deprec['deprec_amortiz'] = deprec['valor']
    del deprec['valor']

    # Gera arquivo de saída

    df = saldos.pivot_table(values='valor', index=['form', 'cod_cvm', 'ano', 'dt_ref'], columns='cod_conta', fill_value=0).reset_index()

    df = df.merge(deprec, on=['form', 'cod_cvm', 'ano', 'dt_ref'], how='left')
    df = df.fillna(0)

    df['ativo'] = df['1']
    df['caixa'] = df['1.01.01'] + df['1.01.02']
    df['divida_curto_prazo'] = df['2.01.04']
    df['divida_longo_prazo'] = df['2.02.01']
    df['divida_total'] = df['divida_curto_prazo'] + df['divida_longo_prazo']
    df['patr_liq'] = df['2.03']
    df['receita_liq'] = df['3.01']
    df['lucro_bruto'] = df['3.03']
    df['lucro_liq'] = df['3.11']
    df['EBIT'] = df['3.05']

    df['endivid_taxa'] = round(df['divida_total'] / df['ativo'], 2)
    df['margem_liq'] = round(df['lucro_liq'] / df['receita_liq'], 4)
    df['EBITDA'] = round(df['EBIT'] + df['deprec_amortiz'], 2)
    df['divida_liq'] = round((df['divida_total'] - df['caixa']) / df['EBITDA'], 2)

    df = df[['form', 'cod_cvm', 'ano', 'dt_ref',
        'ativo', 'patr_liq', 'receita_liq', 'lucro_bruto', 'lucro_liq', 'EBIT',
        'divida_curto_prazo', 'divida_longo_prazo', 'caixa', 'divida_total',
        'endivid_taxa', 'margem_liq', 'deprec_amortiz', 'EBITDA', 'divida_liq']]


    df = df.merge(cadastro, on='cod_cvm')

    df = df[['segmento', 'nome', 'cod_cvm', 'site', 'ticker', 'ano', 'form', 'dt_ref',
        'ativo', 'patr_liq', 'receita_liq', 'lucro_bruto', 'lucro_liq', 'EBIT',
        'deprec_amortiz', 'EBITDA', 'margem_liq', 'divida_curto_prazo', 'divida_longo_prazo',
        'caixa', 'divida_liq', 'divida_total', 'acoes', 'free_float', 'governanca']]


    df = df.sort_values(by=['nome', 'ano'])

    df.to_csv('DadosFinanceiros.csv', sep=';', decimal=',', index=False, encoding='Latin1')



# Procedimento Principal

st.set_page_config(
    layout='wide',
    page_icon='app.jpg',
    page_title='B3')


data_hoje = datetime.today().strftime('%Y-%m-%d')

with open('controle_base_dados_financeiros.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        data_base_dados_financeiros = row[0]

if data_hoje > data_base_dados_financeiros:

    with st.spinner('Processando base Dados Financeiros'):
        cria_base_Dados_Financeiros()

        # Atualiza data da criação da base
        with open('controle_base_dados_financeiros.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([data_hoje])


f = 'DadosFinanceiros.csv'

financ = readDadosFinanceiros(f)

row1_1, row1_2 = st.columns([1.3, 3])

with row1_1:
    # Prepara lista de empresas
    ticker_opcoes = financ.nome + ' ; ' + financ.ticker.str[:4]
    ticker_opcoes = ticker_opcoes.drop_duplicates().sort_values()

    ticker_selecionado = st.selectbox('Selecione a empresa:', ticker_opcoes )
    ticker = ticker_selecionado.split(sep=';')[1].strip()
    empresa = ticker_selecionado.split(sep=';')[0].strip()

# FILTERING DATA (limitando aos 10 últimos anos - tail)
df = financ[financ.ticker.str.startswith(str.upper(ticker))].tail(10).copy()
print(df)

qtd_acoes = df.acoes.iloc[0]

# Define para merge do cálculo do P/L diário
df['prox_ano'] = df.ano + 1

df_aux = df[['ano', 'form', 'receita_liq', 'lucro_liq', 'margem_liq', 'EBITDA', 'divida_liq', 'caixa', 'patr_liq', 'divida_total', 'dt_ref']]

df_aux.reset_index(inplace=True, drop=True) 
df_aux = df_aux.set_index('ano')

df_aux.columns = ['Dem', 'Rec.Líq', 'Luc.Líq', 'Marg.Líq', 'EBITDA', 'Dív.Líq', 'Caixa', 'Patr.Líq', 'Dív.Total', 'Data']

df_aux = df_aux.style.format(thousands=".",
                             decimal = ",",
                             formatter={'Rec.Líq': '{:,.1f}',
                                        'Luc.Líq': '{:,.1f}',
                                        'Marg.Líq': '{:.1%}',
                                        'EBITDA': '{:,.1f}',
                                        'Caixa': '{:,.1f}',
                                        'Patr.Líq': '{:,.1f}',
                                        'Dív.Total': '{:,.1f}',
                                        'Dív.Líq': '{:.1f}'}).applymap(define_color, subset=['Luc.Líq', 'Marg.Líq', 'EBITDA', 'Dív.Líq'])


# EXIBE DATAFRAME
with row1_1:
    #st.write(f'{df.ticker.iloc[0]} - {df.pregao.iloc[0]}')
    st.write(f'{df.nome.iloc[0]}')
    st.write(f'{df.ticker.iloc[0]}')
    #st.write(f'IBovespa: {df.ibovespa.iloc[-1]} - {df.segmento.iloc[0]}')
    st.write(f'{df.segmento.iloc[0]}')
    st.write(f'Governança: {df.governanca.iloc[0]}')
    site = df.site.iloc[0]
    if site[0:4] != 'http' and site != '':
        site = 'http://' + site
    st.write(site)

with row1_2:
    st.table(df_aux)


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
    go.Scatter(x=df.ano, y=df.margem_liq*100, marker=dict(color="crimson"), name='Margem Líquida'), 
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
