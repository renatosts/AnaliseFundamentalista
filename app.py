from bs4 import BeautifulSoup
from datetime import datetime
from plotly.subplots import make_subplots
import io
import pandas as pd
import plotly.graph_objects as go
import re
import requests as req
import sqlite3
import streamlit as st
import yfinance as yf
import zipfile

dbname = 'ANALISE_FUNDAMENTALISTA.db'

conn = sqlite3.connect(dbname)


def config_read(config_parametro):


    sql = f'''
        SELECT *
        FROM CONFIG
        WHERE config_parametro = '{config_parametro}'
    '''
    config = pd.read_sql(sql, conn)

    return config.config_valor.iloc[0]


def config_update(config_parametro, config_valor):


    sql = f'''
        UPDATE CONFIG
        SET config_valor = '{config_valor}'
        WHERE config_parametro = '{config_parametro}'
    '''
    conn.execute(sql)
    conn.commit()
    

def define_color(val):
    if val < 0:
        color = 'red'
    elif val > 0:
        color = 'green'
    else:
        color = 'gray'
    return 'color: %s' % color


def download_arquivos_CVM(dt_ultimo_download, tipo):


    # Donwload dos arquivos do site CVM

    URL_CVM = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo}/DADOS/'

    try:

        resp = req.get(URL_CVM)

    except Exception as e:

        print(e)


    bs = BeautifulSoup(resp.text, 'html.parser')

    pre = bs.find('pre')

    nome = []
    for m in re.finditer(rf'{tipo.lower()}_cia_aberta_\w*.(zip|csv)', pre.text):  
        nome.append(m.group(0))
   
    last_mod = []
    for m in re.finditer(r'\d{2}-\w{3}-\d{4} \w{2}:\w{2}', pre.text):  
        last_mod.append(m.group(0))

    # df contém a lista dos arquivos a serem baixados
    df = pd.DataFrame()
    df['nome'] = nome
    df['last_mod'] = last_mod
    df['ano'] = df.nome.str[-8:-4]
    df['last_mod'] = pd.to_datetime(df.last_mod)

    df = df[df['last_mod'] > dt_ultimo_download]
    df = df[df['nome'].str.endswith('.zip')]

    if tipo == 'DFP' or tipo == 'ITR':

        # Saldos
        arquivos = ['BPA', 'BPP', 'DRE', 'DRA', 'DFC_MD', 'DFC_MI']
        sufixos = ['_con', '_ind']
        novo_form, ult_transm = read_arquivos_cvm(URL_CVM, tipo, df['nome'], df['ano'], arquivos, sufixos) 
        if len(novo_form) > 0:
            with st.spinner(f'Processando base {tipo}'):
                processa_DFP_ITR_saldos(tipo, novo_form, df['ano'])
        # Últimas transmissões
        if len(ult_transm) > 0:
            processa_DFP_ITR_transmissoes(tipo, ult_transm, df['ano'])

    if tipo == 'FRE':

        # Capital - Quantidade de ações
        arquivos = ['distribuicao_capital']
        try:
            novo_form, ult_transm = read_arquivos_cvm(URL_CVM, tipo, df['nome'], df['ano'], arquivos) 
            if len(novo_form) > 0:
                processa_FRE_distribuicao_capital(tipo, novo_form, df['ano'])
        except:
            pass

        # Capital - Quantidade de ações
        arquivos = ['capital_social']
        try:
            novo_form, ult_transm = read_arquivos_cvm(URL_CVM, tipo, df['nome'], df['ano'], arquivos) 
            if len(novo_form) > 0:
                processa_FRE_capital_social(tipo, novo_form, df['ano'])
        except:
            pass


    if tipo == 'FCA':

        # Cadastro
        arquivos = ['geral']
        try:
            novo_form, ult_transm = read_arquivos_cvm(URL_CVM, tipo, df['nome'], df['ano'], arquivos) 
            if len(novo_form) > 0:
                processa_FCA_cadastro(tipo, novo_form, df['ano'])
        except:
            pass

        # Tickers e Governança
        arquivos = ['valor_mobiliario']
        try:
            novo_form, ult_transm = read_arquivos_cvm(URL_CVM, tipo, df['nome'], df['ano'], arquivos) 
            if len(novo_form) > 0:
                processa_FCA_tickers(tipo, novo_form, df['ano'])
        except:
            pass


def download_url(url):


    r = req.get(url)

    z = zipfile.ZipFile(io.BytesIO(r.content))

    return z


def elimina_itr_anteriores(df):


    dfp = df[df.form == 'DFP'].copy()
    ultimo_ano_dfp = dfp.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'dt_ref', 'ano']]
    ultimo_ano_dfp.columns = ['cod_cvm', 'ultimo_dfp_dt_ref', 'ultimo_dfp_ano']

    ano_anterior = dfp.ano.max() - 1

    empresas_ano_anterior = ultimo_ano_dfp[ultimo_ano_dfp.ultimo_dfp_ano >= ano_anterior]
    dfp = dfp[dfp.cod_cvm.isin(empresas_ano_anterior.cod_cvm)]


    itr = df[df.form == 'ITR'].copy()
    ultimo_itr = itr.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'ano', 'dt_ref', 'versao']]
    ultimo_itr = ultimo_itr.merge(ultimo_ano_dfp, on='cod_cvm', how='left')

    ultimo_itr = ultimo_itr[ultimo_itr.dt_ref > ultimo_itr.ultimo_dfp_dt_ref]
    ultimo_itr = ultimo_itr[((ultimo_itr.ultimo_dfp_dt_ref.isna()) & (ultimo_itr.ano > ano_anterior)) |
                            (ultimo_itr.ano > ano_anterior)]


    itr['chave'] = itr.cod_cvm.astype(str) + itr.dt_ref.dt.strftime('%Y-%m-%d') + itr.versao.astype(str)
    ultimo_itr['chave'] = ultimo_itr.cod_cvm.astype(str) + ultimo_itr.dt_ref.dt.strftime('%Y-%m-%d') + ultimo_itr.versao.astype(str)

    itr = itr[itr.chave.isin(ultimo_itr.chave)]

    del itr['chave']

    df = pd.concat([dfp, itr], ignore_index=True)

    return df


def empresas_por_segmento():


    # Empresas por segmento

    sql = '''
        SELECT *
        FROM CADASTRO
        ORDER BY segmento, nome
    '''
    df = pd.read_sql(sql, conn)

    segmentos = st.multiselect('Segmentos:', df.segmento.drop_duplicates())

    if segmentos != []:
        df = df[df.segmento.isin(segmentos)]

    df = df.set_index('segmento')

    df = df[['nome', 'ticker', 'cnpj', 'governanca', 'cod_cvm', 'site']]

    df.columns = ['Nome', 'Ticker', 'CNPJ', 'Governança', 'Cód.CVM', 'Site']

    st.table(df)


def exibe_dados_financeiros():


    financ = read_dados_financeiros()

    row1_1, row1_2 = st.columns([1.3, 3])

    with row1_1:
        # Prepara lista de empresas
        ticker_opcoes = financ.nome + ' ; ' + financ.ticker.str[:4]
        ticker_opcoes = ticker_opcoes.drop_duplicates().sort_values()

        ticker_selecionado = st.selectbox('Selecione a empresa:', ticker_opcoes )
        ticker = ticker_selecionado.split(sep=';')[1].strip()
        empresa = ticker_selecionado.split(sep=';')[0].strip()

    # FILTERING DATA (limitando aos 10 últimos anos - tail)
    df = financ[financ.ticker.str.startswith(str.upper(ticker))].copy()

    df.dt_ref = pd.to_datetime(df.dt_ref, dayfirst=True)

    df_lpa_trim = df[['dt_ref', 'LPA']]
    df_lpa_trim.columns = ['Date', 'LPA trim']

    df_lpa_anual = df[df.form == 'DFP'][['dt_ref', 'LPA']]
    df_lpa_anual.columns = ['Date', 'LPA anual']
    
    df = elimina_itr_anteriores(df)

    df = df.tail(10)
    print(df)

    df['data_form'] = df.dt_ref.dt.strftime('%d/%m/%Y')

    qtd_acoes = df.acoes.iloc[0]

    # Define para merge do cálculo do P/L diário
    df['prox_ano'] = df.ano + 1

    df.form = df.form + '/' + df.grupo.str[:1]

    df_aux = df[['ano', 'form', 'receita_liq', 'lucro_liq', 'margem_liq', 'EBITDA', 'divida_liq_ebitda', 'caixa', 'patr_liq', 'divida_total', 'data_form']]

    df_aux.reset_index(inplace=True, drop=True) 
    df_aux = df_aux.set_index('ano')

    # Ajuste para empresas com mais de um DFP no mesmo ano
    df_aux = df_aux.groupby('ano').last()

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
        st.write(f'CNPJ: {df.cnpj.iloc[0]} - CVM: {df.cod_cvm.iloc[0]}')
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
        go.Bar(x=df.ano, y=df.divida_liq_ebitda, marker=dict(color="red"), showlegend=True, name='Dívida Líquida'),
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

    row1_1, row1_2 = st.columns([1, 0.1])

    dt_hoje = datetime.today().strftime('%Y-%m-%d')

    for tck in ticker_b3:

        try:

            # Cálculo do P/L diário

            df_datas = pd.DataFrame(pd.date_range(start='2012-01-01', end=dt_hoje), columns=['Date'])

            df_b3 = yf.download(f'{tck}.SA', start=f'2012-01-01').reset_index()
            
            df_b3 = df_datas.merge(df_b3, on='Date', how='left')

            df_b3 = df_b3.merge(df_lpa_trim, on='Date', how='left')
            df_b3 = df_b3.merge(df_lpa_anual, on='Date', how='left')

            df_b3 = df_b3.ffill()

            df_b3['P/L trim'] = df_b3['Close'] / df_b3['LPA trim']
            df_b3['P/L anual'] = df_b3['Close'] / df_b3['LPA anual']

            df_b3 = df_b3.dropna()

            # Limita intervalo do P/L entre -150 e 150
            df_b3.loc[df_b3['P/L trim'] > 150, 'P/L trim'] = 150
            df_b3.loc[df_b3['P/L trim'] < -150, 'P/L trim'] = -150
            
            df_b3.loc[df_b3['P/L anual'] > 150, 'P/L anual'] = 150
            df_b3.loc[df_b3['P/L anual'] < -150, 'P/L anual'] = -150


            df_pl_hist = df_b3.tail(1000)
            
            #print(df_pl_hist)


            var = (df_b3["Close"].iloc[-1] / df_b3["Close"].iloc[-2] - 1) * 100

            with row1_1:
                
                fig = go.Figure(data=[
                    go.Scatter(x=df_b3["Date"], y=df_b3["Adj Close"], marker=dict(color="darkgoldenrod"))])

                fig.update_layout(title=f'<b>{tck}     R$ {df_b3["Adj Close"].iloc[-1]:,.2f}   <i> {var:,.2f} % </i></b>')

                st.plotly_chart(fig)


                fig = go.Figure()

                fig.add_trace(
                    go.Scatter(x=df_pl_hist["Date"], y=df_pl_hist["P/L anual"], marker=dict(color="green"), showlegend=True, name='DFP'))
                
                fig.add_trace(
                    go.Scatter(x=df_pl_hist["Date"], y=df_pl_hist["P/L trim"], marker=dict(color="red"), showlegend=True, name='ITR'))

                fig.update_layout(title=f'<b>P/L Histórico Diário ({df_pl_hist["P/L anual"].iloc[-1]:,.2f})    (Ações: {qtd_acoes:,.0f})</b>')

                st.plotly_chart(fig)

        except:
            pass


def gera_Dados_Financeiros():

    # Cadastro

    cadastro = pd.read_sql('SELECT * FROM FCA_CADASTRO', conn)
    del cadastro['ano']

    tickers = pd.read_sql('SELECT * FROM FCA_TICKERS', conn)
    del tickers['ano']

    capital = pd.read_sql('SELECT * FROM FRE_CAPITAL', conn)
    del capital['ano']

    cadastro = cadastro.merge(tickers, on='cnpj')

    cadastro = cadastro.merge(capital, on='cnpj', how='left')

    # Corrige tickers incorretos informados em FCA
    f = f'https://raw.githubusercontent.com/renatosts/AnaliseFundamentalista/main/AJUSTE_TICKERS.csv'
    ajuste_tickers = pd.read_csv(f, sep=';')

    for cod_cvm, ticker in zip(ajuste_tickers.cod_cvm, ajuste_tickers.ticker):
        cadastro.loc[cadastro.cod_cvm == cod_cvm, ['ticker']] = ticker

    cadastro = cadastro[cadastro.ticker != '.']

    # Ajusta tickers para fórmula Graham
    f = f'https://raw.githubusercontent.com/renatosts/AnaliseFundamentalista/main/AJUSTE_TICKERS_GRAHAM.csv'
    ajuste_graham = pd.read_csv(f, sep=';')

    cadastro['ticker_graham'] = cadastro['ticker']

    for cod_cvm, ticker in zip(ajuste_graham.cod_cvm, ajuste_graham.ticker):
        cadastro.loc[cadastro.cod_cvm == cod_cvm, ['ticker_graham']] = ticker

    cadastro = cadastro[cadastro.ticker_graham != '.']

    # Dados Financeiros

    # Processa DFP
    with st.spinner('Processando DFP'):

        dfp = pd.read_sql('SELECT * FROM DFP_SALDOS', conn)

        dfp.dt_ref = pd.to_datetime(dfp.dt_ref)
    
        dfp.ano = dfp.ano.astype(int)

        # Elimina empresas sem DFP no ano anterior
        ultimo_ano_dfp = dfp.groupby(['cod_cvm']).last().reset_index()[['cod_cvm', 'dt_ref', 'ano']]
        ultimo_ano_dfp.columns = ['cod_cvm', 'ultimo_dfp_dt_ref', 'ultimo_dfp_ano']

        ano_anterior = dfp.ano.max() - 1

        empresas_ano_anterior = ultimo_ano_dfp[ultimo_ano_dfp.ultimo_dfp_ano >= ano_anterior]
        dfp = dfp[dfp.cod_cvm.isin(empresas_ano_anterior.cod_cvm)]

    # Processa ITR (deixa somente ITR das empresas que têm DFP)

    with st.spinner('Processando ITR'):

        itr = pd.read_sql('SELECT * FROM ITR_SALDOS', conn)

        itr.dt_ref = pd.to_datetime(itr.dt_ref)

        itr.ano = itr.ano.astype(int)

        itr = itr[itr.cod_cvm.isin(empresas_ano_anterior.cod_cvm)]


        # Elimina saldos específicos do trimestre, deixando os saldos desde início do ano
        itr['dt_ini_exerc'] = pd.to_datetime(itr['dt_ini_exerc'])
        idx_itr = itr.groupby(['cod_cvm', 'dt_ref', 'versao']).min().reset_index()[['cod_cvm', 'dt_ref', 'versao', 'dt_ini_exerc']]

        itr_bp = itr[itr.dt_ini_exerc.isna()]
        itr_dre = itr.merge(idx_itr, on=['cod_cvm', 'dt_ref', 'versao', 'dt_ini_exerc'])

        itr = pd.concat([itr_bp, itr_dre])


    # Junta DFP com ITR
    
    with st.spinner('Preparando base Dados Financeiros'):
        
        financ = pd.concat([dfp, itr], ignore_index=True)

        financ.dt_ini_exerc = pd.to_datetime(financ.dt_ini_exerc)

        financ = financ.sort_values(['cnpj', 'dt_ref', 'versao', 'cod_conta'])

        financ['dt_ini_exerc'] = financ['dt_ini_exerc'].fillna(method='bfill')

        # Depreciação

        # idx deprec
        idx_deprec = (financ.cod_conta.str.startswith('6.01')
                    ) & (
                    financ.desc_conta.str.lower().str.contains('deprec|amortiz', regex=True))


        deprec = financ[idx_deprec]
        deprec = deprec.groupby(['form', 'cod_cvm', 'ano', 'dt_ref', 'versao', 'grupo', 'dt_ini_exerc']).sum('valor').reset_index()
        deprec['deprec_amortiz'] = deprec['valor']
        del deprec['valor']

    # Gera arquivo de saída


    df = financ.pivot_table(values='valor', index=['form', 'cod_cvm', 'ano', 'dt_ref', 'versao', 'grupo', 'dt_ini_exerc'], columns='cod_conta', fill_value=0).reset_index()

    #df.groupby(['form', 'cod_cvm', 'ano', 'dt_ref', 'versao']).max()

    # Junta dados de depreciação
    df = df.merge(deprec, on=['form', 'cod_cvm', 'ano', 'dt_ref', 'versao', 'grupo', 'dt_ini_exerc'], how='left')


    # Define lucro por ação (LPA básico)
    lpa_basico = financ[financ.cod_conta.str.startswith('3.99.01')].groupby(['cod_cvm', 'dt_ref', 'form', 'ano', 'versao', 'grupo', 'dt_ini_exerc']).mean().reset_index()
    lpa_basico.columns = ['cod_cvm', 'dt_ref', 'form', 'ano', 'versao', 'grupo', 'dt_ini_exerc', 'lpa_basico']

    df = df.merge(lpa_basico, on=['form', 'cod_cvm', 'ano', 'dt_ref', 'versao', 'grupo', 'dt_ini_exerc'], how='left')

    # Define lucro por ação (LPA diluído)
    lpa_diluido = financ[financ.cod_conta.str.startswith('3.99.02')].groupby(['cod_cvm', 'dt_ref', 'form', 'ano', 'versao', 'grupo', 'dt_ini_exerc']).mean().reset_index()
    lpa_diluido.columns = ['cod_cvm', 'dt_ref', 'form', 'ano', 'versao', 'grupo', 'dt_ini_exerc', 'lpa_diluido']

    df = df.merge(lpa_diluido, on=['form', 'cod_cvm', 'ano', 'dt_ref', 'versao', 'grupo', 'dt_ini_exerc'], how='left')

    df = df.fillna(0)

    # Se não existir lpa diluído, assume lpa básico
    df['LPA'] = df['lpa_diluido']
    df.loc[df.LPA == 0, 'LPA'] = df['lpa_basico']

    df['ativo'] = df['1']
    df['caixa'] = df['1.01.01'] + df['1.01.02']
    df['divida_curto_prazo'] = df['2.01.04']
    df['divida_longo_prazo'] = df['2.02.01']
    df['patr_liq'] = df['2.03']
    df['receita_liq'] = df['3.01']
    df['lucro_bruto'] = df['3.03']
    df['lucro_liq'] = df['3.11']
    df['EBIT'] = df['3.05']

    df['margem_liq'] = round(df['lucro_liq'] / df['receita_liq'], 4)
    df['EBITDA'] = round(df['EBIT'] + df['deprec_amortiz'], 2)

    df['divida_total'] = df['divida_curto_prazo'] + df['divida_longo_prazo']
    df['divida_liq'] = round(df['divida_total'] - df['caixa'], 2)

    df['divida_liq_ebitda'] = round(df['divida_liq'] / df['EBITDA'], 2)

    df = df[['form', 'cod_cvm', 'ano', 'dt_ref', 'versao', 'grupo', 'dt_ini_exerc',
        'ativo', 'patr_liq', 'receita_liq', 'lucro_bruto', 'lucro_liq', 'EBIT', 'LPA',
        'divida_curto_prazo', 'divida_longo_prazo', 'caixa', 'divida_total',
        'margem_liq', 'deprec_amortiz', 'EBITDA', 'divida_liq', 'divida_liq_ebitda']]

    df = df.merge(cadastro, on='cod_cvm')

    # Indicadores
    #df['LPA'] = round(df['lucro_liq'] / (df['acoes'] / 1000), 2)
    #df['VPA'] = round(df['patr_liq'] / (df['acoes'] / 1000), 2)

    # Fórmula de Graham: raiz quadrada de (22,5 * LPA * VPA)
    #df['vi_graham'] = round((22.5 * df['LPA'] * df['VPA']) ** .5, 2)

    df = df[['nome', 'cnpj', 'cod_cvm', 'ticker', 'ticker_graham', 'segmento', 'site', 'ano', 'form', 
        'dt_ref', 'versao', 'grupo', 'dt_ini_exerc', 'ativo', 'patr_liq', 'receita_liq', 'lucro_bruto', 'lucro_liq',
        'EBIT', 'deprec_amortiz', 'EBITDA', 'margem_liq', 'divida_curto_prazo', 'divida_longo_prazo',
        'caixa', 'divida_liq', 'divida_liq_ebitda', 'divida_total', 'acoes', 'free_float', 
        'governanca', 'LPA']]

    df = df.sort_values(by=['nome', 'dt_ref'])


    # Salva Dados Financeiros

    df.to_sql(name='DADOS_FINANCEIROS', con=conn, if_exists='replace', index=False)

    # Salva Cadastro somente com as empresas com dados financeiros

    cadastro = cadastro[cadastro.cod_cvm.isin(df.cod_cvm)]

    cadastro.to_sql(name='CADASTRO', con=conn, if_exists='replace', index=False)


def importa_cvm(dt_portal_cvm):


    dt_cvm = obtem_data_atualizacao_cvm(dt_portal_cvm)

    if dt_cvm > dt_portal_cvm:

        base_download_cvm = ['DFP', 'ITR', 'FRE', 'FCA']

        for tipo in base_download_cvm:

            download_arquivos_CVM(dt_ultimo_download, tipo)

        gera_Dados_Financeiros()

        config_update('dt_ultimo_download', data_hoje)

        config_update('dt_portal_cvm', dt_cvm)

        conn.execute('VACUUM')

    return dt_cvm


def obtem_data_atualizacao_cvm(dt_portal_cvm):


    #URL_CVM = f'http://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp'
    URL_CVM = f'http://dados.cvm.gov.br/dataset/cia_aberta-cad'

    try:

        resp = req.get(URL_CVM)

    except Exception as e:

        print(e)

        return dt_portal_cvm


    bs = BeautifulSoup(resp.text, 'html.parser')

    elem = bs.find('span', {'class':'automatic-local-datetime'})

    data = elem.attrs['data-datetime']

    dt_portal_cvm = data[:10] + ' ' + data[11:19]

    return dt_portal_cvm


def processa_DFP_ITR_saldos(tipo, novo_form, anos):


    novo_form.VL_CONTA = novo_form.VL_CONTA.astype(float)
    
    novo_form.loc[novo_form['ESCALA_MOEDA'] == 'UNIDADE', 'VL_CONTA'] = novo_form['VL_CONTA'] / 1000

    novo_form.loc[novo_form['DT_REFER'] != '', 'ano'] = novo_form['DT_REFER'].str[:4]

    novo_form['form'] = tipo

    novo_form = novo_form[(novo_form.ORDEM_EXERC == 'ÚLTIMO') &
                          (novo_form.VL_CONTA != 0)]       

    novo_form.columns = ['cnpj', 'dt_ref', 'versao', 'nome', 'cod_cvm', 'grupo', 'moeda', 'escala_moeda',
                         'ordem_exerc', 'dt_fim_exerc', 'cod_conta', 'desc_conta', 'valor', 'sit_conta_fixa',
                         'dt_ini_exerc', 'ano', 'form']

    contas_selec = ['1', '1.01.01', '1.01.02', '2.01.04', '2.02.01',
                    '2.03', '3.01', '3.03', '3.05', '3.11']

    # idx saldos das contas selecionadas
    idx1 = novo_form.cod_conta.isin(contas_selec)

    # idx deprec
    idx2 = (novo_form.cod_conta.str.startswith('6.01')
           ) & (
           novo_form.desc_conta.str.lower().str.contains('deprec|amortiz', regex=True))

    # idx LPA
    idx3 = (novo_form.cod_conta.str.startswith('3.99'))

    novo_form = novo_form[idx1 | idx2 | idx3]

    # Quando houver formulário consolidado e individual,
    # prevalece consolidado, removendo o individual
    novo_form.loc[novo_form.grupo.str.startswith('DF Consolidado'), 'grupo'] = 'Consolidado'
    novo_form.loc[novo_form.grupo.str.startswith('DF Individual'), 'grupo'] = 'Individual'

    idx_grupos = novo_form.groupby(['cnpj', 'dt_ref']).first().reset_index()[['cnpj', 'dt_ref', 'grupo']]

    novo_form = novo_form[(novo_form.cnpj + novo_form.dt_ref + novo_form.grupo).isin(
                            idx_grupos.cnpj + idx_grupos.dt_ref + idx_grupos.grupo)]

    novo_form = novo_form[['cnpj', 'dt_ref', 'versao', 'cod_cvm', 'grupo', 'moeda', 'escala_moeda',
                           'cod_conta', 'desc_conta', 'valor', 'dt_ini_exerc', 'ano', 'form']]

    # Elimina do banco de dados os anos importados
    # e depois acrescenta com os novos dados

    sql = f'SELECT * FROM {tipo}_SALDOS'

    try:
        df = pd.read_sql(sql, conn)
    except:
        df = pd.DataFrame()

    if len(df) > 0:
        df = df[~df.ano.isin(anos)]

    df = pd.concat([df, novo_form], ignore_index=True)

    df = df.sort_values(['cnpj', 'dt_ref', 'cod_conta'])
                         
    df.to_sql(name=f'{tipo}_SALDOS', con=conn, if_exists='replace', index=False)


def processa_DFP_ITR_transmissoes(tipo, novo_form, anos):


    novo_form.loc[novo_form['DT_REFER'] != '', 'ano'] = novo_form['DT_REFER'].str[:4]

    novo_form['form'] = tipo


    novo_form = novo_form[['CNPJ_CIA', 'DT_REFER', 'VERSAO', 'CD_CVM', 'DT_RECEB', 'LINK_DOC', 'ano', 'form']]

    novo_form.columns = ['cnpj', 'dt_ref', 'versao', 'cod_cvm', 'dt_receb', 'link_doc', 'ano', 'form']


    # Elimina do banco de dados os anos importados
    # e depois acrescenta com os novos dados

    sql = f'SELECT * FROM {tipo}_TRANSMISSOES'

    try:
        df = pd.read_sql(sql, conn)
    except:
        df = pd.DataFrame()

    if len(df) > 0:
        df = df[~df.ano.isin(anos)]

    df = pd.concat([df, novo_form], ignore_index=True)


    df.to_sql(name=f'{tipo}_TRANSMISSOES', con=conn, if_exists='replace', index=False)


def processa_FCA_cadastro(tipo, novo_form, anos):


    novo_form.loc[novo_form['Data_Referencia'] != '', 'ano'] = novo_form['Data_Referencia'].str[:4]

    novo_form = novo_form[['CNPJ_Companhia', 
                           'Codigo_CVM', 'Nome_Empresarial', 'Setor_Atividade', 'Pagina_Web',
                           'Dia_Encerramento_Exercicio_Social', 'Mes_Encerramento_Exercicio_Social', 'ano']]


    novo_form.columns = ['cnpj', 'cod_cvm', 'nome', 'segmento', 'site', 'dia_encerr', 'mes_encerr', 'ano']

    # Elimina do banco de dados os anos importados
    # e depois acrescenta os novos dados

    sql = f'SELECT * FROM {tipo}_CADASTRO'

    try:
        df = pd.read_sql(sql, conn)
    except:
        df = pd.DataFrame()

    if len(df) > 0:
        df = df[~df.ano.isin(anos)]

    df = pd.concat([df, novo_form], ignore_index=True)

    df = df.fillna('')
    
    idx = df.groupby('cnpj')['ano'].max().reset_index()

    df = df[(df.cnpj + df.ano).isin(idx.cnpj + idx.ano)].sort_values('cnpj')

    df = df[['cnpj', 'cod_cvm', 'nome', 'segmento', 'site', 'ano']]

    df.segmento = df.segmento.str.replace('Emp. Adm. Part. - ', '', regex=False)
    df.segmento = df.segmento.str.replace('Emp. Adm. Part.-', '', regex=False)
    df.nome = df.nome.str.upper()

    df.to_sql(name=f'{tipo}_CADASTRO', con=conn, if_exists='replace', index=False)


def processa_FCA_tickers(tipo, novo_form, anos):


    novo_form.loc[novo_form['Data_Referencia'] != '', 'ano'] = novo_form['Data_Referencia'].str[:4]

    novo_form = novo_form[['CNPJ_Companhia',
                           'Codigo_Negociacao', 'Segmento', 'ano']]

    novo_form.columns = ['cnpj', 'ticker', 'governanca', 'ano']

    # Cria df com tickers (arquivo original traz um ticker por linha)
    temp = novo_form[['cnpj', 'ticker']].drop_duplicates().dropna()
    temp['ticker_1'] = temp.ticker.str.upper()
    temp = temp.set_index(['cnpj', 'ticker'])
    temp = temp.unstack().fillna('')
    tickers = temp.loc[:].apply(lambda x: ' '. join(x.values) , axis=1).reset_index()
    tickers.columns = ['cnpj', 'ticker']
    tickers.ticker = tickers.ticker.str.strip()
    tickers.ticker = tickers.ticker.str.replace(' ', ',', regex=False)

    # Elimina do banco de dados os anos importados
    # e depois acrescenta os novos dados

    governanca = novo_form.sort_values(['cnpj', 'ano'])
    governanca = governanca.groupby(['cnpj']).last().reset_index()[['cnpj', 'ano', 'governanca']]


    tickers = tickers.merge(governanca, on='cnpj', how='left')
    tickers = tickers[['cnpj', 'ticker', 'governanca', 'ano']]

    sql = f'SELECT * FROM {tipo}_TICKERS'

    try:
        df = pd.read_sql(sql, conn)
    except:
        df = pd.DataFrame()

    if len(df) > 0:
        df = df[~df.ano.isin(anos)]

    df = pd.concat([df, tickers], ignore_index=True)

    df = df.fillna('')
    
    idx = df.groupby('cnpj')['ano'].max().reset_index()

    df = df[(df.cnpj + df.ano).isin(idx.cnpj + idx.ano)].sort_values('cnpj')

    df.to_sql(name=f'{tipo}_TICKERS', con=conn, if_exists='replace', index=False)


def processa_FRE_capital_social(tipo, novo_form, anos):


    novo_form.loc[novo_form['Data_Referencia'] != '', 'ano'] = novo_form['Data_Referencia'].str[:4]

    novo_form = novo_form[novo_form['Tipo_Capital'] == 'Capital Emitido']
    novo_form = novo_form[novo_form['Quantidade_Total_Acoes'] > 0]

    novo_form['acoes'] = (novo_form.Quantidade_Total_Acoes).astype('int64')

    novo_form = novo_form[['CNPJ_Companhia', 'acoes', 'ano']]

    novo_form.columns = ['cnpj', 'acoes', 'ano']

    # Elimina do banco de dados os anos importados
    # e depois acrescenta com os novos dados

    sql = f'SELECT * FROM {tipo}_CAPITAL'

    try:
        df = pd.read_sql(sql, conn)
    except:
        df = pd.DataFrame()

    if len(df) > 0:
        df = df[~df.ano.isin(anos)]

    df = pd.concat([df, novo_form], ignore_index=True)

    df = df.fillna('')
    
    idx = df.groupby('cnpj')['ano'].max().reset_index()

    df = df[(df.cnpj + df.ano).isin(idx.cnpj + idx.ano)].sort_values('cnpj')

    df = df.drop_duplicates('cnpj', keep='last')

    df.to_sql(name=f'{tipo}_CAPITAL', con=conn, if_exists='replace', index=False)


def processa_FRE_distribuicao_capital(tipo, novo_form, anos):


    novo_form.loc[novo_form['Data_Referencia'] != '', 'ano'] = novo_form['Data_Referencia'].str[:4]

    novo_form = novo_form[novo_form['Quantidade_Total_Acoes_Circulacao'] > 0]
    novo_form = novo_form[novo_form['Percentual_Total_Acoes_Circulacao'] > 0]

    novo_form['acoes'] = (novo_form.Quantidade_Total_Acoes_Circulacao / (novo_form.Percentual_Total_Acoes_Circulacao / 100)).astype('int64')
    novo_form['free_float'] = novo_form.Percentual_Total_Acoes_Circulacao

    novo_form = novo_form[['CNPJ_Companhia', 
       'acoes', 'Quantidade_Total_Acoes_Circulacao', 'Percentual_Total_Acoes_Circulacao', 'ano']]

    novo_form.columns = ['cnpj', 'acoes', 'acoes_circulacao', 'free_float', 'ano']

    # Elimina do banco de dados os anos importados
    # e depois acrescenta com os novos dados

    sql = f'SELECT * FROM {tipo}_CAPITAL'

    try:
        df = pd.read_sql(sql, conn)
    except:
        df = pd.DataFrame()

    if len(df) > 0:
        df = df[~df.ano.isin(anos)]

    df = pd.concat([df, novo_form], ignore_index=True)

    df = df.fillna('')
    
    idx = df.groupby('cnpj')['ano'].max().reset_index()

    df = df[(df.cnpj + df.ano).isin(idx.cnpj + idx.ano)].sort_values('cnpj')

    df.to_sql(name=f'{tipo}_CAPITAL', con=conn, if_exists='replace', index=False)


def read_arquivos_cvm(URL_CVM, tipo, nomes, anos, arquivos, sufixos=['']):


    df = pd.DataFrame()

    ult_transm = pd.DataFrame()


    for nome, ano in zip(nomes, anos):

        with st.spinner(f'Download arquivo {nome}'):

            z = download_url(URL_CVM + nome)

            for arq in arquivos:

                for sufixo in sufixos:

                    filename = f'{tipo.lower()}_cia_aberta_{arq}{sufixo}_{ano}.csv'

                    f = z.open(filename)

                    temp = pd.read_csv(f, encoding='Latin-1', delimiter=';')

                    df = pd.concat([df, temp], ignore_index=True)

        # Últimas transmissões - DFP/ITR
        if tipo in (['DFP', 'ITR']):

            filename = f'{tipo.lower()}_cia_aberta_{ano}.csv'

            f = z.open(filename)

            temp = pd.read_csv(f, encoding='Latin-1', delimiter=';')

            ult_transm = pd.concat([ult_transm, temp], ignore_index=True)


    return df, ult_transm


def read_dados_financeiros():

    df = pd.read_sql('SELECT * FROM DADOS_FINANCEIROS', conn)

    df.receita_liq = df.receita_liq / 1_000
    df.lucro_liq = df.lucro_liq / 1_000
    df.EBITDA = df.EBITDA / 1_000
    df.caixa = df.caixa / 1_000
    df.patr_liq = df.patr_liq / 1_000
    df.divida_total = df.divida_total / 1_000
    df.acoes = df.acoes.fillna(0)
    df.acoes = (df.acoes / 1_000).astype(int)
    df.dt_ref = pd.to_datetime(df.dt_ref).dt.strftime('%d/%m/%Y')
    df = df.fillna(0)
    return df


def ultimos_demonstrativos_transmitidos():


    # Empresas por segmento

    sql = '''
        SELECT t1.*, t2.nome, t2.segmento, t2.ticker
        FROM {}_TRANSMISSOES AS t1, CADASTRO AS t2
        WHERE t1.cod_cvm = t2.cod_cvm
    '''
    dfp = pd.read_sql(sql.format('DFP'), conn)

    itr = pd.read_sql(sql.format('ITR'), conn)

    df = pd.concat([dfp, itr], ignore_index=True)
    
    df = df.sort_values(['dt_receb', 'nome'], ascending=[False, True])

    col1, col2, col3 = st.columns([3, 2, 1])

    with col1:

        nomes = st.multiselect('Empresas:', df.nome.sort_values().drop_duplicates())

    with col2:

        segmentos = st.multiselect('Segmentos:', df.segmento.sort_values().drop_duplicates())

    with col3:

        forms = st.multiselect('Formulários:', df.form.sort_values().drop_duplicates())

    if nomes != []:
        df = df[df.nome.isin(nomes)]

    if segmentos != []:
        df = df[df.segmento.isin(segmentos)]

    if forms != []:
        df = df[df.form.isin(forms)]

    df = df.head(1000)

    df.dt_receb = pd.to_datetime(df.dt_receb).dt.strftime('%d/%m/%Y')
    df.dt_ref = pd.to_datetime(df.dt_ref).dt.strftime('%d/%m/%Y')

    df = df.set_index('dt_receb')

    df = df[['form', 'dt_ref', 'versao', 'nome', 'ticker', 'segmento']]

    df.columns = ['Form', 'Data', 'Versão', 'Nome', 'Ticker', 'Segmento']

    st.table(df)


# Procedimento Principal

st.set_page_config(
    layout='wide',
    initial_sidebar_state='collapsed',
    page_icon='app.jpg',
    page_title='B3')


data_hoje = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

dt_ultimo_download = config_read('dt_ultimo_download')

dt_portal_cvm = config_read('dt_portal_cvm')

# Acessa Portal CVM somente uma vez por dia
if data_hoje[:10] > dt_ultimo_download[:10]:
    dt_portal_cvm = importa_cvm(dt_portal_cvm)

dt_cvm_exib = datetime.strptime(dt_portal_cvm, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M:%S')


with st.sidebar:

    opcao = st.selectbox(
        label = 'B3',
        options = [
            'Dados Financeiros',
            'Empresas por Segmento',
            'Últimos Demonstrativos Transmitidos'])

    if st.button('Download do Banco de Dados'):
        opcao = 'Download do Banco de Dados'

    st.write(f'Portal CVM\n\n Última atualização: {dt_cvm_exib}')

if opcao == 'Dados Financeiros':
    exibe_dados_financeiros()

if opcao == 'Empresas por Segmento':
    empresas_por_segmento()

if opcao == 'Últimos Demonstrativos Transmitidos':
    ultimos_demonstrativos_transmitidos()

if opcao == 'Download do Banco de Dados':
    st.subheader('Backup do Banco de Dados')
    with st.spinner('Gerando base para exportar...'):
        f = ''
        for line in conn.iterdump():
            f = f + line
        st.download_button('Download', f, 'AnaliseFundamentalista.sql', 'text/csv')
