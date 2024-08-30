from datetime import timedelta,datetime,time
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import pyarrow

#Trocar após o Desktop\\
df=pd.read_csv("CAMINHO DO RELATORIO.csv",sep=';',decimal=',',encoding='latin-1')

df=df.drop(['Cd.Func Retirada Colmeia','Nome.Func Separação','Cd.Func Separação','N° Separação','Status Pedido','Mapeamento esteira','Nome.Func Retirada Colmeia','EAN','Descrição','Caixa de separação','Prazo Pedido','Caixa de retirada','Dentro/Fora Prazo','Endereço de Separação','Endereço de Colmeia'],axis=1)

colunas_datas=['Data e Horario de Cort','Data integração','Data Geração','Data Inicio da separação','Data finalização de separação','Data Consolidação primeiro item','Data Consolidação ultimo item','Data Retirada','Data de faturamento','Data fechamento Master']
df[colunas_datas] = df[colunas_datas].apply(pd.to_datetime, format='%d/%m/%Y %H:%M:%S')

#Coluna Corte
df['Hora_do_Corte'] = df['Data e Horario de Cort'].dt.strftime('%H:%M:%S')
#Dentro fora do Prazo Sep
df['Status_Separacao']='Dentro do Prazo'
df.loc[df['Data finalização de separação'] >= df['Data Inicio da separação'] + timedelta(hours=2),'Status_Separacao']='Fora do Prazo'

#DENTRO FORA DO PRAZO FATURA SEPARAÇÃO
df['Status_Fatura_Sep']= 'Dentro do Prazo'
df.loc[(df['Data de faturamento'] == '') |(df['Data de faturamento'] >= df['Data finalização de separação'] + timedelta(hours=2)),'Status_Fatura_Sep']= 'Fora do Prazo'

#Status Retirada/Faturamento
df['Status_Fatura_Retirada']= 'Dentro do Prazo'
df.loc[(df['Quantidade Itens'] == 1) | (df['Data Retirada'] == 'NaT') , 'Status_Fatura_Retirada']= 'Unico'
df.loc[(df['Quantidade Itens'] > 1 ) & (df['Data de faturamento'] >= df['Data Retirada'] + timedelta(hours=2)),'Status_Fatura_Retirada']= 'Fora do Prazo'

#Onde perdeu o Prazo
df['Onde_Perdeu_o_Prazo'] = 'Dentro do Prazo'
df.loc[(df['Status_Fatura_Sep'] == 'Dentro do Prazo') & (df['Status_Fatura_Retirada'] == 'Unico'),'Onde_Perdeu_o_Prazo'] = 'Dentro do Prazo'
df.loc[(df['Status_Fatura_Sep'] == 'Fora do Prazo') & (df['Status_Fatura_Retirada'] == 'Fora do Prazo'),'Onde_Perdeu_o_Prazo'] = 'Fora do Prazo'
df.loc[(df['Status_Fatura_Sep'] == 'Fora do Prazo') & (df['Status_Fatura_Retirada'] == 'Dentro do Prazo'),'Onde_Perdeu_o_Prazo'] = 'Separacao'
df.loc[(df['Status_Fatura_Sep'] == 'Dentro do Prazo') & (df['Status_Fatura_Retirada'] == 'Fora do Prazo'),'Onde_Perdeu_o_Prazo'] = 'Retirada'
df.loc[(df['Status_Fatura_Sep'] == 'Fora do Prazo') & (df['Status_Fatura_Retirada'] == 'Unico'),'Onde_Perdeu_o_Prazo'] = 'Separacao'


df['Status_Colmeia_Retirada']= 'Dentro do Prazo'
df.loc[(df['Data Retirada'] >= df['Data Consolidação primeiro item'] + timedelta(hours=2)),'Status_Colmeia_Retirada']= 'Fora do Prazo'
df.loc[(df['Quantidade Itens'] == 1) & (df['Data Consolidação primeiro item'] == 'NaT'), 'Status_Colmeia_Retirada'] = ''

#Fechamento de Master###
df['Status_FaturaMaster'] = 'Fora do Prazo'
df.loc[df['Data de faturamento'] + timedelta(days=1) > df['Data fechamento Master'], 'Status_FaturaMaster']='Dentro do Prazo'

#Fechamento de Master###
df['Status_FaturaMaster2'] = 'Dentro do Prazo'
df.loc[df['Data fechamento Master']  >= df['Data de faturamento'] + timedelta(hours=2), 'Status_FaturaMaster']='Fora do Prazo'

#Turno Resp 3 Turno
df['Turno_responsavel_Corte'] = '3 Turno'
df.loc[(df['Hora_do_Corte'] == '23:01:00') | (df['Hora_do_Corte'] == '23:00:00'),'Turno_responsavel_Corte']= '2 Turno'
df.loc[(df['Hora_do_Corte'] == '15:00:00') | (df['Hora_do_Corte'] == '12:00:00'),'Turno_responsavel_Corte']= '1 Turno'

Turno_inicial1=time(5, 40, 0)
Turno_final1=time(14, 0, 0)
Turno_inicial2=time(14,1,0)
Turno_final2=time(22,0,0)
Turno_inicial3=time(22,1,0)
Turno_final3=time(5,39,0)

#Hora aplicada por turno 
hora_fatura = df['Data de faturamento'].apply(lambda x: x.time() if not pd.isnull(x) else None)


#Comparações do Turno se pode fazer isso 
def determinar_turno(horario):
    if horario is None:
        return "Horario nulo"
    elif Turno_inicial1 <= horario <= Turno_final1:
        return "Turno 1"
    elif Turno_inicial2 <= horario <= Turno_final2:
        return "Turno 2"
    elif (horario <= Turno_inicial3 and horario <= Turno_final3) or (horario >= Turno_inicial3 and horario <= Turno_final3):
        return "Turno 3"
    else:
        return "Nao Faturado"

# Aplicando a função à coluna de horários de faturamento do DataFrame
df['Turno_que_faturou'] = hora_fatura.apply(determinar_turno)


#Hora aplicada para turno para separação
hora_Sep = df['Data finalização de separação'].apply(lambda x: x.time() if not pd.isnull(x) else None)

df['Turno_que_Separou']=hora_Sep.apply(determinar_turno)
df=df.rename(columns={'N° Pedido': 'NPedido', 'Data e Horario de Cort': 'Data_e_Horario_de_Cort','Data integração':'Data_integracao','Data Geração': 'Data_Geracao','Data Inicio da separação':'Data_Inicio_da_separacao','Data finalização de separação': 'Data_finalizacao_de_separacao','Data Consolidação primeiro item': 'Data_Consolidacao_primeiro_item',	'Data Consolidação ultimo item': 'Data_Consolidacao_ultimo_item','Data Retirada':'data_retirada','Tipo de pedido':'tipo_de_pedido',	'Quantidade Itens':'QTD_items',	'Data de faturamento':'Data_fatura','Data fechamento Master':'Data_fechamento_master','Quantidade SKU pedido':'Quantidade_SKU_Pedido'})
# Define o caminho para o arquivo de chave JSON
caminho_arquivo_chave = 'ARQUIVO.json'

# Cria credenciais usando a biblioteca google-auth
credenciais = service_account.Credentials.from_service_account_file(
    caminho_arquivo_chave,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

nome_tabela='SLA_Projeto.SLA_Saida'
to_gbq(df,credentials=credenciais,destination_table=nome_tabela,project_id='sheets-423213',if_exists='append')