import pandas as pd
import numpy as np

# Lê os dados do ENEM 2019
#enem = pd.read_csv('data/DADOS/MICRODADOS_ENEM_2019.csv', sep=';', decimal=',', encoding='cp1252')
#print(enem.head())

#enem.shape

# Mantém apenas os alunos residentes em MG
#enemminas = enem.loc[enem.SG_UF_RESIDENCIA == 'MG']
#enemminas.shape

# Escreve os dados para economizar memória
#enemminas.to_csv('data/enemminas.csv', index=False)

enemminas = pd.read_csv('data/enemminas.csv')

