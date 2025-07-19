# %%
import pandas as pd
import numpy as np
import datetime
from pymongo import MongoClient

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

# %%
df = pd.read_excel('http://www.leb.esalq.usp.br/leb/automatica/diario2020.xls')

# %%
columns = df.iloc[6].tolist()

# %%
df_data = df.iloc[9:]
df_data.columns = columns
df_data.head(10)

# %%
df_data['TIMESTAMP'] = pd.to_datetime(df_data['TIMESTAMP'])
df_data.head()

# %%
df_data.info()

# %%
df_data[df_data['Chuva_mm'].notnull()].info()

# %%
firstYear = 1997
currYear = datetime.date.today().year

url = 'http://www.leb.esalq.usp.br/leb/automatica/diario{year}.xls'

for year in range(firstYear, currYear + 1):
    data_raw = pd.read_excel(url.format(year=year))
    index_columns = 0
    
    # Verifying correct columns and values for each year
    if year < 2003:
        index_columns = 11
    elif year < 2017:
        index_columns = 14
    elif year < 2024:
        index_columns = 6
    else:
        index_columns = 2
    
    columns_data = data_raw.iloc[index_columns].tolist()
    data_nan_columns = data_raw.iloc[index_columns + 3:].copy()
    
    # Transform different target names to a unique name and remove NaN column names
    columns_data = ['Chuva' if column == 'Chuva_mm' or column == 'Precip' else column for column in columns_data]
    columns_notna = [column for column in columns_data if column is not np.nan]
    
    data_nan_columns.columns = columns_data
    
    # Add valid columns to new dataframe
    data_nan_columns = data_nan_columns[data_nan_columns['Chuva'].notnull()]
    
    data_valid_columns = data_nan_columns[columns_notna].copy()
    
    # Verifying null values in columns after cleaning target values
    print(f'{year}: \n\nColunas padrão: {columns_notna}')
    print(f'Colunas não nulas: {columns_notna}')
    
    if data_valid_columns.isnull().values.any():
        print('Resultado: Valor nulo detectado após limpeza em ', end='')
        
        columns_null = []
        for column in columns_notna:
            if data_valid_columns[column].isnull().values.any():
                columns_null.append(column)
                
        print(columns_null)
    else:
        print('Resultado: OK')
        
    print('\n---------------------------------------------------------------\n')
    
# %%
firstYear = 1997
currYear = datetime.date.today().year

url = 'http://www.leb.esalq.usp.br/leb/automatica/diario{year}.xls'

for year in range(firstYear, currYear + 1):
    data_raw = pd.read_excel(url.format(year=year))
    index_columns = 0
    
    # Verifying correct columns and values for each year
    if year < 2003:
        index_columns = 11
    elif year < 2017:
        index_columns = 14
    elif year < 2024:
        index_columns = 6
    else:
        index_columns = 2
    
    columns_data = data_raw.iloc[index_columns].tolist()
    data_nan_columns = data_raw.iloc[index_columns + 3:].copy()
    
    # Transform different target names to a unique name and remove NaN column names
    columns_data = ['Chuva' if column == 'Chuva_mm' or column == 'Precip' else column for column in columns_data]
    columns_notna = [column for column in columns_data if column is not np.nan]
    
    data_nan_columns.columns = columns_data
    
    # Add valid columns to new dataframe
    data_nan_columns = data_nan_columns[data_nan_columns['Chuva'].notnull()]
    
    data_valid_columns = data_nan_columns[columns_notna].copy()
    
    # Verifying null values in columns after cleaning target values
    print(f'{year}: \n\nColunas padrão: {columns_data}')
    print(f'Colunas não nulas: {columns_notna}')
    
    if data_valid_columns.isnull().values.any():
        print('Resultado: Valor nulo detectado após limpeza em ', end='')
        
        columns_null = []
        for column in columns_notna:
            if data_valid_columns[column].isnull().values.any():
                data_valid_columns = data_valid_columns[data_valid_columns[column].notnull()]
                columns_null.append(column)
                
        print(columns_null)
        print(f'Registros após remoções: {data_valid_columns.shape[0]} linhas')
    else:
        print('Resultado: OK')
        
    print('\n---------------------------------------------------------------\n')

# %%
firstYear = 1997
currYear = datetime.date.today().year

url = 'http://www.leb.esalq.usp.br/leb/automatica/diario{year}.xls'

client = MongoClient('mongodb://localhost:27017')
collection = client['esalq_met']['raw_data']

for year in range(firstYear, currYear + 1):
    data_raw = pd.read_excel(url.format(year=year))
    index_columns = 0
    
    # Verifying correct columns and values for each year
    if year < 2003:
        index_columns = 11
    elif year < 2017:
        index_columns = 14
    elif year < 2024:
        index_columns = 6
    else:
        index_columns = 2
    
    columns_data = data_raw.iloc[index_columns].tolist()
    data_nan_columns = data_raw.iloc[index_columns + 3:].copy()
    
    # Transform different target names to a unique name and remove NaN column names
    columns_data = ['Chuva' if column == 'Chuva_mm' or column == 'Precip' else column for column in columns_data]
    columns_data = [columns_data[i] + str(i) if columns_data[i] is not np.nan and columns_data.count(columns_data[i]) > 1 else columns_data[i] for i in range(0, len(columns_data))]
    columns_notna = [column for column in columns_data if column is not np.nan]
    
    data_nan_columns.columns = columns_data
    
    # Add valid columns to new dataframe
    data_nan_columns = data_nan_columns[data_nan_columns['Chuva'].notnull()]
    
    data_valid_columns = data_nan_columns[columns_notna].copy()
    data_valid_columns = data_valid_columns.drop_duplicates()
    
    columns_form = [str(column).replace('.', '_') for column in columns_notna]
    data_valid_columns.columns = columns_form
    data_valid_columns['yearRef'] = year
    
    # Adding values to MongoDB
    print(f'{year}: Adding values to database')
    
    data_dict = data_valid_columns.to_dict("records")
    collection.insert_many(data_dict)
        
    print('\n---------------------------------------------------------------\n')
    
# %%
df_mongo = pd.DataFrame(list(collection.find({})))
df_mongo.head()

# %%
df_mongo[df_mongo['yearRef'] == 1997].dropna(axis=1, how='all').columns

# %%
df_mongo[df_mongo['yearRef'] == 2025].dropna(axis=1, how='all').columns

# %%
# The three column go until 2016.
# From 2017, the column TIMESTAMP is used
time_2400 = (df_mongo['Horário'] == 2400) | (df_mongo['Horario'] == 2400) | (df_mongo['Horas'] == 2400)
df_mongo[time_2400]['yearRef'].unique()

# %%
# Remove times with NaN values
time_nan = (
    (df_mongo['yearRef'] < 2017) & (
        (df_mongo['Horario'].isna())
        & (df_mongo['Horário'].isna())
        & (df_mongo['Horas'].isna())
    )
)
df_mongo = df_mongo[~time_nan]

# %%
ts_ref = datetime.datetime(1997, 1, 2)
ts_ref + datetime.timedelta(days=0, minutes=2400)

# %%
df_mongo[df_mongo['yearRef'] == 1997]['Horario'].unique()

# %%
df_mongo['Horario'] = (
    df_mongo['Horario']
        .apply(lambda x: x - (int(x / 100) * 40) if not np.isnan(x) else x)
)
df_mongo.head(10)

# %%
# Create TIMESTAMP for old data
# TODO: Organize all old data in one column to be
# processed by the same function.
# TODO: Instead of using MongoDB, create concats
# to create the full database.
def create_timestamp(row):
    dt = datetime.datetime
    td = datetime.timedelta
    
    ret = row['TIMESTAMP']

    if not (np.isnan(row['Dia Juliano']) or np.isnan(row['Horario'])):
        ret = dt(row['yearRef'], 1, 1) + \
            td(days=row['Dia Juliano'] - 1, minutes=row['Horario'])
        
    return ret

df_mongo['TIMESTAMP'] = (
    df_mongo
        .apply(
            create_timestamp,
            axis=1
        )
)

df_mongo['TIMESTAMP']

# %%
df_mongo['yearRef']
