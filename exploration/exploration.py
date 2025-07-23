# %%
import pandas as pd
import numpy as np
import datetime
from pymongo import MongoClient

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

# %%
df = pd.read_excel('http://www.leb.esalq.usp.br/leb/automatica/diario2016.xls')

# %%
df.iloc[6095:6103]

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

client = MongoClient('mongodb://localhost:27017')
collection = client['esalq_met']['raw_data']

reprocessing_2016 = False

df_met = pd.DataFrame()

for year in range(firstYear, currYear + 1):
    data_raw = pd.read_excel(url.format(year=year))
    index_columns = 2
    
    while True:
        # Verifying correct columns and values for each year
        if year < 2003:
            index_columns = 11
        elif year < 2017:
            index_columns = 14
        elif year < 2024:
            index_columns = 6
    
        if year == 2016 and not reprocessing_2016:
            reprocessing_2016 = True
        elif reprocessing_2016:
            index_columns = 6099
            reprocessing_2016 = False

        columns_data = data_raw.iloc[index_columns].tolist()
        
        if reprocessing_2016:
            data_nan_columns = data_raw.iloc[index_columns + 3:6095].copy()
        else:
            data_nan_columns = data_raw.iloc[index_columns + 3:].copy()
            
        
        # Transform different target names to a unique name and remove NaN column names
        columns_data = ['Chuva_mm' if column == 'Chuva' or column == 'Precip' else column for column in columns_data]
        columns_data = [columns_data[i] + str(i) if columns_data[i] is not np.nan and columns_data.count(columns_data[i]) > 1 else columns_data[i] for i in range(0, len(columns_data))]
        columns_notna = [column for column in columns_data if column is not np.nan]
        
        data_nan_columns.columns = columns_data
        
        # Add valid columns to new dataframe
        data_nan_columns = data_nan_columns[data_nan_columns['Chuva_mm'].notnull()]
        
        data_valid_columns = data_nan_columns[columns_notna].copy()
        data_valid_columns = data_valid_columns.drop_duplicates()
        
        columns_form = [str(column).replace('.', '_') for column in columns_notna]
        data_valid_columns.columns = columns_form
        data_valid_columns['yearRef'] = year
        
        # Adding values to MongoDB
        print(f'{year}: Adding values to database')
        
        #data_dict = data_valid_columns.to_dict("records")
        #collection.insert_many(data_dict)
        
        df_met = pd.concat([df_met, data_valid_columns], ignore_index=True)

        print('\n---------------------------------------------------------------\n')

        if not reprocessing_2016:
            break

# %%
# Defining all values from other columns
# to "Dia" and "Horas"
def centralize_columns_info(row):
    timestamp_nan = pd.isna(row['TIMESTAMP'])

    # For day
    dia_juliano_nan = pd.isna(row['Dia Juliano'])
    
    if timestamp_nan and not dia_juliano_nan:
        row['Dia'] = row['Dia Juliano']
    
    # For time
    timestamp_nan = pd.isna(row['TIMESTAMP'])
    horas_nan = pd.isna(row['Horas'])
    horario_nan = pd.isna(row['Horario'])
    
    if timestamp_nan and horas_nan:
        row['Horas'] = row['Horário'] if horario_nan else row['Horario']
        
    return [row['Dia'], row['Horas']]
    
df_met[['Dia', 'Horas']] = pd.DataFrame(
    df_met.apply(centralize_columns_info, axis=1).to_list(),
    columns=["Dia", "Horas"]
)

# %%
# Remove times with NaN values
time_nan = (df_met['TIMESTAMP'].isna()) & (df_met['Horas'].isna())
df_met = df_met[~time_nan]

# %%
# Transform "Horas" to minutes
df_met['Horas'] = (
    df_met['Horas']
        .apply(lambda x: x - (int(x / 100) * 40) if not pd.isna(x) else x)
)

# %%
# Create TIMESTAMP for old data
def create_timestamp(row):
    dt = datetime.datetime
    td = datetime.timedelta
    
    ret = row['TIMESTAMP']

    if not (np.isnan(row['Dia']) or np.isnan(row['Horas'])):
        ret = dt(row['yearRef'], 1, 1) + \
            td(days=row['Dia'] - 1, minutes=row['Horas'])
        
    return ret

df_met['TIMESTAMP'] = (
    df_met
        .apply(
            create_timestamp,
            axis=1
        )
)

# %%
# Listing the current used columns in new entries
columns = [
    'TIMESTAMP',
    'BattV_Avg',
    'Tar_AVG',
    'UR_inst',
    'Vvento_ms_AVG',
    'Dvento_G',
    'Qg_AVG',
    'PAR_AVG',
    'Rn_Avg',
    'Chuva_mm',
    'Patm_kPa_AVG',
    'rQg_AVG',
    'Qatm_AVG',
    'Qsup_AVG',
    'Boc_AVG',
    'Bol_AVG',
    'Albedo_Avg',
    'QatmC_AVG',
    'QsupC_AVG',
    'Vvento_ms_S_WVT',
    'Dvento_D1_WVT',
    'Dvento_SD1_WVT',
    'PainelT'
]
df_met[columns]
