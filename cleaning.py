import pandas as pd
import pyodbc
import os
import time
files = os.listdir('Source Files')[3:]

conn = pyodbc.connect(
    "driver={ODBC Driver 17 for SQL Server};"
    "server=localhost\\SQLEXPRESS;"
    "database=SSIS_Telecom_DB;"
    "trusted_connection=yes;"
)

cursor = conn.cursor()

reference = pd.read_sql_query("select * from dim_imsi_reference", conn)

print('Reference Data')
counter = 1
for file in files:

    df = pd.read_csv('Source Files\\' + file , sep='|')
    null_df = df[df.isna().any(axis=1)]
    null_df['imsi'] = null_df['imsi'].astype(str, errors='ignore').str.split('.').str[0]
    null_df['cell'] = null_df['cell'].astype(int, errors='ignore')
    null_df['lac'] = null_df['lac'].astype(int, errors='ignore')
    null_df['event_type'] = null_df['event_type'].astype(str, errors='ignore').str.split('.').str[0]
    null_df['imei'] = null_df['imei'].astype(int, errors='ignore')
    null_df['imei'] = null_df['imei'].astype(str, errors='ignore').str.split('.').str[0]
    null_df['event_ts'] = pd.to_datetime(null_df['event_ts'], format='%d/%m/%Y %H:%M', errors='coerce')
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in null_df.itertuples(index=False, name=None)
    ]

    cursor.executemany('''insert into error_destination_output values (?, ?, ?, ?, ?, ?, ?)''', rows)
    print(f'File {counter} read')

    df.dropna(subset=["imsi", "cell", "lac", 'event_type', 'event_ts'], how='any', inplace=True)
    print(f'nulls dropped of file {counter}')
    df['imsi'] = df['imsi'].astype(str).str.split('.').str[0]
    df['cell'] = df['cell'].astype(int)
    df['lac'] = df['lac'].astype(int)
    df['event_type'] = df['event_type'].astype(str).str.split('.').str[0]
    df['imei'] = df['imei'].astype(int, errors='ignore')
    df['imei'] = df['imei'].astype(str, errors='ignore').str.split('.').str[0]
    df['event_ts'] = pd.to_datetime(df['event_ts'], format='%d/%m/%Y %H:%M')

    pre_final = df.merge(reference, how='left', on='imsi')
    pre_final['subscriber_id'] = pre_final['subscriber_id'].astype('Int64', errors='ignore')
    pre_final['subscriber_id'] = pre_final['subscriber_id'].fillna(-99999)
    pre_final['TAC'] = pre_final['imei'].str[:8]
    pre_final['SNR'] = pre_final['imei'].str[8:]
    pre_final['SNR'] = pre_final['SNR'].fillna('-99999')
    pre_final['TAC'] = pre_final['TAC'].fillna('-99999')
    pre_final['imei'] = pre_final['imei'].fillna('-99999')
    pre_final.drop(columns=['id_y'], inplace=True)
    pre_final.rename(columns={'id_x': 'Transaction_id'}, inplace=True)


    final = pd.DataFrame(pre_final[['Transaction_id', 'imsi', 'subscriber_id',
                                    'TAC', 'SNR', 'imei', 'cell', 'lac','event_type', 'event_ts']])

    final.to_csv('Processed Files\\' + file, index=False)
    counter += 1

    time.sleep(2)



processed_files = os.listdir('Processed Files')
for file in processed_files:
    df = pd.read_csv('Processed Files\\' + file)
    rows = list(df.itertuples(index=False, name=None))

    cursor.executemany('''insert into fact_transaction (transaction_id ,imsi,subscriber_id,tac,snr,imei,cell,lac,event_type,event_ts)
                              values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', rows)
    conn.commit()


