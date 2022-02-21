import boto3
import logging
import io
from openpyxl import load_workbook
import datetime
from typing import List


FORMAT = '%(asctime)-15s - %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(logging.INFO)

def list_s3_keys(bucket_name, prefix):
    S3 = boto3.client('s3')
    log.info(f'Listing files in s3://{bucket_name}/{prefix}')
    bucket_paginator = S3.get_paginator('list_objects_v2').paginate(Bucket=bucket_name, Prefix=prefix)
    for page in bucket_paginator:
        for f in page.get('Contents', []):
            yield f['Key']

def fetch_last_run_datetime(bucket_name, prefix, checksum_separator=None):
    valid_datetimes = []
    for key in list_s3_keys(bucket_name, prefix):
        try:
            datetime_raw = key.split('/')[-1].split('.')[0]
            if checksum_separator:
                datetime_raw = datetime_raw.split(checksum_separator)[0]
            valid_datetimes.append(datetime_raw)
        except (ValueError, IndexError) as e:
            log.warning(f'Got error from parsing past runs: {e}')
    if not valid_datetimes:
        return None
    return max(valid_datetimes)

def s3_binary_to_workbook(file,table):
    data = io.BytesIO(file['Body'].read())
    sheet = load_workbook(data).active
    sheet = convert_none_null(convert_dt_columns_values(sheet,table))
    return sheet

def add_info_columns(tables: List[tuple]):
    for table, last_dt, name in tables:
        try:
            load_time = datetime.datetime.now()
            load_time = load_time.strftime("%Y-%m-%d %H:%M:%S")
            last_row = len(table['A'])
            table.insert_cols(idx=1, amount=2)
            table['A1'] = 'FILE_DT'
            table['B1'] = 'LOAD_TIME'
            for x in range(2, last_row):
                table[f'A{x}'] = last_dt
                table[f'B{x}'] = load_time
            yield table, name
        except:
            pass

def convert_dt_columns_values(sheet,name):
    max_row = len(sheet['A'])
    max_col = sheet.max_column
    if name == 'fato_leitura':
        for rows in range(2,max_row):
            for columns in range(max_col-2 ,max_col+1):
                cell = sheet.cell(row=rows, column=columns)
                cell.value = str(cell.value)
    return sheet

def convert_none_null(sheet):
    max_row = len(sheet['A'])
    max_col = sheet.max_column
    for rows in range(2,max_row):
        for columns in range(1,max_col+1):
            cell = sheet.cell(row=rows, column=columns)
            if cell.value == 'NULL' or cell.value == None:
                cell.value = ''
            if cell.value == '0.000000000000000000':
                cell.value = 0
    return sheet