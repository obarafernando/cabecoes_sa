import boto3
from typing import Optional
import argh
import os
import botocore
from .utils import fetch_last_run_datetime, log, s3_binary_to_workbook, add_info_columns, return_table_schema
import cx_Oracle

user = os.environ['ORACLE_USER']
password = os.environ['ORACLE_PASS']
tls = os.environ['ORACLE_TLS']

# SCRIPT_PATH = os.environ['FULLPATH']
lib_dir = r"C:\Users\Fefe\Documents\GitHub\cabecoes_sa\instantclient_19_14" #Should use SCRIPT_PATH on Linux

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket = s3_resource.Bucket("elocase")

default_tables = ['fato_leitura', 'dim_medidor', 'dim_segmento_mercado']

def get_tables_from_s3(s3_tables):
    for table in s3_tables:
        last_dt = fetch_last_run_datetime(bucket.name, table)
        try:
            file = s3_client.get_object(Bucket = bucket.name, Key = f"{table}/{last_dt}.xlsx")
            _table = s3_binary_to_workbook(file,table)
            yield _table, last_dt, table
        except botocore.exceptions.ClientError as e:
            log.info("Unexpected error: %s" % e)

def load_data(tables):
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        connection = cx_Oracle.connect(user=user,password=password, dsn=tls)
        if connection:
            cur = connection.cursor()
            for table, name in tables:
                log.info(f'Droping table {name} if exists.')
                cur.execute(f"""begin
                                    execute immediate 'drop table {name}';
                                    exception when others then if sqlcode <> -942 then raise; end if;
                                end;""")
                log.info(f'Creating table {name}.')
                columns_and_types = return_table_schema(name, 'types')
                columns = return_table_schema(name, None)
                cur.execute(f"""create table {name}({columns_and_types})""")
                log.info(f'Table {name} created.')
                log.info(f'Inserting data into table {name}.')
                max_row = len(table['A'])
                max_col = table.max_column
                for rows in range(2,max_row):
                    for row in table.iter_rows(min_row=rows, max_row=rows, max_col=max_col,values_only=True):
                        #Uncomment line below to see insert
                        #log.info(f"""insert into {name}({insert_param[name]}) values {row}""")
                        cur.execute(f"""insert into {name}({columns}) values {row}""")
            log.info(f"Commiting table {name}.")
            connection.commit()

    except cx_Oracle.DatabaseError as e:
        err, = e.args
        log.info("Oracle-Error-Code:", err.code)
        log.info("Oracle-Error-Message:", err.message)

    finally:
        cur.close()
        connection.close()

@argh.arg("--tables", "-t", nargs='+', type=str)
def main(tables: Optional[str]=None):
    if tables == None:
        tables = default_tables


    log.info("Selected tables %s", tables)
    log.info('Getting tables from s3.')
    tables = list(get_tables_from_s3(tables))

    log.info('Adding info columns.')
    tables_with_info = list(add_info_columns(tables))

    log.info('Loading data into Oracle Database.')
    load_data(tables_with_info)

if __name__ == "__main__":
    parser = argh.ArghParser()
    parser.set_default_command(main)
    parser.dispatch()
