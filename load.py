import boto3
from typing import Optional
import argh
import os
import botocore
from .utils import fetch_last_run_datetime, log, s3_binary_to_workbook, add_info_columns
import cx_Oracle


USER = os.environ['ORACLE_USER']
PASS = os.environ['ORACLE_PASS']
TLS = os.environ['ORACLE_TLS']

# SCRIPT_PATH = os.environ['FULLPATH']
lib_dir = r"C:\Users\Fefe\Documents\GitHub\elocase\cabecoes_sa\instantclient_19_14" #Should use SCRIPT_PATH on Linux

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
BUCKET = s3_resource.Bucket("elocase")

DEFAULT_TABLES = ['fato_leitura', 'dim_medidor', 'dim_segmento_mercado']

MANUAL_CATALOG = {
    "fato_leitura" : """FILE_DT varchar2(100),
                        LOAD_TIME varchar2(100),
                        CD_DOCUMENTO_LEITURA number(38,2),
                        SK_SEGMENTO_MERCADO number(38,2),
                        SK_INSTALACAO number(38,2),
                        SK_MEDIDOR number(38,2),
                        SK_MOTIVO_LEITURA number(38,2),
                        SK_NOTA_LEITURISTA number(38,2),
                        SK_STATUS_LEITURA number(38,2),
                        CD_MEDIDOR number(38,2),
                        CD_MOTIVO_LEITURA number(38,2),
                        CD_ADICIONADO_POR varchar2(100),
                        CD_MODIFICADO_POR varchar2(100),
                        CD_STATUS_LEITURA number(38,2),
                        CD_REGISTRADOR number(38,2),
                        VL_LEITURA_ATUAL number(38,2),
                        VL_LEITURA_ANTERIOR number(38,2),
                        VL_LEITURA_PERIODO_ANTERIOR number(38,2),
                        QT_CONTADOR number(38,2),
                        DT_LEITURA varchar2(100),
                        DT_LEITURA_PREVISTA varchar2(100),
                        DT_MODIFICACAO varchar2(100)"""

    ,"dim_medidor" : """FILE_DT varchar2(100),
                        LOAD_TIME varchar2(100),
                        SK_MEDIDOR number(38,2),
                        CD_MEDIDOR number(38,2),
                        NR_SERIE_MEDIDOR varchar2(100),
                        CD_LOCAL_INSTALACAO number(38,2),
                        DS_FABRICANTE varchar2(100),
                        DS_MEDIDOR varchar2(100)"""

    ,"dim_segmento_mercado" : """FILE_DT varchar2(100),
                        LOAD_TIME varchar2(100),
                        SK_SEGMENTO_MERCADO number(38,2),
                        CD_SEGMENTO_MERCADO number(38,2),
                        DS_SEGMENTO_MERCADO varchar2(100)"""
}

INSERT_PARAM = {
    "fato_leitura" : """FILE_DT,
                        LOAD_TIME,
                        CD_DOCUMENTO_LEITURA,
                        SK_SEGMENTO_MERCADO,
                        SK_INSTALACAO,
                        SK_MEDIDOR,
                        SK_MOTIVO_LEITURA,
                        SK_NOTA_LEITURISTA,
                        SK_STATUS_LEITURA,
                        CD_MEDIDOR,
                        CD_MOTIVO_LEITURA,
                        CD_ADICIONADO_POR,
                        CD_MODIFICADO_POR,
                        CD_STATUS_LEITURA,
                        CD_REGISTRADOR,
                        VL_LEITURA_ATUAL,
                        VL_LEITURA_ANTERIOR,
                        VL_LEITURA_PERIODO_ANTERIOR,
                        QT_CONTADOR,
                        DT_LEITURA,
                        DT_LEITURA_PREVISTA,
                        DT_MODIFICACAO"""

    ,"dim_medidor" : """FILE_DT,
                        LOAD_TIME,
                        SK_MEDIDOR,
                        CD_MEDIDOR,
                        NR_SERIE_MEDIDOR,
                        CD_LOCAL_INSTALACAO,
                        DS_FABRICANTE,
                        DS_MEDIDOR"""

    ,"dim_segmento_mercado" : """FILE_DT,
                        LOAD_TIME,
                        SK_SEGMENTO_MERCADO,
                        CD_SEGMENTO_MERCADO,
                        DS_SEGMENTO_MERCADO"""
}

def get_tables_from_s3(s3_tables):
    for table in s3_tables:
        last_dt = fetch_last_run_datetime(BUCKET.name, table)
        try:
            file = s3_client.get_object(Bucket = BUCKET.name, Key = f"{table}/{last_dt}.xlsx")
            _table = s3_binary_to_workbook(file,table)
            yield _table, last_dt, table
        except botocore.exceptions.ClientError as e:
            log.info("Unexpected error: %s" % e)

def load_data(tables):
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        connection = cx_Oracle.connect(user=USER,password=PASS, dsn=TLS)
        if connection:
            cur = connection.cursor()
            for table, name in tables:
                log.info(f'Droping table {name} if exists.')
                cur.execute(f"""begin
                                    execute immediate 'drop table {name}';
                                    exception when others then if sqlcode <> -942 then raise; end if;
                                end;""")
                log.info(f'Creating table {name}.')
                if name in MANUAL_CATALOG.keys():
                    cur.execute(f"""create table {name}({MANUAL_CATALOG[name]})""")
                log.info(f'Table {name} created.')
                log.info(f'Inserting data into table {name}.')
                if name in INSERT_PARAM.keys():
                    max_row = len(table['A'])
                    max_col = table.max_column
                    for rows in range(2,max_row):
                        for row in table.iter_rows(min_row=rows, max_row=rows, max_col=max_col,values_only=True):
                            cur.execute(f"""insert into {name}({INSERT_PARAM[name]}) values {row}""")
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
        tables = DEFAULT_TABLES

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
