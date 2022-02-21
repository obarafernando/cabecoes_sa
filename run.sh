#!/bin/bash

set -ex

{
    SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
    CLIENTPATH="/instantclient_19-14"
    FULLPATH=$SCRIPTPATH$CLIENTPATH
    export FULLPATH
    export ORACLE_USER=$(aws s3 cp s3://oracle-credentials/oracle_user.txt -)
    export ORACLE_PASS=$(aws s3 cp s3://oracle-credentials/oracle_pass.txt -)
    export ORACLE_TLS=$(aws s3 cp s3://oracle-credentials/tls.txt -)
} &> /dev/null

cd $SCRIPTPATH/..
python3 -m cabecoes_sa.load

