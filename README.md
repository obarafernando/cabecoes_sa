# Cabecoes SA

This project contains a tables-load script and a pbix file.

## Context

- To help Cabecoes SA with their business analysis we have to load some date (Fact and Dimensions) into a proper database (using Oracle Autonomous DB).
- With Oracle Database Actions and the loaded data we created some tables to help us solve some of the company questions.
- Then we configured a connection between Power BI and the Oracle Autonomous DB to make a dashboard highlighting some indicators.

## load script

The load script consists of 3 steps:
- Retrieving data (tables) from storage (s3).
  - Get the xlsx files and convert into workbook object.
  - Standardize cell values.
- Adding info columns.
  - Just added some useful columns (load timestamp and file date).
- Loading data into database.
  - Connects to Oracle Client.
  - Retrieve metadata from schema.json.
  - Drop/Create and insert values from s3 csvs into tables.
  - Commit

Won't run without changing some authentication files (.aws/credentials). The credentials to connect into Oracle Autonomous Database are also retrieved from s3.

## pbix
Was made to answer the following questions:

![alt text](https://github.com/obarafernando/cabecoes_sa/blob/main/questions.png)

