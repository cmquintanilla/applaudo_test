# -*- coding: utf-8 -*-
"""
@CreatedDate: Sat Aug 01 08:00:00 2020
@Author: Carlos Quintanilla
"""
#=======================================================================================================
# Importing libs
#=======================================================================================================
import pandas_profiling as pdp
import pandas as pd
import numpy as np
import requests 
from sqlalchemy import create_engine, text
#Connect to a Azure Blob container and read everyfile
from azure.storage.blob import ContainerClient as cc
from io import StringIO, BytesIO
from datetime import datetime
import textblob
import difflib 
import pytz

#=======================================================================================================
# String connections
#=======================================================================================================

#Remember to change the user y password of yours
user_postgres = 'postgres'
pass_postgres = 'applaudo'
user_sql_server = 'xxxxx'
pass_sql_server = 'XXXXXXXX'
postgresql_conn = 'postgresql://'+user_postgres+':'+ pass_postgres +'@localhost:5432/applaudo_test'
sql_server_conn = 'mssql://'+user_sql_server+':'+pass_sql_server+'@orderservers.database.windows.net/orderdb?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes'

#=======================================================================================================
# Database Connection
#=======================================================================================================
print(f'{datetime.now()} - Connecting to postgresql database')
engine = create_engine(postgresql_conn)

#=======================================================================================================
# Truncating staging data
#=======================================================================================================
#product catalog
print(f'{datetime.now()} - Truncating staging data in table: [staging.staging_product_details]')
connection = engine.connect()
connection.execute('''TRUNCATE TABLE staging.staging_product_details''')

#order details
print(f'{datetime.now()} - Truncating staging data in table: [staging.staging_order_details]')
connection.execute('''TRUNCATE TABLE staging.staging_order_details''')
connection.close()

#=======================================================================================================
# API DATA: processing and reading PRODUCTS DETAILS from API (Json)
#=======================================================================================================
#I'm going to use it to update the product names
print(f'{datetime.now()} - processing and reading PRODUCTS DETAILS from API')
r = requests.get('https://etlwebapp.azurewebsites.net/api/products')
json_api = r.json()
json_df = pd.DataFrame(json_api['results'][0]['items'])
json_df.columns = map(str.upper, json_df.columns)

#=======================================================================================================
# Cleaning data from json, removing repeated products and adding UPPER columns for comparison
#=======================================================================================================
#Adding UPPER columns used to clean
print(f'{datetime.now()} - adding UPPER columns used to clean')
json_df['PRODUCT_NAME_UPPER'] = json_df['PRODUCT_NAME'].astype(str).str.upper()
json_df['AISLE_UPPER'] = json_df['AISLE'].astype(str).str.upper()
json_df['DEPARTMENT_UPPER'] = json_df['DEPARTMENT'].astype(str).str.upper()

#Removing the columns used to remove duplicates
print(f'{datetime.now()} - removing the columns used to remove duplicates')
json_df = json_df.drop_duplicates(subset=['PRODUCT_NAME_UPPER', 'AISLE_UPPER', 'DEPARTMENT_UPPER'], keep='first')

#striping the product and aisle
print(f'{datetime.now()} - striping product and aisle')
json_df['AISLE'] = json_df['AISLE'].str.strip()
json_df['PRODUCT_NAME'] = json_df['PRODUCT_NAME'].str.strip()
json_df['DEPARTMENT'] = json_df['DEPARTMENT'].astype(str).str.strip()

#=======================================================================================================
# Inserting or Appending API data into Postgresql
#=======================================================================================================
print(f'{datetime.now()} - Inserting or Appending API data into product_details table in Postgresql')
json_df[['PRODUCT_NAME', 'AISLE', 'DEPARTMENT']].to_sql('staging_product_details', engine, if_exists='append', index=False, schema='staging')
connection = engine.connect()
#connection.execute('''SELECT * FROM upload_product_details()''')
connection.execute('''DELETE FROM product_details pd WHERE EXISTS(
                                SELECT
                                    1--"PRODUCT_NAME", "AISLE", "DEPARTMENT"
                                FROM staging.staging_product_details spd
                                WHERE 
                                    spd."PRODUCT_NAME" = pd."PRODUCT_NAME"
                                    AND spd."AISLE" = pd."AISLE"
                                    AND spd."DEPARTMENT" = pd."DEPARTMENT"
                            );
                            INSERT INTO product_details ("PRODUCT_NAME", "AISLE", "DEPARTMENT")
                            SELECT
                                "PRODUCT_NAME", "AISLE", "DEPARTMENT"
                            FROM staging.staging_product_details;''')
connection.close()

print(f'{datetime.now()} - Reading product_details table')
json_df = pd.read_sql_table("product_details",con=engine)

print(f'{datetime.now()} - #Adding auxiliary columns')
json_df['PRODUCT_UPPER'] = json_df['PRODUCT_NAME'].astype(str).str.upper()
json_df['PRODUCT_NAME_UPPER'] = json_df['PRODUCT_NAME'].astype(str).str.upper()
json_df['AISLE_UPPER'] = json_df['AISLE'].astype(str).str.upper()
json_df['DEPARTMENT_UPPER'] = json_df['DEPARTMENT'].astype(str).str.upper()

#=======================================================================================================
# Reading and cleaning data from azure (CSV Files)
#=======================================================================================================
#Connection string
sas_url = "https://orderstg.blob.core.windows.net/ordersdow?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https&sig=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"
sas__blob_url = "https://orderstg.blob.core.windows.net/ordersdow/[blob]?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https&sig=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"
#sas__blob_url = "https://orderstg.blob.core.windows.net/ordersdow/00.csv?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https&sig=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"

#Creating container connection
blob_container_client = cc.from_container_url(sas_url)
blob_list = blob_container_client.list_blobs()

for blob in blob_list:
    #replace file name to get file url
    sas_blob_url = sas__blob_url.replace("[blob]", blob.name)
    print(f'{datetime.now()} - Procesing file [sas_blob_url]')
    df = pd.read_csv(sas_blob_url, engine='python', sep=",", encoding = "ANSI")

    #Converting
    print(f'{datetime.now()} - Converting the order_details to rows and columns')
    s = df['ORDER_DETAIL'].str.split('~').apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = 'ORDER_DETAIL'
    del df['ORDER_DETAIL']
    df = df.join(s.apply(lambda x: pd.Series(x.split('|'))))
    df = df.rename(columns={0: 'PRODUCT', 1 : 'AISLE', 2: 'ADD_TO_CART_ORDER'})
    df['SOURCE'] = blob.name
    
    print(f'{datetime.now()} - Cleaning data')
    df['ORDER_HOUR_OF_DAY'] = pd.to_numeric(df['ORDER_HOUR_OF_DAY']).abs()
    df['DAYS_SINCE_PRIOR_ORDER'] = pd.to_numeric(df['DAYS_SINCE_PRIOR_ORDER']).abs()
    df['ADD_TO_CART_ORDER'] = pd.to_numeric(df['ADD_TO_CART_ORDER']).abs()
    df['PRODUCT'] = df['PRODUCT'].str.strip()
    df['AISLE'] = df['AISLE'].str.strip()
    df['PRODUCT_UPPER'] = df['PRODUCT'].astype(str).str.upper()
    df['AISLE_UPPER'] = df['AISLE'].astype(str).str.upper()
    
    #Product not found in catalog
    print(f'{datetime.now()} - Finding products missing in catalog')
    index2 = pd.MultiIndex.from_arrays([df[col] for col in ['PRODUCT_UPPER', 'AISLE_UPPER']])
    index1 = pd.MultiIndex.from_arrays([json_df[col] for col in ['PRODUCT_NAME_UPPER', 'AISLE_UPPER']])
    df_prod_not_found =  df.loc[~index2.isin(index1)]
    
    #Finding product in catalog
    print(f'{datetime.now()} - Finding products already stored in catalog')
    
    #******FOUND PRODUCTS******
    df_prod_found =  df.loc[index2.isin(index1)]
    print(f'{datetime.now()} - Merging data to get the Department')
    df_prod_found = df_prod_found.merge(json_df, on=['PRODUCT_UPPER', 'AISLE_UPPER'])
    df_prod_found = df_prod_found.rename(columns={'PRODUCT_y': 'PRODUCT', 'AISLE_y': 'AISLE'})
    print(f'{datetime.now()} - Migrating from CSV Azure Blob to SQL database')
    df_prod_found[['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY', 'DAYS_SINCE_PRIOR_ORDER', 'PRODUCT', 'AISLE', 'ADD_TO_CART_ORDER', 'DEPARTMENT', 'SOURCE']].to_sql('staging_order_details', engine, if_exists='append', index=False, schema='staging')
    
    #******NOT FOUND PRODUCTS******
    print(f'{datetime.now()} - Finding if the NOT FOUND products could be updated')
    df_prod_not_found = df_prod_not_found.assign(**{'PRODUCT': df_prod_not_found['PRODUCT'].apply(lambda x: difflib.get_close_matches(x, json_df['PRODUCT_NAME'])[0])})
    df_prod_not_found['PRODUCT_UPPER'] = df_prod_not_found['PRODUCT'].astype(str).str.upper()
    
    print(f'{datetime.now()} - Merging data to get the Department - NOT FOUND')
    df_prod_not_found = df_prod_not_found.merge(json_df, how="left", on=['PRODUCT_UPPER', 'AISLE_UPPER'])
    df_prod_not_found = df_prod_not_found.rename(columns={'PRODUCT_x': 'PRODUCT', 'AISLE_x': 'AISLE'})
    print(f'{datetime.now()} - Migrating from CSV Azure Blob to SQL database - NOT FOUND')
    df_prod_not_found[['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY', 'DAYS_SINCE_PRIOR_ORDER', 'PRODUCT', 'AISLE', 'ADD_TO_CART_ORDER', 'DEPARTMENT', 'SOURCE']].to_sql('staging_order_details', engine, if_exists='append', index=False, schema='staging')

#=======================================================================================================
# Reading from SQL Server and migrating to postgresql
#=======================================================================================================
#"""DB DATA"""
print(f'{datetime.now()} - Reading from SQL Server to read [order_details] and migrating to postgresql')
engine_mssql = create_engine(sql_server_conn)
table_df = pd.read_sql_table("order_details",con=engine_mssql)

print(f'{datetime.now()} - Converting the order_details to rows and columns')
table_df.columns = map(str.upper, table_df.columns)
s = table_df['ORDER_DETAIL'].str.split('~').apply(pd.Series, 1).stack()
s.index = s.index.droplevel(-1)
s.name = 'ORDER_DETAIL'
del table_df['ORDER_DETAIL']
table_df = table_df.join(s.apply(lambda x: pd.Series(x.split('|'))))
table_df = table_df.rename(columns={0: 'PRODUCT', 1 : 'AISLE', 2: 'ADD_TO_CART_ORDER'})
table_df['SOURCE'] = 'SQL SERVER'

print(f'{datetime.now()} - Cleaning data')
table_df['ORDER_HOUR_OF_DAY'] = pd.to_numeric(table_df['ORDER_HOUR_OF_DAY']).abs()
table_df['DAYS_SINCE_PRIOR_ORDER'] = pd.to_numeric(table_df['DAYS_SINCE_PRIOR_ORDER']).abs()
table_df['ADD_TO_CART_ORDER'] = pd.to_numeric(table_df['ADD_TO_CART_ORDER']).abs()
table_df['PRODUCT'] = table_df['PRODUCT'].str.strip()
table_df['AISLE'] = table_df['AISLE'].str.strip()
table_df['PRODUCT_UPPER'] = table_df['PRODUCT'].astype(str).str.upper()
table_df['AISLE_UPPER'] = table_df['AISLE'].astype(str).str.upper()

print(f'{datetime.now()} - Finding product not in catalog')

index2 = pd.MultiIndex.from_arrays([table_df[col] for col in ['PRODUCT_UPPER', 'AISLE_UPPER']])
index1 = pd.MultiIndex.from_arrays([json_df[col] for col in ['PRODUCT_NAME_UPPER', 'AISLE_UPPER']])
table_prod_not_found =  table_df.loc[~index2.isin(index1)]

#FOUND PRODUCTS
print(f'{datetime.now()} - Finding product in catalog')
table_prod_found =  table_df.loc[index2.isin(index1)]
print(f'{datetime.now()} - Merging data to get the Department - FOUND')
table_prod_found = table_prod_found.merge(json_df, on=['PRODUCT_UPPER', 'AISLE_UPPER'])
table_prod_found = table_prod_found.rename(columns={'PRODUCT_y': 'PRODUCT', 'AISLE_y': 'AISLE'})
print(f'{datetime.now()} - Migrating from SQL Server to SQL database - FOUND')
table_prod_found[['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY', 'DAYS_SINCE_PRIOR_ORDER', 'PRODUCT', 'AISLE', 'ADD_TO_CART_ORDER', 'DEPARTMENT', 'SOURCE']].to_sql('staging_order_details', engine, if_exists='append', index=False, schema='staging')

#NOT FOUND PRODUCTS
print(f'{datetime.now()} - Finding if the not found products could be updated - NOT FOUND')
table_prod_not_found = table_prod_not_found.assign(**{'PRODUCT': table_prod_not_found['PRODUCT'].apply(lambda x: difflib.get_close_matches(x, json_df['PRODUCT_NAME'])[0])})
print(f'{datetime.now()} - Merging data to get the Department - NOT FOUND')
table_prod_not_found['PRODUCT_UPPER'] = table_prod_not_found['PRODUCT'].astype(str).str.upper()
table_prod_not_found = table_prod_not_found.merge(json_df, how="left", on=['PRODUCT_UPPER', 'AISLE_UPPER'])
table_prod_not_found = table_prod_not_found.rename(columns={'PRODUCT_x': 'PRODUCT', 'AISLE_x': 'AISLE'})

#Migration to SQL data base FROM SQL SERVER Database
print(f'{datetime.now()} - Merging data to get the Department - NOT FOUND')
#FOUND PRODUCTS
table_prod_not_found[['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY', 'DAYS_SINCE_PRIOR_ORDER', 'PRODUCT', 'AISLE', 'ADD_TO_CART_ORDER', 'DEPARTMENT', 'SOURCE']].to_sql('staging_order_details', engine, if_exists='append', index=False, schema='staging')

#Creating categories
print(f'{datetime.now()} - Creating Categories')

connection = engine.connect()
#Getting and setting the data for calculation of Categories and inserting on staging table: staging_user_category

print(f'{datetime.now()} - Loading table [staging.staging_user_category]')
#connection.execute('''SELECT * FROM user_product_summary ()''')
connection.execute('''INSERT INTO staging.staging_user_category
                                ("USER_ID", "BUY_MOM_QTY", "BUY_SINGLE_QTY", "BUY_PET_FRIENDLY_QTY", "BUY_COMPLETE_MYSTERY_QTY", "BUY_TOTAL")
                            SELECT
                                "USER_ID",
                                SUM(BUY_MOM_QTY) AS BUY_MOM_QTY, SUM(BUY_SINGLE_QTY) AS BUY_SINGLE_QTY,
                                SUM(BUY_PET_FRIENDLY_QTY) AS BUY_PET_FRIENDLY_QTY, SUM(BUY_COMPLETE_MYSTERY_QTY) AS BUY_COMPLETE_MYSTERY_QTY,
                                SUM(BUY_TOTAL) AS BUY_TOTAL
                            FROM
                                (
                            SELECT
                                    "USER_ID",
                                    CASE
                                    WHEN LOWER("DEPARTMENT") IN ('dairy eggs', 'bakery', 'household', 'babies')
                                        THEN SUM("ADD_TO_CART_ORDER")
                                END AS BUY_MOM_QTY,
                                    CASE
                                    WHEN LOWER("DEPARTMENT") IN ('canned goods', 'meat seafood', 'alcohol', 'snacks', 'beverages')
                                        THEN SUM("ADD_TO_CART_ORDER")
                                END AS BUY_SINGLE_QTY,
                                    CASE
                                    WHEN LOWER("DEPARTMENT") IN ('canned goods', 'pets', 'frozen')
                                        THEN SUM("ADD_TO_CART_ORDER")
                                END AS BUY_PET_FRIENDLY_QTY,
                                    CASE
                                    WHEN LOWER("DEPARTMENT") NOT IN (
                                                            'dairy eggs', 'bakery', 'household', 'babies', 
                                                            'canned goods', 'meat seafood', 'alcohol', 'snacks', 'beverages', 
                                                            'canned goods', 'pets', 'frozen'
                                                            )
                                        THEN SUM("ADD_TO_CART_ORDER")
                                END AS BUY_COMPLETE_MYSTERY_QTY,
                                    SUM("ADD_TO_CART_ORDER") AS BUY_TOTAL
                                FROM staging.staging_order_details
                                GROUP BY
                                "USER_ID",
                                "DEPARTMENT"
                                ) percentages
                            GROUP BY
                            "USER_ID";''')

print(f'{datetime.now()} - Setting the category labels for every consumer')
#connection.execute('''SELECT * FROM user_category_label ()''')
connection.execute('''INSERT INTO user_category
                                ("USER_ID", "USER_CATEGORY")
                            SELECT
                                "USER_ID",
                                CASE
                                WHEN SUM("BUY_MOM_QTY") / SUM("BUY_TOTAL") > 0.5 THEN 'Mom'
                                WHEN SUM("BUY_SINGLE_QTY") / SUM("BUY_TOTAL") > 0.6 THEN 'Single'
                                WHEN SUM("BUY_PET_FRIENDLY_QTY") / SUM("BUY_TOTAL") > 0.3 THEN 'Pet Friendly'
                                ELSE 'A complete mystery'
                            END AS "USER_CATEGORY"
                            FROM
                                staging.staging_user_category
                            GROUP BY
                            "USER_ID"
                            ON CONFLICT
                            ("USER_ID") 
                            DO
                            UPDATE SET "USER_CATEGORY" = excluded."USER_CATEGORY";''')

print(f'{datetime.now()} - Setting the staging order data to order data table')
#connection.execute('''SELECT * FROM upload_order_details ()''')
connection.execute('''INSERT INTO order_details
                                ("ORDER_ID", "USER_ID", "ORDER_NUMBER", "ORDER_DOW", "ORDER_HOUR_OF_DAY", "DAYS_SINCE_PRIOR_ORDER", "PRODUCT", "AISLE", "DEPARTMENT", "ADD_TO_CART_ORDER")
                            SELECT
                                "ORDER_ID", "USER_ID", "ORDER_NUMBER", "ORDER_DOW", "ORDER_HOUR_OF_DAY", "DAYS_SINCE_PRIOR_ORDER", "PRODUCT", "AISLE", "DEPARTMENT", "ADD_TO_CART_ORDER"
                            FROM staging.staging_order_details sod;''')

print(f'{datetime.now()} - Calculating Segmentation')
sql_segmentation_data = pd.read_sql_query('''SELECT * FROM get_segmentation_data ()''', connection)
segmentation_df = pd.DataFrame(sql_segmentation_data)

print(f'{datetime.now()} - Processing every quantile by segmentation group')

def q1(x):
    return x.quantile(0.25)

def q2(x):
    return x.quantile(0.50)

def q3(x):
    return x.quantile(0.75)

functions = {'OUT_ADD_TO_CART_ORDER': [q1, q2, q3]}
quantile_df = segmentation_df.groupby(['OUT_ORDER_DOW', 'OUT_DAYS_SINCE_PRIOR_ORDER', 'OUT_SEGMENTATION']).agg(functions)

print(f'{datetime.now()} - Flattening the dataframe')
quantile_df = quantile_df.reset_index()
quantile_df.columns = quantile_df.columns.map('|'.join).str.strip('|')


print(f'{datetime.now()} - Get user data segmentation')
sql_user_segmentation_data = pd.read_sql_query('''SELECT * FROM get_user_products_data ()''', connection)
user_segmentation_df = pd.DataFrame(sql_user_segmentation_data)
user_segmentation_df = user_segmentation_df.merge(quantile_df, how="left", on=['OUT_ORDER_DOW', 'OUT_DAYS_SINCE_PRIOR_ORDER','OUT_SEGMENTATION'])

def get_label(row):    
    if(row['OUT_TOTAL_PRODUCTS'] > row['OUT_ADD_TO_CART_ORDER|q3'] and row['OUT_SEGMENTATION'] == 'YGAFIM'):
        return "You've Got a Friend in Me"
    if(row['OUT_TOTAL_PRODUCTS'] > row['OUT_ADD_TO_CART_ORDER|q2'] and row['OUT_SEGMENTATION'] == 'BCB'):
        return "Baby come Back"
    if(row['OUT_TOTAL_PRODUCTS'] > row['OUT_ADD_TO_CART_ORDER|q1'] and row['OUT_SEGMENTATION'] == 'SO'):
        return "Special Offers"
    return ""

print(f'{datetime.now()} - Setting the segmentation label')
user_segmentation_df = user_segmentation_df.assign(**{'SEGMENTATION_LABEL': user_segmentation_df.apply(get_label, axis = 1)})

print(f'{datetime.now()} - Truncating data in table: [public.user_segmentation]')
connection.execute('''TRUNCATE TABLE user_segmentation''')

print(f'{datetime.now()} - Inserting the user segmentation table: [public.user_segmentation]')
user_segmentation_df = user_segmentation_df.rename(columns={'OUT_USER_ID': 'USER_ID', 'SEGMENTATION_LABEL': 'USER_SEGMENTATION'})
user_segmentation_df[['USER_ID', 'USER_SEGMENTATION']].to_sql('user_segmentation', engine, if_exists='append', index=False)
connection.close()