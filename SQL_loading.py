#!/usr/bin/env python
# coding: utf-8

# # Import Data to SQL Database

# In[49]:


import pymysql
import csv
import codecs
import boto3
import sys
import os
import pandas as pd
import numpy as np


# In[ ]:


## create query table
# conn = pymysql.connect(host='database0825.cjz4rwfpstwu.ap-northeast-1.rds.amazonaws.com',
#                          port=3306, user='root', passwd='___', db='test', charset='utf8')
# cur = conn.cursor()
# create_table = 'CREATE TABLE test_table2( '
# create_table += 'bill_PayerAccountId VARCHAR(15), '
# create_table += 'lineItem_UnblendedCost DECIMAL(30,15), '
# create_table += 'lineItem_UnblendedRate DECIMAL(30,15), '
# create_table += 'lineItem_UsageAccountId VARCHAR(15), '
# create_table += 'lineItem_UsageAmount DECIMAL(30,15), '
# create_table += 'lineItem_UsageStartDate DATETIME, '
# create_table += 'lineItem_UsageEndDate DATETIME, '
# create_table += 'product_ProductName VARCHAR(50)) ; '
# cur.execute(create_table)
# cur.close()
# conn.close()


# In[53]:


## create index to enhanece query performance
# conn = pymysql.connect(host='database0825.cjz4rwfpstwu.ap-northeast-1.rds.amazonaws.com',
#                          port=3306, user='root', passwd='___', db='test', charset='utf8')
# cur = conn.cursor()
# build_index = 'CREATE INDEX IDX_PNAME_LCOST_ID_AMOUNT_SDATE ON test_table2 (product_ProductName, '
# build_index += 'lineItem_UnblendedCost, lineItem_UsageAccountId, lineItem_UsageStartDate, lineItem_UsageAmount) ;'
# cur.execute(build_index)
# cur.close()
# conn.close()


# In[6]:


# loading data ex1
# def get_conn():
#     conn = pymysql.connect(host='database0825.cjz4rwfpstwu.ap-northeast-1.rds.amazonaws.com',
#                          port=3306, user='root', passwd='___', db='test', charset='utf8')
#     return conn
# def insert(cur, sql, args):
#     cur.execute(sql, args)
# def read_csv_to_mysql(filename):
#     with codecs.open(filename=filename, mode='r', encoding='utf-8') as f:
#         reader = csv.reader(f)
#         head = next(reader)
#         conn = get_conn()
#         cur = conn.cursor()
#         sql = 'insert into test_table values(%s,%s,%s,%s,%s,%s,%s,%s)'
#         for ind, item in enumerate(reader):
#             if item[1] is None or item[1] == '': 
#                 continue
#             if ind > 100:
#                 break
#             args = tuple(item)
#             print(ind, ' ', args)
#             insert(cur, sql=sql, args=args)
#     conn.commit()
#     cur.close()
#     conn.close()
# if __name__ == '__main__':
#     read_csv_to_mysql('output_test.csv')


# In[51]:


# loading data 2
data = pd.read_csv('output_test.csv', header = 0).fillna(0)
data.columns = ('bill_PayerAccountId', 'lineItem_UnblendedCost', 'lineItem_UnblendedRate', 'lineItem_UsageAccountId', 'lineItem_UsageAmount', 'lineItem_UsageStartDate', 'lineItem_UsageEndDate', 'product_ProductName')

# Connect to the database
conn = pymysql.connect(host='database0825.cjz4rwfpstwu.ap-northeast-1.rds.amazonaws.com',
                         port=3306, user='root', passwd='___', db='test', charset='utf8')


# create cursor
cur = conn.cursor()

# creating column list for insertion
cols = "`,`".join([str(i) for i in data.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in data.iterrows():
    print(i)
    sql = "INSERT INTO `test_table2` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cur.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()
cur.close()
conn.close()

