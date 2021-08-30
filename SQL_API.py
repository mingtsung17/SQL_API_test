#!/usr/bin/env python
# coding: utf-8

# ## Flask

# In[2]:


import pymysql
import csv
import codecs
import boto3
import sys
import os
import pandas as pd
import numpy as np
from flask import Flask, request, render_template


# In[4]:


app = Flask(__name__)


@app.route('/')
def main():
     return render_template('login.html')


@app.route('/login', methods=['POST'])
def result():
     if request.method == 'POST':
            passwd = request.values['passwords']
            user = request.values['id']
            user += ' '
            conn = pymysql.connect(host='database0825.cjz4rwfpstwu.ap-northeast-1.rds.amazonaws.com',
                         port=3306, user='root', passwd = passwd, db='test', charset='utf8')
            cur = conn.cursor()
            try:
                t = cur.execute(f'SELECT product_ProductName, SUM(lineItem_UnblendedCost) FROM test_table2 WHERE lineItem_UsageAccountId IN ({user})  GROUP BY product_ProductName ;')
                if t == 0:
                    df = pd.DataFrame(columns =[f'Cannot find lineitem/usageaccountid: {user} at problem 1 in database'], data=([0]))
                else:
                    content = cur.fetchall()
                    df = pd.DataFrame(content)
                    df.columns = ['product_ProductName', 'SUM(lineItem_UnblendedCost)']
            except conn.ProgrammingError:
                return ('Please input id number')
            except conn.OperationalError:
                return ('Please input id number')
            
            sql = 'SELECT product_ProductName, DATE(lineItem_UsageStartDate), SUM(lineItem_UsageAmount) '
            sql += f'FROM test_table2 WHERE lineItem_UsageAccountId IN ({user}) '
            sql += 'GROUP BY product_ProductName, DATE(lineItem_UsageStartDate) '
            sql += 'ORDER BY product_ProductName, DATE(lineItem_UsageStartDate) ;'
            try:
                t = cur.execute(sql)
                if t == 0:
                    df2 = pd.DataFrame(columns =[f'Cannot find lineitem/usageaccountid: {user} at problem 2 in database'], data=([0]))
                else:
                    content = cur.fetchall()
                    df2 = pd.DataFrame(content)
                    df2.columns = ['product_ProductName', 'lineItem_UsageStartDate', 'SUM(lineItem_UsageAmount)']
            except conn.ProgrammingError:
                return ('Please input id number')
            except conn.OperationalError:
                return ('Please input id number')
            
            return render_template('result.html', tables=[df.to_html(classes='data')], tables2=[df2.to_html(classes='data2')], user_id = user)


if __name__ == '__main__':
     app.run()


# In[ ]:




