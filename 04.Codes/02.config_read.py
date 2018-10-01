# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 12:34:24 2018

@author: dmiglani
"""
%reset -f

import pyodbc
import pandas as pd
import configparser
import os

config = configparser.ConfigParser()
config.read('config_modern_american.ini')


###################### Set Project Directory ##############################
proj_path = config['PATH']['Project Directory']
os.chdir(proj_path)

###################### CATASTROPHY DATES REGION ##################
print(config['CATASTROPHY DATES REGION']['Dates'])

####################### Connect to SQL Server ################################
driver = config['SQL_SERVER']['DRIVER']
server = config['SQL_SERVER']['SERVER']
database = config['SQL_SERVER']['DATABASE']
uid = config['SQL_SERVER']['User_ID']
pwd = config['SQL_SERVER']['Password']

connSqlServer = pyodbc.connect(driver=driver, server=server, database=database, uid=uid, pwd=pwd)

sql_querry = "select ac.subject,ac.*,tr.* from cc_transaction tr inner join cc_claim c on c.id = tr.claimid \
inner join cc_activity ac on ac.ClaimID = c.id where tr.Subtype = 1 and c.ClaimNumber = '000-00-000115'"

dat = pd.read_sql(sql_querry,connSqlServer)
                      
connSqlServer.close()