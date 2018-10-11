# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 11:48:48 2018

@author: dmiglani
"""

import configparser

################################### Read INI File ##########################################

config = configparser.ConfigParser()
config.read(ini_path)

proj_path = config['PATH']['Project Directory']

print("Project Directory : " + proj_path)
os.chdir(proj_path) # Set Project Directory 

host = config['Oracle_Connect']['Host']
port = config['Oracle_Connect']['Port']
db = config['Oracle_Connect']['Database']
user = config['Oracle_Connect']['User_ID']
pwd = config['Oracle_Connect']['Password']

################################### Connect to oracle DB ################################
conn_str = user + "/" + pwd + "@" + host + ":" + port + "/" + db
#conn_str = 'ClAIMUSER' + "/" + pwd + "@" + host + ":" + port + "/" + 'CCDatabase'
conn = cx_Oracle.connect(conn_str)
del host, port, db, user, pwd, conn_str