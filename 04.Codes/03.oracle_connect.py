# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 12:27:49 2018

@author: dmiglani
"""

import cx_Oracle
import pandas as pd

conn_str = 'CCUSER/Oracle123@10.14.230.38:1521/ClaimCenterDatabase'
con2 = cx_Oracle.connect(conn_str)

query = 'select * from cc_claim'
dat = pd.read_sql(query,con = con2)
