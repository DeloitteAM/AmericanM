# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 12:27:49 2018

@author: dmiglani
"""

import cx_Oracle
import pandas as pd
import os

os.chdir('C:\\Users\\dmiglani\\Desktop\\ModernAmerican')

conn_str = 'CCUSER/Oracle123@10.14.230.19:1521/ClaimCenterDatabase'
con2 = cx_Oracle.connect(conn_str)


###############################################################

query = 'select * from cc_claim'
cc_claim = pd.read_sql(query,con = con2)

query = 'select * from cc_Incident'
cc_Incident = pd.read_sql(query,con = con2)

query = 'select * from cc_transaction'
cc_transaction = pd.read_sql(query,con = con2)

query = 'select * from cc_activity'
cc_activity = pd.read_sql(query,con = con2)

query = 'select * from cc_policy'
cc_policy = pd.read_sql(query,con = con2)


cc_claim.to_csv("05.Intertmediate/cc_claim.csv", encoding='utf-8', index=False)
cc_Incident.to_csv("05.Intertmediate/cc_Incident.csv", encoding='utf-8', index=False)
cc_transaction.to_csv("05.Intertmediate/cc_transaction.csv", encoding='utf-8', index=False)
cc_activity.to_csv("05.Intertmediate/cc_activity.csv", encoding='utf-8', index=False)
cc_policy.to_csv("05.Intertmediate/cc_policy.csv", encoding='utf-8', index=False)


############## How many approavls for each claim ##############################

multiple_approvals = cc_activity.groupby('CLAIMID').agg({'UPDATEUSERID': 'nunique'})

multiple_approvals_grp = multiple_approvals.groupby('UPDATEUSERID').agg({'UPDATEUSERID': 'count'})

#cc_claim = pd.merge(cc_claim, cc_claim, left_index=True, right_index=True)


