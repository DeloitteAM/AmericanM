# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 11:33:59 2018

@author: dmiglani
"""
######################## Save Oracle Datatable for backup purpose ##############################
query = 'select * from cc_claim'
cc_claim = pd.read_sql(query,con = conn)
query = 'select * from cc_Incident'
cc_Incident = pd.read_sql(query,con = conn)
query = 'select * from cc_transaction'
cc_transaction = pd.read_sql(query,con = conn)
query = 'select * from cc_activity'
cc_activity = pd.read_sql(query,con = conn)
query = 'select * from cc_policy'
cc_policy = pd.read_sql(query,con = conn)
query = 'select * from cctl_incident'
cctl_incident = pd.read_sql(query,con = conn)
query = 'select * from cc_check'
cc_check = pd.read_sql(query,con = conn)
query = 'select * from cc_address'
cc_address = pd.read_sql(query,con = conn)

cc_claim.to_csv("05.Intertmediate/cc_claim.csv", encoding='utf-8', index=False)
cc_Incident.to_csv("05.Intertmediate/cc_Incident.csv", encoding='utf-8', index=False)
cc_transaction.to_csv("05.Intertmediate/cc_transaction.csv", encoding='utf-8', index=False)
cc_activity.to_csv("05.Intertmediate/cc_activity.csv", encoding='utf-8', index=False)
cc_policy.to_csv("05.Intertmediate/cc_policy.csv", encoding='utf-8', index=False)
cctl_incident.to_csv("05.Intertmediate/cctl_incident.csv", encoding='utf-8', index=False)
cc_check.to_csv("05.Intertmediate/cc_check.csv", encoding='utf-8', index=False)
cc_address.to_csv("05.Intertmediate/cc_address.csv", encoding='utf-8', index=False)

del query
