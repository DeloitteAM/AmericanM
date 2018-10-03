# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 12:27:49 2018

@author: dmiglani
"""

import cx_Oracle
import pandas as pd
import os
import configparser
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
#import Levenshtein as lv

config = configparser.ConfigParser()
config.read('config_modern_am_oracle.ini')


###################### Set Project Directory ##############################
proj_path = config['PATH']['Project Directory']
os.chdir(proj_path)

####################### Connect to oracle DB ################################
host = config['Oracle_Connect']['Host']
port = config['Oracle_Connect']['Port']
db = config['Oracle_Connect']['Database']
user = config['Oracle_Connect']['User_ID']
pwd = config['Oracle_Connect']['Password']

conn_str = user + "/" + pwd + "@" + host + ":" + port + "/" + db

conn = cx_Oracle.connect(conn_str)



######################## Save Intermediate Files #######################################

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


############## How many approavls for each claim ##############################

multiple_approvals = cc_activity.groupby('CLAIMID').agg({'UPDATEUSERID': 'nunique'})

multiple_approvals_grp = multiple_approvals.groupby('UPDATEUSERID').agg({'UPDATEUSERID': 'count'})



############### For a claim, duplicate payments ##########################

cc_check = cc_check[['CLAIMID','PAYTO','REPORTABLEAMOUNT']]

cc_check['Duplicated_Amt'] = cc_check.duplicated(['CLAIMID','REPORTABLEAMOUNT'], False)

############### For a claim, multiple payments to same receiver ##########################
cc_check['PAYTO'] = cc_check['PAYTO'].str.upper()
cc_check['PAYTO'] = cc_check['PAYTO'].str.replace('[^A-Za-z]+\s', '')
cc_check['Duplicated_Receiver'] = cc_check.duplicated(['CLAIMID','PAYTO'], False)

unique_reciever = pd.unique(cc_check['PAYTO'])

#fuzz.ratio("HDFC", "HDFC Corp")
#fuzz.token_sort_ratio("HDFC Corp", "Corp HDFC")

cc_check['PAYTO_fuzzy'] = cc_check['PAYTO'].apply(lambda x: \
        process.extract(x, unique_reciever, scorer = fuzz.ratio)[1][0])

cc_check['PAYTO_fuzzy_score'] = cc_check['PAYTO'].apply(lambda x: \
        process.extract(x, unique_reciever, scorer = fuzz.ratio)[1][1])


##################### cc_claim ##############################################################
cc_claim = cc_claim[['ID','REPORTEDDATE','LITIGATIONSTATUS', 'CLAIMTIER', 'LOSSCAUSE', 'LOSSDATE', \
                     'ASSIGNEDUSERID', 'INSUREDDENORMID', 'CLAIMNUMBER', 'POLICYID', 'LOSSTYPE', \
                     'LOSSLOCATIONID']]


#################### Same Loss Type, Same Location #########################################
claim_losstype_loc = cc_claim.groupby(['LOSSTYPE', 'LOSSLOCATIONID'], \
                                      as_index = False). agg({'ID' : 'count'})


####################### Fuzzy Match Address ##############################################
cc_address = cc_address[['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 'CITY', 'POSTALCODE']]
cc_address['ADDRESSLINE1']= cc_address['ADDRESSLINE1'].astype(str)
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.upper()
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('[^A-Za-z]+\s', '')
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('.', '')


unique_address = pd.unique(cc_address['ADDRESSLINE1'])
temp = process.extract(unique_address[0], unique_address, scorer = fuzz.ratio)
temp[1]>90


cc_address['ADDRESS_fuzzy'] = cc_address['ADDRESSLINE1'].apply(lambda x: \
        process.extract(x, unique_address, scorer = fuzz.ratio)[1][0])

cc_address['ADDRESS_fuzzy_score'] = cc_address['ADDRESSLINE1'].apply(lambda x: \
        process.extract(x, unique_address, scorer = fuzz.ratio)[1][1])

#########################################################
############# Merge CC_Claim with address

cc_claim = cc_claim.merge(cc_address, 'left', left_on = 'LOSSLOCATIONID', right_on = 'ID')
claim_losstype_loc = cc_claim.groupby(['LOSSTYPE', 'ADDRESSLINE1','POSTALCODE'], \
                                      as_index = False). agg({'ID_x' : 'count'})


