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
import numpy as np
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

#conn_str = 'ClAIMUSER' + "/" + pwd + "@" + host + ":" + port + "/" + 'CCDatabase'

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

################ For a claim, duplicate payments ##########################
#
#cc_check = cc_check[['CLAIMID','PAYTO','REPORTABLEAMOUNT']]
#
#cc_check['Duplicated_Amt'] = cc_check.duplicated(['CLAIMID','REPORTABLEAMOUNT'], False)
#
################ For a claim, multiple payments to same receiver ##########################
#cc_check['PAYTO'] = cc_check['PAYTO'].str.upper()
#cc_check['PAYTO'] = cc_check['PAYTO'].str.replace('[^A-Za-z]+\s', '')
#cc_check['Duplicated_Receiver'] = cc_check.duplicated(['CLAIMID','PAYTO'], False)
#
#unique_reciever = pd.unique(cc_check['PAYTO'])
#
##fuzz.ratio("HDFC Crp", "HDFC Corp")
##fuzz.token_sort_ratio("HDFC Corp", "Corp HDFC")
##fuzz.token_sort_ratio("HDFC Corp", "Crp HDFC")
#
#cc_check['PAYTO_fuzzy'] = cc_check['PAYTO'].apply(lambda x: \
#        process.extract(x, unique_reciever, scorer = fuzz.ratio)[1][0])
#
#cc_check['PAYTO_fuzzy_score'] = cc_check['PAYTO'].apply(lambda x: \
#        process.extract(x, unique_reciever, scorer = fuzz.ratio)[1][1])
#
#
###################### cc_claim ##############################################################
#cc_claim = cc_claim[['ID','REPORTEDDATE','LITIGATIONSTATUS', 'CLAIMTIER', 'LOSSCAUSE', 'LOSSDATE', \
#                     'ASSIGNEDUSERID', 'INSUREDDENORMID', 'CLAIMNUMBER', 'POLICYID', 'LOSSTYPE', \
#                     'LOSSLOCATIONID']]
#
#
##################### Same Loss Type, Same Location #########################################
#claim_losstype_loc = cc_claim.groupby(['LOSSTYPE', 'LOSSLOCATIONID'], \
#                                      as_index = False). agg({'ID' : 'count'})
#
#
######################## Fuzzy Match Address ##############################################
#cc_address = cc_address[['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 'CITY', 'POSTALCODE']]
#cc_address['ADDRESSLINE1']= cc_address['ADDRESSLINE1'].astype(str)
#cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.upper()
#cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('[^A-Za-z]+\s', '')
#cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('.', '')
#
#
#unique_address = pd.unique(cc_address['ADDRESSLINE1'])
#temp = process.extract(unique_address[0], unique_address, scorer = fuzz.ratio)
#
#
#
#cc_address['ADDRESS_fuzzy'] = cc_address['ADDRESSLINE1'].apply(lambda x: \
#        process.extract(x, unique_address, scorer = fuzz.ratio)[1][0])
#
#cc_address['ADDRESS_fuzzy_score'] = cc_address['ADDRESSLINE1'].apply(lambda x: \
#        process.extract(x, unique_address, scorer = fuzz.ratio)[1][1])
#
##########################################################
############## Merge CC_Claim with address
#
#cc_claim = cc_claim.merge(cc_address, 'left', left_on = 'LOSSLOCATIONID', right_on = 'ID')
#claim_losstype_loc = cc_claim.groupby(['LOSSTYPE', 'ADDRESSLINE1','POSTALCODE'], \
#                                      as_index = False). agg({'ID_x' : 'count'})




######################################################################################################
######################################################################################################
############## Fraud Scenerio :Dupicate claims and payments for same cause ############################


cc_check_grp = cc_check.groupby(['CLAIMID', 'PAYTO'], as_index = False).agg({'REPORTABLEAMOUNT' : 'sum'})

cc_claim = cc_claim[['ID','REPORTEDDATE','LITIGATIONSTATUS', 'CLAIMTIER', 'LOSSCAUSE', 'LOSSDATE', \
                     'ASSIGNEDUSERID', 'INSUREDDENORMID', 'CLAIMNUMBER', 'POLICYID', 'LOSSTYPE', \
                     'LOSSLOCATIONID']]
cc_claim.rename(columns={'ID':'CLAIMID'}, inplace=True)

cc_address = cc_address[['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 'CITY', 'POSTALCODE']]
cc_address['ADDRESSLINE1']= cc_address['ADDRESSLINE1'].astype(str)
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.upper()
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('[^A-Za-z]+\s', '')
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('.', '')

cc_claim = cc_claim.merge(cc_address, 'left', left_on = 'LOSSLOCATIONID', right_on = 'ID')

cc_policy.columns.values
cc_policy = cc_policy[['ID', 'REPORTINGDATE', 'EFFECTIVEDATE', 'EXPIRATIONDATE', 'POLICYNUMBER']]
cc_claim = cc_claim.merge(cc_policy, 'left', left_on = 'POLICYID', right_on = 'ID')

cc_claim_payment = cc_claim.merge(cc_check_grp, 'inner', on = 'CLAIMID')
cc_claim_payment.columns.values
cc_claim_payment['FraudScene1'] = cc_claim_payment.duplicated(['REPORTABLEAMOUNT', 'POLICYNUMBER'\
                ,'LOSSCAUSE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'POSTALCODE', 'PAYTO'], False)


cc_claim_f1 = cc_claim_payment.loc[cc_claim_payment['FraudScene1'] == True]



######################### Fraud Scenerio 2 : Slight change in address ##################################
cc_claim_payment['FraudScene2'] = cc_claim_payment.duplicated(['REPORTABLEAMOUNT', 'POLICYNUMBER'\
                ,'LOSSCAUSE', 'POSTALCODE', 'PAYTO'], False)

cc_claim_f2 = cc_claim_payment.loc[cc_claim_payment['FraudScene2'] == True]

cc_claim_f2 = cc_claim_f2.loc[cc_claim_f2['FraudScene1'] == False]

cc_claim_f2_grp = cc_claim_f2.groupby(['REPORTABLEAMOUNT', 'POLICYNUMBER'\
                ,'LOSSCAUSE', 'POSTALCODE', 'PAYTO'], as_index = False).agg({'ADDRESSLINE1' : 'unique'})

cc_claim_f2_grp.rename(columns={'ADDRESSLINE1':'Grp_ADDRESSLINE'}, inplace=True)

cc_claim_f2 = cc_claim_f2.merge(cc_claim_f2_grp)

cc_claim_f2['FuzzyAddress'] = ''
cc_claim_f2['FuzzyAddressScore'] = np.nan

for x in range(len(cc_claim_f2.index)) :
    cc_claim_f2.loc[x, 'FuzzyAddress'] = process.extract(cc_claim_f2['ADDRESSLINE1'][x], 
               cc_claim_f2['Grp_ADDRESSLINE'][x], scorer = fuzz.token_sort_ratio)[1][0]
    cc_claim_f2.loc[x, 'FuzzyAddressScore'] = process.extract(cc_claim_f2['ADDRESSLINE1'][x], 
               cc_claim_f2['Grp_ADDRESSLINE'][x], scorer = fuzz.token_sort_ratio)[1][1]
    
    
cc_claim_f2 = cc_claim_f2.loc[cc_claim_f2['FuzzyAddressScore'] > 75]


######################## manual Cheque Fraud #######################################################

cc_check.columns.values

cc_check = cc_check[['CLAIMID', 'PAYTO', 'REPORTABLEAMOUNT', 'PAYMENTMETHOD']]
cc_check = cc_check.merge(cc_claim[['CLAIMID', 'ASSIGNEDUSERID']], 'left')

cc_check_manual = cc_check.groupby(['ASSIGNEDUSERID','PAYMENTMETHOD'], as_index= False).agg(\
        {'REPORTABLEAMOUNT' : 'sum'})

#cc_check_manual = cc_check_manual.loc[cc_check_manual['PAYMENTMETHOD'] == 1]


cc_check_grp = cc_check.groupby(['ASSIGNEDUSERID'], as_index= False).agg({'REPORTABLEAMOUNT' : 'sum'})
cc_check_grp.rename(columns={'REPORTABLEAMOUNT':'Ttl_REPORTABLEAMOUNT'}, inplace=True)

cc_check_manual = cc_check_manual.merge(cc_check_grp, 'left')
cc_check_manual = cc_check_manual.loc[cc_check_manual['PAYMENTMETHOD'] == 1]
cc_check_manual['PercentageManual'] = 100 * cc_check_manual['REPORTABLEAMOUNT'] / cc_check_manual['Ttl_REPORTABLEAMOUNT']






