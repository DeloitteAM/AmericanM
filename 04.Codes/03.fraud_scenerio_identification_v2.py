# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 11:13:51 2018
1. Connect with OracleDB
2. 


@author: dmiglani
"""

import cx_Oracle
import pandas as pd
import os
import configparser
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import numpy as np


################################## Thresold ##############################################
Thresold_Address_Fuzzy_Match = 75
Thresold_Perc_Manual_Payment = 10
num_std_dev = 1

################################### Read INI File ##########################################

config = configparser.ConfigParser()
config.read('config_modern_am_oracle.ini')

proj_path = config['PATH']['Project Directory']

host = config['Oracle_Connect']['Host']
port = config['Oracle_Connect']['Port']
db = config['Oracle_Connect']['Database']
user = config['Oracle_Connect']['User_ID']
pwd = config['Oracle_Connect']['Password']

del config

################################### Set Project Directory ##############################
os.chdir(proj_path)

intermediate_dir_path = proj_path + "/05.Intertmediate/"
del proj_path

################################## Connect to oracle DB ################################
conn_str = user + "/" + pwd + "@" + host + ":" + port + "/" + db
#conn_str = 'ClAIMUSER' + "/" + pwd + "@" + host + ":" + port + "/" + 'CCDatabase'
conn = cx_Oracle.connect(conn_str)

del host, port, db, user, pwd, conn_str


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
query = 'select * from cc_contact'
cc_contact = pd.read_sql(query,con = conn)
query = 'select * from cc_exposure'
cc_exposure = pd.read_sql(query,con = conn)
query = 'select * from cctl_losscause'
cctl_losscause = pd.read_sql(query,con = conn)
query = 'select * from cc_transactionlineitem'
cc_transactionlineitem = pd.read_sql(query,con = conn)
query = 'select * from cc_reserveline'
cc_reserveline = pd.read_sql(query,con = conn)
query = 'select * from cc_claimcontact'
cc_claimcontact = pd.read_sql(query,con = conn)

del query


cc_claim.to_csv(intermediate_dir_path + "cc_claim.csv", encoding='utf-8', index=False)
cc_Incident.to_csv(intermediate_dir_path + "cc_Incident.csv", encoding='utf-8', index=False)
cc_transaction.to_csv(intermediate_dir_path + "cc_transaction.csv", encoding='utf-8', index=False)
cc_activity.to_csv(intermediate_dir_path + "cc_activity.csv", encoding='utf-8', index=False)
cc_policy.to_csv(intermediate_dir_path + "cc_policy.csv", encoding='utf-8', index=False)
cctl_incident.to_csv(intermediate_dir_path + "cctl_incident.csv", encoding='utf-8', index=False)
cc_check.to_csv(intermediate_dir_path + "cc_check.csv", encoding='utf-8', index=False)
cc_address.to_csv(intermediate_dir_path + "cc_address.csv", encoding='utf-8', index=False)
cc_contact.to_csv(intermediate_dir_path + "cc_contact.csv", encoding='utf-8', index=False)
cc_exposure.to_csv(intermediate_dir_path + "cc_exposure.csv", encoding='utf-8', index=False)
cctl_losscause.to_csv(intermediate_dir_path + "cctl_losscause.csv", encoding='utf-8', index=False)
cc_transactionlineitem.to_csv(intermediate_dir_path + "cc_transactionlineitem.csv", encoding='utf-8', index=False)
cc_reserveline.to_csv(intermediate_dir_path + "cc_reserveline.csv", encoding='utf-8', index=False)
cc_claimcontact.to_csv(intermediate_dir_path + "cc_claimcontact.csv", encoding='utf-8', index=False)


cc_claim2 = cc_claim.copy()
cc_address2 = cc_address.copy()
cc_policy2 = cc_policy.copy()
cc_check2 = cc_check.copy()
cc_exposure2 = cc_exposure.copy()
cc_contact2 = cc_contact.copy()
cc_claimcontact2 = cc_claimcontact.copy()


############################### Select Desired Columns in Datatables ############################
################# Will do same with SQL query only once we are done with column selection #######
cc_claim = cc_claim[['ID','REPORTEDDATE','LITIGATIONSTATUS', 'CLAIMTIER', 'LOSSCAUSE', 'LOSSDATE', \
                     'ASSIGNEDUSERID', 'INSUREDDENORMID', 'CLAIMNUMBER', 'POLICYID', 'LOSSTYPE', \
                     'LOSSLOCATIONID']]

cc_address = cc_address[['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 'CITY', 'POSTALCODE']]

cc_policy = cc_policy[['ID', 'REPORTINGDATE', 'EFFECTIVEDATE', 'EXPIRATIONDATE', 'POLICYNUMBER']]

cc_check['CLAIMCONTACTID'].nunique()
cc_check = cc_check[['CLAIMID', 'PAYTO', 'REPORTABLEAMOUNT', 'PAYMENTMETHOD', 'CLAIMCONTACTID']]

cc_exposure = cc_exposure[['ID','COVERAGEID', 'EXAMINATIONDATE', 'DEPRECIATEDVALUE', 'INCIDENTID', \
                           'REPLACEMENTVALUE', 'LOSTPROPERTYTYPE', 'CREATETIME', 'CLAIMID', \
                           'CLAIMANTDENORMID']]

cc_contact = cc_contact[['TAXID', 'ID', 'LASTNAME', 'FIRSTNAME', 'EMPLOYEENUMBER', 'NAME', \
                         'GREENCARDNUMBER', 'GENDER', 'MARITALSTATUS']]

cc_claimcontact = cc_claimcontact[['ID', 'CONTACTID']]

################## Preprocessing Columns #########################################################
cc_exposure.rename(columns={'ID':'EXPOSUREID'}, inplace=True)
cc_exposure.rename(columns={'CLAIMANTDENORMID':'CONTACTID'}, inplace=True)

cc_claim.rename(columns={'ID':'CLAIMID'}, inplace=True)

cc_address['ADDRESSLINE1']= cc_address['ADDRESSLINE1'].astype(str)
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.upper()
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('[^A-Za-z]+\s', '')
cc_address['ADDRESSLINE1'] = cc_address['ADDRESSLINE1'].str.replace('.', '')


cc_check['PAYTO']= cc_check['PAYTO'].astype(str)
cc_check['PAYTO'] = cc_check['PAYTO'].str.upper()
cc_check['PAYTO'] = cc_check['PAYTO'].str.replace('[^A-Za-z]+\s', '')
cc_check['PAYTO'] = cc_check['PAYTO'].str.replace('.', '')


cc_transaction.rename(columns={'ID':'TRANSACTIONID'}, inplace=True)
cc_transaction['CREATETIME'] = pd.to_datetime(cc_transaction["CREATETIME"])




cc_contact.rename(columns={'ID':'CONTACTID'}, inplace=True)
cc_claimcontact.rename(columns={'ID':'CLAIMCONTACTID'}, inplace=True)

################## Merge Data Tables ############################################################
cc_claim = cc_claim.merge(cc_address, 'left', left_on = 'LOSSLOCATIONID', right_on = 'ID')
cc_claim = cc_claim.merge(cc_policy, 'left', left_on = 'POLICYID', right_on = 'ID')

cc_check = cc_check.merge(cc_claimcontact, 'left')
#################### Number of approavals for each claim #########################################
multiple_approvals = cc_activity.groupby('CLAIMID').agg({'UPDATEUSERID': 'nunique'})
multiple_approvals_grp = multiple_approvals.groupby('UPDATEUSERID').agg({'UPDATEUSERID': 'count'})

###################################################################################################


############## Fraud Scenerio 1 :Dupicate claims and payments for same cause ############################

amt_paid_dat = cc_check.groupby(['CLAIMID', 'PAYTO'], as_index = False).agg({'REPORTABLEAMOUNT' : 'sum'})

claim_amt_dat = cc_claim.merge(amt_paid_dat, 'inner', on = 'CLAIMID')
claim_amt_dat.columns.values
claim_amt_dat['FraudScene1'] = claim_amt_dat.duplicated(['REPORTABLEAMOUNT', 'POLICYNUMBER'\
                ,'LOSSCAUSE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'POSTALCODE', 'PAYTO'], False)

cc_claim_f1 = claim_amt_dat.loc[claim_amt_dat['FraudScene1'] == True]


######################### Fraud Scenerio 2 : Slight change in address ##################################
claim_amt_dat['FraudScene2'] = claim_amt_dat.duplicated(['REPORTABLEAMOUNT', 'POLICYNUMBER'\
                ,'LOSSCAUSE', 'POSTALCODE', 'PAYTO'], False)

cc_claim_f2 = claim_amt_dat.loc[claim_amt_dat['FraudScene2'] == True]
cc_claim_f2 = cc_claim_f2.loc[cc_claim_f2['FraudScene1'] == False]

cc_claim_f2_grp = cc_claim_f2.groupby(['REPORTABLEAMOUNT', 'POLICYNUMBER'\
                ,'LOSSCAUSE', 'POSTALCODE', 'PAYTO'], as_index = False).agg({'ADDRESSLINE1' : 'unique'})
cc_claim_f2_grp.rename(columns={'ADDRESSLINE1':'Grp_ADDRESSLINE'}, inplace=True)

cc_claim_f2 = cc_claim_f2.merge(cc_claim_f2_grp)
del cc_claim_f2_grp, claim_amt_dat

cc_claim_f2['FuzzyAddress'] = ''
cc_claim_f2['FuzzyAddressScore'] = np.nan

for x in range(len(cc_claim_f2.index)) :
    cc_claim_f2.loc[x, 'FuzzyAddress'] = process.extract(cc_claim_f2['ADDRESSLINE1'][x], 
               cc_claim_f2['Grp_ADDRESSLINE'][x], scorer = fuzz.token_sort_ratio)[1][0]
    cc_claim_f2.loc[x, 'FuzzyAddressScore'] = process.extract(cc_claim_f2['ADDRESSLINE1'][x], 
               cc_claim_f2['Grp_ADDRESSLINE'][x], scorer = fuzz.token_sort_ratio)[1][1]

cc_claim_f2 = cc_claim_f2.loc[cc_claim_f2['FuzzyAddressScore'] >= Thresold_Address_Fuzzy_Match]
del Thresold_Address_Fuzzy_Match, x

######################## Fraud Scenerio 3 : Manual Cheque Fraud #######################################################


cc_check = cc_check.merge(cc_claim[['CLAIMID', 'ASSIGNEDUSERID']], 'left')

cc_check_manual = cc_check.groupby(['ASSIGNEDUSERID','PAYMENTMETHOD'], as_index= False).agg(\
        {'REPORTABLEAMOUNT' : 'sum'})

#cc_check_manual = cc_check_manual.loc[cc_check_manual['PAYMENTMETHOD'] == 1]

cc_check_grp = cc_check.groupby(['ASSIGNEDUSERID'], as_index= False).agg({'REPORTABLEAMOUNT' : 'sum'})
cc_check_grp.rename(columns={'REPORTABLEAMOUNT':'Ttl_REPORTABLEAMOUNT'}, inplace=True)

cc_check_manual = cc_check_manual.merge(cc_check_grp, 'left')
del cc_check_grp
cc_check_manual = cc_check_manual.loc[cc_check_manual['PAYMENTMETHOD'] == 1]
cc_check_manual['PercentageManual'] = 100 * cc_check_manual['REPORTABLEAMOUNT'] / cc_check_manual['Ttl_REPORTABLEAMOUNT']



cc_check_manual = cc_check_manual.loc[cc_check_manual['PercentageManual'] > Thresold_Perc_Manual_Payment]

cc_claim_f3 = cc_claim.merge(cc_check_manual, 'inner')

del cc_check_manual, Thresold_Perc_Manual_Payment


#######################################################################################################

##################### Fraud 4a : Adjustor- Reciever Pair Insured Pair #################################################


pair_adjustor_receiver = cc_check.groupby(['PAYTO','ASSIGNEDUSERID'], as_index = False).agg({\
                                         'REPORTABLEAMOUNT' : 'sum', 'CLAIMID' : 'nunique'})


pair_adjustor_receiver = pair_adjustor_receiver.sort_values('REPORTABLEAMOUNT', ascending=False)
pair_adjustor_receiver.rename(columns={'CLAIMID':'count_CLAIMID'}, inplace=True)

Thresold_Percentile = pair_adjustor_receiver['REPORTABLEAMOUNT'].quantile(.75)

cc_claim_f4 = pair_adjustor_receiver.loc[
        pair_adjustor_receiver['REPORTABLEAMOUNT'] >= Thresold_Percentile]


receiver_amt_dat = cc_check.groupby(['PAYTO'], as_index = False).agg({'REPORTABLEAMOUNT' : 'sum'})

##################### Fraud 4b : Adjustor- Claimant Pair Insured Pair #################################################


pair_adjustor_claimant = cc_check.groupby(['CONTACTID','ASSIGNEDUSERID'], as_index = False).agg({\
                                         'REPORTABLEAMOUNT' : 'sum', 'CLAIMID' : 'nunique'})


pair_adjustor_claimant = pair_adjustor_claimant.sort_values('REPORTABLEAMOUNT', ascending=False)
pair_adjustor_claimant.rename(columns={'CLAIMID':'count_CLAIMID'}, inplace=True)

Thresold_Percentile = pair_adjustor_claimant['REPORTABLEAMOUNT'].quantile(.75)

cc_claim_f4b = pair_adjustor_claimant.loc[
        pair_adjustor_claimant['REPORTABLEAMOUNT'] >= Thresold_Percentile]


claimant_amt_dat = cc_check.groupby(['CONTACTID'], as_index = False).agg({'REPORTABLEAMOUNT' : 'sum'})



#####################################################################################################

####################### Fraud 5 : Adjustor Overpaying for certain cause ###################################

cc_check = cc_check.merge(cc_claim[['CLAIMID', 'LOSSCAUSE']], 'left')

cause_pymt = cc_check.groupby('LOSSCAUSE', as_index = False).agg({'REPORTABLEAMOUNT' : ['mean', 'std']})
cause_pymt.columns = ['LOSSCAUSE', 'Mean_Payment', 'Std_Payment']

cc_check = cc_check.merge(cause_pymt)

cc_check_f5 = cc_check.loc[cc_check['REPORTABLEAMOUNT'] > (cc_check['Mean_Payment'] \
                                       + num_std_dev * cc_check['Std_Payment'])]


###################################################################################
cc_exposure.columns.values
cc_contact.columns.values

###############################################################################################
##################### Fraud : Adjustor approving more than claim ##############################

reserve_dat = cc_transaction.loc[cc_transaction['SUBTYPE'] == 2]
reserve_dat.columns.values
reserve_dat = reserve_dat[['CREATETIME', 'CLAIMID','EXPOSUREID', 'TRANSACTIONID', 'CLAIMCONTACTID']]

reserve_dat['CLAIMID'].nunique()
reserve_dat[['CLAIMID', 'EXPOSUREID']].nunique()
reserve_dat.groupby(['CLAIMID', 'EXPOSUREID']).ngroups

reserve_dat = reserve_dat.sort_values('CREATETIME', ascending = True)
reserve_first = reserve_dat.groupby(['CLAIMID', 'EXPOSUREID'], as_index = False).first()
reserve_first['TRANSACTIONID'].nunique()

cc_transactionlineitem['TRANSACTIONID'].nunique()

amt_claimed = cc_transactionlineitem.groupby('TRANSACTIONID', as_index = False).agg({'REPORTINGAMOUNT' : 'sum'})

reserve_first = reserve_first.merge(amt_claimed, 'left')
reserve_first = reserve_first.merge(cc_exposure[['CONTACTID', 'EXPOSUREID']], 'left')


amt_claimant_received = cc_check.groupby(['CLAIMID', 'CONTACTID'], as_index = False).agg(\
                                        {'REPORTABLEAMOUNT' : 'sum'})

payment_claimed_dat = reserve_first.merge(amt_claimant_received, 'left')

payment_claimed_dat = payment_claimed_dat.merge(cc_claim[['CLAIMID', 'ASSIGNEDUSERID']], 'left')

payment_claimed_dat = payment_claimed_dat[['CLAIMID', 'CONTACTID', 'ASSIGNEDUSERID',\
                                          'REPORTINGAMOUNT', 'REPORTABLEAMOUNT']]

payment_claimed_dat['Claim_Approve_Ratio'] = payment_claimed_dat['REPORTABLEAMOUNT']/payment_claimed_dat[\
                   'REPORTINGAMOUNT']


payment_claimed_f6 = payment_claimed_dat.loc[payment_claimed_dat['Claim_Approve_Ratio'] > 1]




######################################### 
#select CLAIMANTDENORMID,ClaimID from cc_exposure;
#
#select claimnumber,LOSSCAUSE from cc_claim where ID = 114;
#
#select Name from CCTL_LOSSCAUSE where ID = 10034;
#
#select Firstname,Lastname,Name from cc_contact where ID = 504;
#
#select * from CC_TRANSACTION where SUBTYPE = 2;
#select * from cctl_transaction; 
# 




