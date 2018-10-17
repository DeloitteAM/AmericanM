# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 11:51:33 2018

@author: dmiglani
"""

######################### Import Libraries ####################################
import os

import pandas as pd
import numpy as np

import configparser
import cx_Oracle

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.graph_objs import *

import matplotlib.pyplot as plt

from collections import Counter

############### Constants ######################################################
num_std_dev = 2

################################# Read INI File ###############################
ini_path = 'C:\\Users\\dmiglani\\Desktop\\ModernAmerican\\config_modern_am_oracle.ini'

config = configparser.ConfigParser()
config.read(ini_path)

proj_path = config['PATH']['Project Directory']

host = config['Oracle_Connect']['Host']
port = config['Oracle_Connect']['Port']
db = config['Oracle_Connect']['Database']
user = config['Oracle_Connect']['User_ID']
pwd = config['Oracle_Connect']['Password']

del config

################################# Import functions ############################
os.chdir(proj_path + '/04.Codes/Functions')
from preprocess import column_preprocess
os.chdir(proj_path)

intermediate_dir_path = proj_path + "/05.Intertmediate/Pekin/"
del proj_path

################################## Connect to oracle DB #######################
conn_str = user + "/" + pwd + "@" + host + ":" + port + "/" + db
conn_str = 'ClAIMUSER' + "/" + pwd + "@" + host + ":" + port + "/" + 'CCDatabase'
conn = cx_Oracle.connect(conn_str)

del host, port, db, user, pwd, conn_str

######################## Read Oracle DB #######################################
read_db_path = '04.Codes/read_db.py'
exec(compile(open(read_db_path, "rb").read(), read_db_path, 'exec'))

################## Select desired columns #####################################

cc_claim = cc_claim[['ID','REPORTEDDATE','LITIGATIONSTATUS', 'LOSSCAUSE', 
                     'LOSSDATE', 'ASSIGNEDUSERID', 'INSUREDDENORMID', 
                     'CLAIMNUMBER', 'POLICYID', 'LOSSTYPE', 'LOSSLOCATIONID',
                     'CATASTROPHEID', 'LOBCODE', 'STATE']]

cc_address = cc_address[['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 'CITY', 
                         'POSTALCODE']]

cc_policy = cc_policy[['ID', 'REPORTINGDATE', 'EFFECTIVEDATE', 'EXPIRATIONDATE', 
                       'POLICYNUMBER']]

cc_check = cc_check[['ID', 'CLAIMID', 'PAYTO', 'REPORTABLEAMOUNT', 'PAYMENTMETHOD', 
                     'CLAIMCONTACTID']]

cc_exposure = cc_exposure[['ID','COVERAGEID', 'EXAMINATIONDATE', 
                           'DEPRECIATEDVALUE', 'INCIDENTID', 'REPLACEMENTVALUE', 
                           'LOSTPROPERTYTYPE', 'CREATETIME', 'CLAIMID',
                           'CLAIMANTDENORMID']]

cc_contact = cc_contact[['TAXID', 'ID', 'LASTNAME', 'FIRSTNAME', 
                         'EMPLOYEENUMBER', 'NAME','GREENCARDNUMBER',
                         'PRIMARYADDRESSID', 'ADDRESSBOOKUID']]

cc_claimcontact = cc_claimcontact[['ID', 'CONTACTID']]

cctl_losscause = cctl_losscause[['ID', 'DESCRIPTION']]

cc_checkpayee = cc_checkpayee[['ID', 'CHECKID', 'PAYEEDENORMID', 'CLAIMCONTACTID']]

cc_activity = cc_activity[['CLAIMID', 'CREATETIME', 'UPDATEUSERID']]

cc_user = cc_user[['ID','AUTHORITYPROFILEID', 'EXPERIENCELEVEL']]

cc_authoritylimit = cc_authoritylimit[['PROFILEID', 'LIMITAMOUNT', 'LIMITTYPE']]

cc_claimIndicator = cc_claimIndicator[['CLAIMID', 'SUBTYPE', 'ISON']]

cctl_userexperiencetype = cctl_userexperiencetype[['ID', 'NAME']]

#################### Filter tables ############################################
cc_claim = cc_claim.loc[cc_claim['STATE'] != 1] #Remove Draft Claims

###Filter HO claims
#cc_claim = cc_claim.loc[cc_claim['LOBCODE'] == 10005] 

cc_check = cc_check.loc[cc_check.CLAIMID.isin(cc_claim.ID)]
cc_exposure = cc_exposure.loc[cc_exposure.CLAIMID.isin(cc_claim.ID)]
cc_activity = cc_activity.loc[cc_activity.CLAIMID.isin(cc_claim.ID)]
cc_claimIndicator = cc_claimIndicator.loc[
        cc_claimIndicator.CLAIMID.isin(cc_claim.ID)]


#Filter Payment Limit
cc_authoritylimit = cc_authoritylimit.loc[cc_authoritylimit['LIMITTYPE'] == 2] 
cc_authoritylimit = cc_authoritylimit.drop(columns = ['LIMITTYPE'])

#Filter  Severeity Indicator Flag 
cc_claimIndicator = cc_claimIndicator.loc[cc_claimIndicator['SUBTYPE'] == 4]
cc_claimIndicator = cc_claimIndicator.drop(columns = ['SUBTYPE'])

################## Preprocessing Columns ######################################
cc_exposure.rename(columns={'ID':'EXPOSUREID'}, inplace=True)
cc_exposure.rename(columns={'CLAIMANTDENORMID':'CONTACTID'}, inplace=True)
cc_claim.rename(columns={'ID':'CLAIMID'}, inplace=True)
cc_transaction.rename(columns={'ID':'TRANSACTIONID'}, inplace=True)
cc_contact.rename(columns={'ID':'CONTACTID'}, inplace=True)
cc_claimcontact.rename(columns={'ID':'CLAIMCONTACTID'}, inplace=True)
cc_address.rename(columns={'ID':'ADDRESSID'}, inplace=True)
cc_policy.rename(columns={'ID':'POLICYID'}, inplace=True)
cctl_losscause.rename(columns={'ID':'LOSSCAUSE'}, inplace=True)
cctl_losscause.rename(columns={'DESCRIPTION':'LOSSDESCRIPTION'}, inplace=True)
cc_user.rename(columns={'ID':'USERID'}, inplace=True)
cctl_userexperiencetype.rename(columns = {'NAME' : 'ADJUSTOR_EXPERIENCE'},
                               inplace=True)
cc_check.rename(columns={'ID':'CHECKID'}, inplace=True)



cc_address = column_preprocess(cc_address, ['ADDRESSLINE1'])
cc_check = column_preprocess(cc_check, ['PAYTO'])
cc_contact = column_preprocess(cc_contact,['FIRSTNAME', 'LASTNAME', 'NAME'])

cc_transaction['CREATETIME'] = pd.to_datetime(cc_transaction["CREATETIME"])
cc_activity['CREATETIME'] = pd.to_datetime(cc_activity["CREATETIME"])

################## Merge Data Tables ##########################################

loss_address = cc_address.copy()
loss_address.columns = ['LOSS_' + str(col) for col in loss_address.columns]
cc_claim = cc_claim.merge(loss_address, 'left', 
                          left_on = 'LOSSLOCATIONID', 
                          right_on = 'LOSS_ADDRESSID')
del loss_address
cc_claim = cc_claim.merge(cc_policy, 'left')
cc_claim = cc_claim.merge(cc_claimIndicator, 'left') 

freq = Counter(cc_claim['ISON'])
cc_claim['ISON'] = cc_claim['ISON'].astype(float)
cc_claim['ISON'] = np.where(np.isnan(cc_claim['ISON']), 0, cc_claim['ISON'])

del cc_address, cc_policy, cc_claimIndicator

cc_check = cc_check.merge(cc_claimcontact, 'left')
cc_check = cc_check.merge(cc_claim[['CLAIMID', 'ASSIGNEDUSERID','LOSSCAUSE',
                                    'LOSS_POSTALCODE']], 'left')
cc_check = cc_check.merge(cctl_losscause, 'left')
cc_check = cc_check.merge(cc_contact[['CONTACTID', 'FIRSTNAME', 
                                      'LASTNAME', 'NAME', 'ADDRESSBOOKUID']], 'left')

############## Last Assigned Approval #########################################
cc_activity = cc_activity.sort_values('CREATETIME', ascending = False)
cc_activity_last = cc_activity.groupby('CLAIMID', as_index = False).first()

cc_check = cc_check.merge(cc_activity_last[['CLAIMID', 'UPDATEUSERID']], 'left')
cc_check['LastAssignedUser'] = np.where(np.isnan(cc_check['UPDATEUSERID']), 
        cc_check['ASSIGNEDUSERID'],
        cc_check['UPDATEUSERID'])

cc_check = cc_check.merge(cc_user, 'left', left_on = 'LastAssignedUser',
                          right_on = 'USERID')
cc_check = cc_check.merge(cctl_userexperiencetype, 'left', 
                          left_on = 'EXPERIENCELEVEL', 
                          right_on = 'ID')

cc_claim = cc_claim.merge(cc_activity_last[['CLAIMID', 'UPDATEUSERID']], 'left')
cc_claim['LastAssignedUser'] = np.where(np.isnan(cc_claim['UPDATEUSERID']), 
        cc_claim['ASSIGNEDUSERID'],
        cc_claim['UPDATEUSERID'])

cc_claim = cc_claim.merge(cc_user, 'left', left_on = 'LastAssignedUser',
                          right_on = 'USERID')
cc_claim = cc_claim.merge(cctl_userexperiencetype, 'left', 
                          left_on = 'EXPERIENCELEVEL', 
                          right_on = 'ID')

Counter(cc_claim['ADJUSTOR_EXPERIENCE'])
#################### Useful Tables ############################################
claim_payment = cc_check.groupby('CLAIMID', as_index= False).\
    agg({'REPORTABLEAMOUNT' : 'sum'})
claim_payment.rename(columns={'REPORTABLEAMOUNT':'ClaimPayment'}, 
                     inplace=True)

cc_claim = cc_claim.merge(claim_payment, 'left')
cc_claim['ClaimPayment'].fillna(0, inplace = True)

############# TAX ID  Merge ###################################################
cc_checkpayee = cc_checkpayee.merge(cc_contact, 'inner',
                                    left_on = 'PAYEEDENORMID',
                                    right_on = 'CONTACTID')

Counter(cc_checkpayee['ADDRESSBOOKUID'])

cc_checkpayee['VendorIndicator'] = np.where(pd.isna(cc_checkpayee['ADDRESSBOOKUID']), 0, 1)
Counter(cc_checkpayee['VendorIndicator'])

vendor_dat = cc_checkpayee.loc[cc_checkpayee['VendorIndicator'] == 1]
vendor_dat.columns = ['VENDOR_' + str(col) for col in vendor_dat.columns]

vendor_dat['VENDOR_CHECKID'].nunique()


cc_check = cc_check.merge(vendor_dat, 'left',
                          left_on = 'CHECKID',
                          right_on = 'VENDOR_CHECKID')

###############################################################################

#################### Number of approvals for each claim ######################

approvals_dat = cc_activity.groupby('CLAIMID', as_index = False).\
    agg({'UPDATEUSERID': 'nunique'})
approvals_dat.rename(columns={'UPDATEUSERID':'NumApprovals'}, inplace=True)

cc_claim = cc_claim.merge(approvals_dat, 'left')
cc_claim['NumApprovals'].fillna(0, inplace = True) 
 
approvals_summary = cc_claim.groupby('NumApprovals', as_index = False).\
    agg({'CLAIMID' : 'nunique', 'ClaimPayment' : 'sum'})
    
approvals_summary.rename(columns={'CLAIMID':'NumClaims', 
                                  'ClaimPayment' : 'Payment'}, 
                         inplace=True)
approvals_summary = approvals_summary.sort_values('NumApprovals', ascending = True)

approvals_summary['NumApprovals'] = np.where(approvals_summary['NumApprovals'] >=3,
                 3, approvals_summary['NumApprovals'])
approvals_summary['NumApprovals'] = approvals_summary['NumApprovals'].astype(int).astype(str)
approvals_summary['NumApprovals'] = np.where(approvals_summary['NumApprovals'] == "3",
                 ">=3", approvals_summary['NumApprovals'])

approvals_summary = approvals_summary.groupby('NumApprovals', as_index = False).\
    agg({'NumClaims' :'sum', 'Payment' : 'sum'})

approvals_summary['Avg_Payment'] = approvals_summary['Payment']/approvals_summary['NumClaims']

##approvals_summary.to_csv(intermediate_dir_path + "Visualization/Pekin_approval_freq.csv", 
##                   encoding='utf-8', index=False)
#
###  Visualization : Distribution of Number of Approvals 
#labels = approvals_summary['NumApprovals']
#values = approvals_summary['NumClaims']
#colors = ['#FEBFB3', '#E1396C', '#96D38C', '#D0F9B1']
#
#trace = go.Pie(labels=labels, values=values,
#               hoverinfo='value', textinfo='percent', sort = False,
#               textfont=dict(size=20),
#               marker=dict(colors=colors, 
#                           line=dict(color='#000000', width=2)))
#
#data = [trace]
#layout = Layout(
#    title = "Distribution of Number of Approvals",
#    showlegend=True,
#    height=600,
#    width=600
#)
#
#fig = dict( data=data, layout=layout )
#
#plot(fig)  
#
#
### Visulaization Dollar
#labels = approvals_summary['NumApprovals']
#values = approvals_summary['Payment']
#colors = ['#FEBFB3', '#E1396C', '#96D38C', '#D0F9B1']
#
#trace = go.Pie(labels=labels, values=values,
#               hoverinfo='value', textinfo='percent', sort = False,
#               textfont=dict(size=20),
#               marker=dict(colors=colors, 
#                           line=dict(color='#000000', width=2)))
#
#data = [trace]
#layout = Layout(
#    title = "Distribution of Number of Approvals (Dolarwise) ",
#    showlegend=True,
#    height=600,
#    width=600
#)
#
#fig = dict( data=data, layout=layout )
#
#plot(fig)  

######################## Fraud Scenerio 3 : Manual Cheque Fraud ###############

### Need to do by adjustor level : #####################################
### Compare the manual payment across adjustors (identify outliers)


approval_mthd_pymt = cc_check.groupby(['LastAssignedUser','PAYMENTMETHOD'], 
                                 as_index= False).\
                                 agg({'REPORTABLEAMOUNT' : 'sum'})

approval_pymt = cc_check.groupby(['LastAssignedUser'], 
                                 as_index= False).\
                                 agg({'REPORTABLEAMOUNT' : 'sum'})                                 
approval_pymt.rename(columns={'REPORTABLEAMOUNT':'Ttl_REPORTABLEAMOUNT'}, 
                     inplace=True)

approval_mthd_pymt['Manual_Check_Amt'] = np.where(\
                  approval_mthd_pymt['PAYMENTMETHOD']== 1, 
                  approval_mthd_pymt['REPORTABLEAMOUNT'], 0)

approval_manual_pymt = approval_mthd_pymt.groupby('LastAssignedUser',
                                                  as_index = False).\
                                                  agg({'Manual_Check_Amt' : 'sum'})
                                                  
approval_manual_pymt = approval_manual_pymt.merge(approval_pymt, 'left')                                                  
                                                  

approval_manual_pymt['PercentageManual'] = \
    100 * approval_manual_pymt['Manual_Check_Amt'] / approval_manual_pymt['Ttl_REPORTABLEAMOUNT']
    
#approval_manual_pymt.to_csv(intermediate_dir_path + "Visualization/Test_manual_check.csv", 
#                   encoding='utf-8', index=False)

Thresold_Perc_Manual_Payment = np.mean(approval_manual_pymt['PercentageManual']) + \
    np.std(approval_manual_pymt['PercentageManual'])

approval_manual_pymt = approval_manual_pymt.loc[approval_manual_pymt['PercentageManual'] \
                                                > Thresold_Perc_Manual_Payment]

f3_claim = cc_claim.merge(approval_manual_pymt, 'inner')

del Thresold_Perc_Manual_Payment

### Visulaization
#
#trace = go.Histogram(
#    x=approval_manual_pymt['PercentageManual'],
#    xbins = dict(start=0,end=25,size=5),
#    autobinx = False, 
#    marker = dict(color='#EB89B5'),
#    opacity=0.75
#)
#data = [trace]
#layout = Layout(
#    title = "Manual Check Percentage",
#    showlegend=False,
#    height=600,
#    width=600
#)
#
#fig = dict( data=data, layout=layout )
#
#plot(fig)  

################ Fraud 5 : Adjustor Overpaying for certain cause ##############


cause_pymt = cc_check.groupby('LOSSCAUSE', 
                              as_index = False).\
                              agg({'REPORTABLEAMOUNT' : ['mean', 'std']})                         
cause_pymt.columns = ['LOSSCAUSE', 'Mean_Payment', 'Std_Payment']

cc_check = cc_check.merge(cause_pymt, 'left') 

cc_check['Cause_Thresold'] = cc_check['Mean_Payment'] + \
    num_std_dev * cc_check['Std_Payment']
    
f5_check = cc_check
f5_check['ExtraThresold'] = f5_check['REPORTABLEAMOUNT'] - \
    f5_check['Cause_Thresold']
    
f5_check['ExtraThresold'] = np.where(f5_check['ExtraThresold'] > 0,
        f5_check['ExtraThresold'], 0)

f5_summary = f5_check.groupby('LOSSDESCRIPTION', as_index = False).\
    agg({'ExtraThresold':'sum', 'REPORTABLEAMOUNT' : 'sum'})
    
f5_summary['ExcessPaidAmt'] = 100 * \
    f5_summary['ExtraThresold']/ f5_summary['REPORTABLEAMOUNT']

f5_summary = f5_summary.sort_values('ExcessPaidAmt', ascending = False) #order the reserves

#f5_summary.to_csv(intermediate_dir_path + "Visualization/Pekin_cause_extra_3sd.csv", 
#                   encoding='utf-8', index=False)



###  Visualization 
#trace = go.Bar(x = f5_summary['LOSSDESCRIPTION'],
#              y = f5_summary['ExcessPaidAmt'])
#          
#
#data = [trace]
#layout = Layout(title = "Cause v/s Extra Payment Done",
#                height=600, width=600,)
#
#fig = dict( data=data, layout=layout )
#
#plot(fig)  



############### Adjustor - Vendor Pair ########################################
cc_check.columns.values

## Fuzzy Match
adjustor_payto = cc_check.groupby(['LastAssignedUser'], 
                                  as_index = False).\
                                  agg({'PAYTO' : 'unique'})                                  
adjustor_payto.rename(columns={'PAYTO':'all_PAYTO'}, inplace=True)
adjustor_payto['all_PAYTO'] = adjustor_payto['all_PAYTO'].astype(tuple)


pair_adjustor_receiver = cc_check.groupby(['LastAssignedUser', 'PAYTO'], 
                                          as_index = False).\
                                          agg({'REPORTABLEAMOUNT' : 'sum', 
                                               'CLAIMID' : 'nunique'})

pair_adjustor_receiver = pair_adjustor_receiver.merge(adjustor_payto)

pair_adjustor_receiver['FuzzyPayTo'] = ''
pair_adjustor_receiver['FuzzyPayToScore'] = np.nan

pair_adjustor_receiver['Replaced_With'] = ''
x = 1



for x in range(len(pair_adjustor_receiver.index)) :
    pay_to = pair_adjustor_receiver['PAYTO'][x]
    pay_to_same_adjustor = pair_adjustor_receiver['all_PAYTO'][x]
    temp = process.extract(pay_to, pay_to_same_adjustor,
                           scorer = fuzz.token_sort_ratio)
    if (len(temp) > 1):
        temp = temp[1]
        pair_adjustor_receiver.loc[x, 'FuzzyPayTo'] = temp[0]
        pair_adjustor_receiver.loc[x, 'FuzzyPayToScore'] = temp[1]
        
Thresold_fuzzy = 83
fuzzy_pair = pair_adjustor_receiver.loc[pair_adjustor_receiver['FuzzyPayToScore'] >= \
                                        Thresold_fuzzy]

fuzzy_pair = fuzzy_pair[['PAYTO', 'FuzzyPayTo']]
fuzzy_pair['Replace_With'] = None
x = 38
replaced_with = []
for x in fuzzy_pair.index :
    if (fuzzy_pair['PAYTO'][x] not in replaced_with) :
        fuzzy_pair.loc[x, 'Replace_With'] = fuzzy_pair['FuzzyPayTo'][x]
        replaced_with.append(fuzzy_pair['FuzzyPayTo'][x])
        
fuzzy_pair = fuzzy_pair[['PAYTO', 'Replace_With']]
fuzzy_pair = fuzzy_pair.loc[~pd.isna(fuzzy_pair['Replace_With'])]

cc_check_fuzzy = cc_check.merge(fuzzy_pair,'left')
cc_check_fuzzy['PAYTO'] = np.where(pd.isna(cc_check_fuzzy['Replace_With']),
              cc_check_fuzzy['PAYTO'], cc_check_fuzzy['Replace_With'])

f1_adj_ven = cc_check_fuzzy.groupby(['LOSS_POSTALCODE','LOSSCAUSE', 
                               'ADJUSTOR_EXPERIENCE', 'LastAssignedUser', 
                               'PAYTO'], as_index = False).\
                               agg({'REPORTABLEAMOUNT' : 'sum',
                                    'CLAIMID' : 'nunique'})
f1_adj_ven.rename(columns={'CLAIMID':'Num_Claims'}, inplace=True)

f1_ven = cc_check.groupby(['LOSS_POSTALCODE','LOSSCAUSE', 
                               'PAYTO'], as_index = False).\
                               agg({'REPORTABLEAMOUNT' : 'sum',
                                    'CLAIMID' : 'nunique'})
f1_ven.rename(columns={'CLAIMID':'Num_Claims_Vendor'}, inplace=True)
f1_ven.rename(columns={'REPORTABLEAMOUNT':'REPORTABLEAMOUNT_Vendor'}, inplace=True)

f1_adj_ven = f1_adj_ven.merge(f1_ven)

f1_adj_ven['Perc_Amount'] = 100 * \
    f1_adj_ven['REPORTABLEAMOUNT'] / f1_adj_ven['REPORTABLEAMOUNT_Vendor']

f1_adj_ven['Perc_Freq'] = 100 * \
    f1_adj_ven['Num_Claims'] / f1_adj_ven['Num_Claims_Vendor']
    

## Reliability Index

###############################################################################
