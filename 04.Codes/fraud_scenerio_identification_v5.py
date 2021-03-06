# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 12:25:34 2018

@author: dmiglani
"""

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
conn_str = 'ClAIMUSER' + "/" + pwd + "@" + host + ":" + port + "/" + db
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

cc_claimcontact = cc_claimcontact[['ID', 'CONTACTID', 'CLAIMID']]

cctl_losscause = cctl_losscause[['ID', 'DESCRIPTION']]

cc_checkpayee = cc_checkpayee[['ID', 'CHECKID', 'PAYEEDENORMID', 'CLAIMCONTACTID']]

cc_activity = cc_activity[['CLAIMID', 'CREATETIME', 'UPDATEUSERID']]

cc_user = cc_user[['ID','AUTHORITYPROFILEID', 'EXPERIENCELEVEL','CREATETIME']]

cc_authoritylimit = cc_authoritylimit[['PROFILEID', 'LIMITAMOUNT', 'LIMITTYPE']]

cc_claimIndicator = cc_claimIndicator[['CLAIMID', 'SUBTYPE', 'ISON']]

cctl_userexperiencetype = cctl_userexperiencetype[['ID', 'NAME']]

cc_catastrophe = cc_catastrophe[['ID', 'TYPE', 'CATASTROPHEVALIDFROM',
                                 'CATASTROPHEVALIDTO']]
cctl_catastrophetype = cctl_catastrophetype[['ID', 'NAME']]

cc_catastrophezone = cc_catastrophezone[['ID', 'CATASTROPHEID', 'COUNTRY', 
                                         'ZONETYPE']]

cctl_zonetype = cctl_zonetype[['ID', 'NAME']]

cctl_contactrole = cctl_contactrole[['ID', 'NAME']]

cc_claimcontactrole = cc_claimcontactrole[['ID', 'CLAIMCONTACTID', 'ROLE']]


#################### Filter tables ############################################
cc_claim = cc_claim.loc[cc_claim['STATE'] != 1] #Remove Draft Claims

###Filter HO claims
cc_claim = cc_claim.loc[cc_claim['LOBCODE'] == 10005] 

cc_check = cc_check.loc[cc_check.CLAIMID.isin(cc_claim.ID)]
cc_exposure = cc_exposure.loc[cc_exposure.CLAIMID.isin(cc_claim.ID)]
cc_activity = cc_activity.loc[cc_activity.CLAIMID.isin(cc_claim.ID)]
cc_claimIndicator = cc_claimIndicator.loc[
        cc_claimIndicator.CLAIMID.isin(cc_claim.ID)]
cc_transaction = cc_transaction.loc[cc_transaction.CLAIMID.isin(cc_claim.ID)]



#Filter Payment Limit
cc_authoritylimit = cc_authoritylimit.loc[cc_authoritylimit['LIMITTYPE'] == 7] 
cc_authoritylimit = cc_authoritylimit.drop(columns = ['LIMITTYPE'])
cc_authoritylimit3 = cc_authoritylimit.drop_duplicates(keep = 'first')

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
cc_catastrophe.rename(columns={'ID':'CATASTROPHEID'}, inplace=True)
cc_catastrophe.rename(columns={'TYPE':'CATASTROPHETYPEID'}, inplace=True)
cctl_catastrophetype.rename(columns={'ID':'CATASTROPHETYPEID'}, inplace=True)
cctl_catastrophetype.rename(columns={'NAME':'CATASTROPHETYPE'}, inplace=True)
cc_catastrophezone.rename(columns={'ID':'CATASTROPHEZONEID'}, inplace=True)
cc_catastrophezone.rename(columns={'ZONETYPE':'ZONETYPEID'}, inplace=True)
cctl_zonetype.rename(columns={'ID':'ZONETYPEID'}, inplace=True)
cctl_zonetype.rename(columns={'NAME':'ZONETYPE'}, inplace=True)
cctl_contactrole.rename(columns={'ID':'ROLE'}, inplace=True)
cctl_contactrole.rename(columns={'NAME':'CLAIM_ROLE'}, inplace=True)

cc_address = column_preprocess(cc_address, ['ADDRESSLINE1'])
cc_check = column_preprocess(cc_check, ['PAYTO'])
cc_contact = column_preprocess(cc_contact,['FIRSTNAME', 'LASTNAME', 'NAME'])

cc_transaction['CREATETIME'] = pd.to_datetime(cc_transaction["CREATETIME"])
cc_activity['CREATETIME'] = pd.to_datetime(cc_activity["CREATETIME"])
cc_user['CREATETIME'] = pd.to_datetime(cc_user["CREATETIME"])
cc_policy['EFFECTIVEDATE'] = pd.to_datetime(cc_policy["EFFECTIVEDATE"])
cc_policy['EXPIRATIONDATE'] = pd.to_datetime(cc_policy["EXPIRATIONDATE"])

cc_address['POSTALCODE'] = cc_address['POSTALCODE'].str[:5]
################### New variables #############################################
cc_user['Experience_Months'] = pd.to_datetime('today').to_period('M') - \
    cc_user['CREATETIME'].dt.to_period('M')
cc_user = cc_user.drop(columns = 'CREATETIME')

cc_policy['Policy_Age'] = pd.to_datetime('today').to_period('M') - \
    cc_policy['EFFECTIVEDATE'].dt.to_period('M')
    
cc_policy['Policy_New'] = np.where(cc_policy['Policy_Age'] < 3, 1,0)



################## Merge Data Tables ##########################################
#cc_user = cc_user.merge(cc_authoritylimit, )
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

del cc_policy, cc_claimIndicator

cc_check = cc_check.merge(cc_claimcontact[['CLAIMCONTACTID', 'CONTACTID']], 
                          'left')
cc_check = cc_check.merge(cc_claim[['CLAIMID', 'ASSIGNEDUSERID','LOSSCAUSE',
                                    'LOSS_POSTALCODE', 'CATASTROPHEID']], 'left')
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

approvals_summary = approvals_summary.groupby('NumApprovals', 
                                              as_index = False).\
                                              agg({'NumClaims' :'sum', 
                                                   'Payment' : 'sum'})

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

#approval_manual_pymt = approval_manual_pymt.loc[approval_manual_pymt['PercentageManual'] \
#                                                > Thresold_Perc_Manual_Payment]

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

################ Patterns of historical payments for each adjuster compared to the other adjusters (and this is done by experience level)  ##############

cc_check_non_cat = cc_check.loc[pd.isna(cc_check['CATASTROPHEID'])]
cause_pymt = cc_check_non_cat.groupby(['LOSSCAUSE', 'ADJUSTOR_EXPERIENCE'],
                              as_index = False).\
                              agg({'REPORTABLEAMOUNT' : ['mean', 'std']}) 
                              
cause_pymt.columns = ['LOSSCAUSE', 'ADJUSTOR_EXPERIENCE', 
                      'Mean_Payment', 'Std_Payment']

cc_check = cc_check.merge(cause_pymt, 'left') 

cc_check['Cause_Thresold'] = cc_check['Mean_Payment'] + \
    num_std_dev * cc_check['Std_Payment']
    
f5_check = cc_check.copy()
f5_check['ExtraThresold'] = f5_check['REPORTABLEAMOUNT'] - \
    f5_check['Cause_Thresold']
    
f5_check['ExtraThresold'] = np.where(f5_check['ExtraThresold'] > 0,
        f5_check['ExtraThresold'], 0)

f5_summary = f5_check.groupby(['LOSSDESCRIPTION', 'ADJUSTOR_EXPERIENCE'],
                              as_index = False).\
                              agg({'ExtraThresold':'sum', 
                                   'REPORTABLEAMOUNT' : 'sum'})
    
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



############### Adjustor - Vendor Pair(Multiple/ Fraudulent payments to same party/vendor) ########################################
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

################# Auhoriser History ###########################################
################Adjusters with a history of irresponsible behavior 
#############(Number of claims guidelines / rules broken)#############    
f7_adj = cc_claim.groupby('LastAssignedUser', as_index = False).\
    agg({'ISON' : 'sum', 'CLAIMID' : 'nunique'})

f7_adj['PercFraud'] = f7_adj['ISON']/f7_adj['CLAIMID']

f7_adj = f7_adj.sort_values('PercFraud', ascending = False)


############################################################################### 
############### Authority level fraud(Catastrophe) ###################################################

cc_claim_catastrophe = cc_claim.loc[~pd.isna(cc_claim['CATASTROPHEID'])]
cc_claim_catastrophe = cc_claim_catastrophe.merge(cc_catastrophe, 'left')  

cc_check_catastrophe = cc_check.merge(cc_catastrophe, 'inner')

##Adjustor Overpaying for certain cause in catastrophe


cause_pymt = cc_check_catastrophe.groupby([ 'CATASTROPHEID', 'LOSSCAUSE'], 
                              as_index = False).\
                              agg({'REPORTABLEAMOUNT' : ['mean', 'std']})  
                              
cause_pymt.columns = ['CATASTROPHEID' , 'LOSSCAUSE', 'Mean_Payment', 'Std_Payment']

cc_check_catastrophe = cc_check_catastrophe.merge(cause_pymt, 'left') 

cc_check_catastrophe['Cause_Thresold'] = cc_check_catastrophe['Mean_Payment'] + \
    num_std_dev * cc_check_catastrophe['Std_Payment']
    
f8_check = cc_check_catastrophe.copy()
f8_check.columns.values
f8_check['ExtraThresold'] = f8_check['REPORTABLEAMOUNT'] - \
    f8_check['Cause_Thresold']
    
f8_check['ExtraThresold'] = np.where(f8_check['ExtraThresold'] > 0,
        f8_check['ExtraThresold'], 0)

f8_summary = f8_check.groupby(['CATASTROPHEID','LOSSCAUSE'], as_index = False).\
    agg({'ExtraThresold':'sum', 'REPORTABLEAMOUNT' : 'sum'})
    
f8_summary['ExcessPaidAmt'] = 100 * \
    f8_summary['ExtraThresold']/ f8_summary['REPORTABLEAMOUNT']

f8_summary = f8_summary.sort_values('ExcessPaidAmt', ascending = False) #order the reserves


##################### Fraud : Adjuster paying the insured more than the claim amount (but staying within their authorization limit)
 ##############################

# Only keep reserve data
reserve_dat = cc_transaction.loc[cc_transaction['SUBTYPE'] == 2] 

## Get Claimed Amount for each claimant and claim
reserve_dat = reserve_dat[['CREATETIME', 'CLAIMID','EXPOSUREID', 
                           'TRANSACTIONID', 'CLAIMCONTACTID']]
#Order the reserves
reserve_dat = reserve_dat.sort_values('CREATETIME', 
                                      ascending = True) 

#keep only first reserve
## may be remove if first created is zero

reserve_first = reserve_dat.groupby(['CLAIMID', 'EXPOSUREID'], 
                                    as_index = False).first() 
##  For Non reserve transaction, 
## multiple entries possible in cc_transactionlineitem

amt_claimed = cc_transactionlineitem.groupby('TRANSACTIONID', 
                                             as_index = False).\
                                             agg({'REPORTINGAMOUNT' : 'sum'})

reserve_first = reserve_first.merge(amt_claimed, 'left') #get the claimed amount
reserve_first = reserve_first.merge(cc_exposure[['CONTACTID', 'EXPOSUREID']], 
                                    'left') # get claimant id

reserve_first = reserve_first.merge(cc_contact[['CONTACTID', 
                                                'FIRSTNAME', 'LASTNAME', 
                                                'NAME', 'ADDRESSBOOKUID',
                                                'PRIMARYADDRESSID']], 'left') # get claimant info


## Amt received by claimant on each claim
amt_claimant_received = cc_check.groupby(['CLAIMID', 'FIRSTNAME', 'LASTNAME', 
                                          'NAME'], as_index = False).agg(\
                                        {'REPORTABLEAMOUNT' : 'sum'})

## Merging amount received and claimed together
payment_claimed_dat = reserve_first.merge(amt_claimant_received, 'left')

payment_claimed_dat = payment_claimed_dat.merge(cc_claim[['CLAIMID', 
                                                          'LastAssignedUser']],\
                                                'left')
payment_claimed_dat.columns.values
payment_claimed_dat = payment_claimed_dat[['CLAIMID', 'EXPOSUREID', 'CONTACTID',
                                           'FIRSTNAME', 'LASTNAME', 
                                           'NAME', 'LastAssignedUser',\
                                           'ADDRESSBOOKUID', 'PRIMARYADDRESSID',
                                          'REPORTINGAMOUNT', 'REPORTABLEAMOUNT']]

## fill 0 for payment amount not available


payment_claimed_dat['REPORTABLEAMOUNT'] = np.where(
    np.isnan(payment_claimed_dat['REPORTABLEAMOUNT']),0,
    payment_claimed_dat['REPORTABLEAMOUNT'])    

payment_claimed_dat['Claim_Approve_Ratio'] = \
    payment_claimed_dat['REPORTABLEAMOUNT']/payment_claimed_dat['REPORTINGAMOUNT']

f6_payment_claimed = payment_claimed_dat.loc[payment_claimed_dat['Claim_Approve_Ratio'] > 1]

payment_claimed_grp = f6_payment_claimed.groupby('LastAssignedUser', 
                                                 as_index = False).\
                                                 agg({'REPORTINGAMOUNT' : 'sum', 
                                                      'REPORTABLEAMOUNT' : 'sum', 
                                                      'Claim_Approve_Ratio' : 'mean'})   
    

############### Adjustor Claimant pair ####################
adj_claimed_dat = payment_claimed_dat.loc[~pd.isna(payment_claimed_dat['EXPOSUREID'])]

claimant_address = cc_address.copy()
claimant_address.columns = ['CLAIMANT_' + str(col) for col in 
                            claimant_address.columns]

adj_claimed_dat = adj_claimed_dat.merge(claimant_address, 'left', 
                                        left_on = 'PRIMARYADDRESSID',
                                        right_on = 'CLAIMANT_ADDRESSID')

adj_claimed_dat['FIRSTNAME'] = np.where(adj_claimed_dat['FIRSTNAME'] == "NONE", 
               "", adj_claimed_dat['FIRSTNAME'])
adj_claimed_dat['LASTNAME'] = np.where(adj_claimed_dat['LASTNAME'] == "NONE", 
               "", adj_claimed_dat['LASTNAME'])
adj_claimed_dat['NAME'] = np.where(adj_claimed_dat['NAME'] == "NONE", 
               "", adj_claimed_dat['NAME'])

adj_claimed_dat['NAME_ADDRESS'] = adj_claimed_dat['FIRSTNAME'] + ' & ' +\
    adj_claimed_dat['LASTNAME'] + ' & ' + adj_claimed_dat['NAME'] + ' & ' +\
    adj_claimed_dat['CLAIMANT_ADDRESSLINE1']
    
#cc_exposure = cc_exposure.merge(cc_contact, 'left')
name_add_all = adj_claimed_dat['NAME_ADDRESS'].unique()

adj_claimed_dat['FuzzyKey'] = ''
adj_claimed_dat['FuzzyKeyScore'] = np.nan
adj_claimed_dat['Key_Replaced_With'] = ''

for x in range(len(adj_claimed_dat.index)) :
    key = adj_claimed_dat['NAME_ADDRESS'][x]
    key_all = name_add_all
    temp = process.extract(key, key_all,
                           scorer = fuzz.token_sort_ratio)
    if (len(temp) > 1):
        temp = temp[1]
        adj_claimed_dat.loc[x, 'FuzzyKey'] = temp[0]
        adj_claimed_dat.loc[x, 'FuzzyKeyScore'] = temp[1]
        
Thresold_fuzzy = 80
fuzzy_pair = adj_claimed_dat.loc[adj_claimed_dat['FuzzyKeyScore'] >= \
                                 Thresold_fuzzy]
fuzzy_pair.columns.values
fuzzy_pair = fuzzy_pair[['NAME_ADDRESS', 'FuzzyKey', 'FuzzyKeyScore','FIRSTNAME', 
                         'LASTNAME', 'NAME', 'CLAIMANT_ADDRESSLINE1']]
fuzzy_pair['Replace_With'] = None
replaced_with = []


for x in fuzzy_pair.index :
    if (fuzzy_pair['NAME_ADDRESS'][x] not in replaced_with) :
        fuzzy_pair.loc[x, 'Replace_With'] = fuzzy_pair['FuzzyKey'][x]
        replaced_with.append(fuzzy_pair['FuzzyKey'][x])
        
fuzzy_pair = fuzzy_pair[['NAME_ADDRESS', 'Replace_With']]
fuzzy_pair = fuzzy_pair.loc[~pd.isna(fuzzy_pair['Replace_With'])]

adj_claimed_fuzzy = adj_claimed_dat.merge(fuzzy_pair,'left')
adj_claimed_fuzzy['NAME_ADDRESS'] = np.where(pd.isna(adj_claimed_fuzzy['Replace_With']),
              adj_claimed_fuzzy['NAME_ADDRESS'], adj_claimed_fuzzy['Replace_With'])

adj_claimed_fuzzy_temp = adj_claimed_fuzzy.groupby(['NAME_ADDRESS', 'LastAssignedUser'],
                                              as_index = False).agg({'CLAIMID' : 'nunique',
                                                              'REPORTABLEAMOUNT' : 'sum'})