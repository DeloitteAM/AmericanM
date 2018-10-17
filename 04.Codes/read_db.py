# -*- coding: utf-8 -*-
"""
Created on Thu Oct 11 11:17:49 2018

@author: dmiglani
"""
query = 'select * from cctl_lobcode'
cctl_lobcode = pd.read_sql(query,con = conn)
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

query = 'select * from cc_user'
cc_user = pd.read_sql(query,con = conn)

query = 'select * from cc_authorityprofile'
cc_authorityprofile = pd.read_sql(query,con = conn)
query = 'select * from cc_authoritylimit'
cc_authoritylimit = pd.read_sql(query,con = conn)
query = 'select * from cctl_authoritylimittype'
cctl_authoritylimittype = pd.read_sql(query,con = conn)
query = 'select * from cctl_userexperiencetype'
cctl_userexperiencetype = pd.read_sql(query,con = conn)

query = 'select * from cc_checkpayee'
cc_checkpayee = pd.read_sql(query,con = conn)

query = 'select * from cc_claimIndicator'
cc_claimIndicator = pd.read_sql(query,con = conn)

query = 'select * from cc_userregion'
cc_userregion = pd.read_sql(query,con = conn)


query = 'select * from cc_region'
cc_region = pd.read_sql(query,con = conn)

query = 'select * from cc_region_zone'
cc_region_zone = pd.read_sql(query,con = conn)

 

#cc_claim = pd.read_csv(intermediate_dir_path + "cc_claim.csv")
#cc_Incident = pd.read_csv(intermediate_dir_path + "cc_Incident.csv")
#cc_transaction = pd.read_csv(intermediate_dir_path + "cc_transaction.csv")
#cc_activity = pd.read_csv(intermediate_dir_path + "cc_activity.csv")
#cc_policy = pd.read_csv(intermediate_dir_path + "cc_policy.csv")
#cctl_incident = pd.read_csv(intermediate_dir_path + "cctl_incident.csv")
#cc_check = pd.read_csv(intermediate_dir_path + "cc_check.csv")
#cc_address = pd.read_csv(intermediate_dir_path + "cc_address.csv")
#cc_contact = pd.read_csv(intermediate_dir_path + "cc_contact.csv")
#cc_exposure = pd.read_csv(intermediate_dir_path + "cc_exposure.csv")
#cctl_losscause = pd.read_csv(intermediate_dir_path + "cctl_losscause.csv")
#cc_reserveline = pd.read_csv(intermediate_dir_path + "cc_reserveline.csv")
#cc_claimcontact = pd.read_csv(intermediate_dir_path + "cc_claimcontact.csv")
#cc_user = pd.read_csv(intermediate_dir_path + "cc_user.csv")
#cc_authorityprofile = pd.read_csv(intermediate_dir_path + "cc_authorityprofile.csv")
#cc_authoritylimit = pd.read_csv(intermediate_dir_path + "cc_authoritylimit.csv")
#cctl_authoritylimittype = pd.read_csv(intermediate_dir_path + "cctl_authoritylimittype.csv")
#cctl_userexperiencetype = pd.read_csv(intermediate_dir_path + "cctl_userexperiencetype.csv")
#
#cc_transactionlineitem = pd.read_csv(intermediate_dir_path + "cc_transactionlineitem.csv")

###############################################################################

################ Temporary code for Backup ####################################

cc_claim2 = cc_claim.copy()
cc_Incident2 = cc_Incident.copy()
cc_transaction2 = cc_transaction.copy()
cc_activity2 = cc_activity.copy()
cctl_incident2 = cctl_incident.copy()
cc_address2 = cc_address.copy()
cc_policy2 = cc_policy.copy()
cc_check2 = cc_check.copy()
cc_exposure2 = cc_exposure.copy()
cc_contact2 = cc_contact.copy()
cc_claimcontact2 = cc_claimcontact.copy()
cctl_losscause2 = cctl_losscause.copy()
cc_transactionlineitem2 = cc_transactionlineitem.copy()
cc_reserveline2 = cc_reserveline.copy()
cc_user2 = cc_user.copy()
cc_authorityprofile2 = cc_authorityprofile.copy()
cc_authoritylimit2 = cc_authoritylimit.copy()
cctl_authoritylimittype2 = cctl_authoritylimittype.copy()
cctl_userexperiencetype2 = cctl_userexperiencetype.copy()
cc_checkpayee2 = cc_checkpayee.copy() 
cc_claimIndicator2 = cc_claimIndicator.copy()

#cc_claim = cc_claim2.copy()
#cc_Incident = cc_Incident2.copy()
#cc_transaction = cc_transaction2.copy()
#cc_activity = cc_activity2.copy()
#cctl_incident = cctl_incident2.copy()
#cc_address = cc_address2.copy()
#cc_policy = cc_policy2.copy()
#cc_check = cc_check2.copy()
#cc_exposure = cc_exposure2.copy()
#cc_contact = cc_contact2.copy()
#cc_claimcontact = cc_claimcontact2.copy()
#cctl_losscause = cctl_losscause2.copy()
#cc_transactionlineitem = cc_transactionlineitem2.copy()
#cc_reserveline = cc_reserveline2.copy()
#cc_user = cc_user2.copy()
#cc_authorityprofile = cc_authorityprofile2.copy()
#cc_authoritylimit = cc_authoritylimit2.copy()
#cctl_authoritylimittype = cctl_authoritylimittype2.copy()
#cctl_userexperiencetype = cctl_userexperiencetype2.copy()
#cc_checkpayee = cc_checkpayee2.copy() 
#cc_claimIndicator = cc_claimIndicator2.copy()


cctl_lobcode.to_csv(intermediate_dir_path + "cctl_lobcode.csv", 
                encoding='utf-8', index=False)
cc_claim.to_csv(intermediate_dir_path + "cc_claim.csv", 
                encoding='utf-8', index=False)
cc_Incident.to_csv(intermediate_dir_path + "cc_Incident.csv", 
                   encoding='utf-8', index=False)
cc_transaction.to_csv(intermediate_dir_path + "cc_transaction.csv", 
                      encoding='utf-8', index=False)
cc_activity.to_csv(intermediate_dir_path + "cc_activity.csv", 
                   encoding='utf-8', index=False)
cc_policy.to_csv(intermediate_dir_path + "cc_policy.csv", 
                 encoding='utf-8', index=False)
cctl_incident.to_csv(intermediate_dir_path + "cctl_incident.csv", 
                     encoding='utf-8', index=False)
cc_check.to_csv(intermediate_dir_path + "cc_check.csv", 
                encoding='utf-8', index=False)
cc_address.to_csv(intermediate_dir_path + "cc_address.csv", 
                  encoding='utf-8', index=False)
cc_contact.to_csv(intermediate_dir_path + "cc_contact.csv", 
                  encoding='utf-8', index=False)
cc_exposure.to_csv(intermediate_dir_path + "cc_exposure.csv", 
                   encoding='utf-8', index=False)
cctl_losscause.to_csv(intermediate_dir_path + "cctl_losscause.csv", 
                      encoding='utf-8', index=False)
cc_transactionlineitem.to_csv(intermediate_dir_path + "cc_transactionlineitem.csv", 
                              encoding='utf-8', index=False)
cc_reserveline.to_csv(intermediate_dir_path + "cc_reserveline.csv", 
                      encoding='utf-8', index=False)
cc_claimcontact.to_csv(intermediate_dir_path + "cc_claimcontact.csv", 
                       encoding='utf-8', index=False)
cc_user.to_csv(intermediate_dir_path + "cc_user.csv", 
               encoding='utf-8', index=False)
cc_authorityprofile.to_csv(intermediate_dir_path + "cc_authorityprofile.csv", 
                           encoding='utf-8', index=False)
cc_authoritylimit.to_csv(intermediate_dir_path + "cc_authoritylimit.csv", 
                         encoding='utf-8', index=False)
cctl_authoritylimittype.to_csv(intermediate_dir_path + "cctl_authoritylimittype.csv", 
                               encoding='utf-8', index=False)
cctl_userexperiencetype.to_csv(intermediate_dir_path + "cctl_userexperiencetype.csv", 
                               encoding='utf-8', index=False)
cc_checkpayee.to_csv(intermediate_dir_path + "cc_checkpayee.csv", 
                               encoding='utf-8', index=False)
cc_claimIndicator.to_csv(intermediate_dir_path + "cc_claimIndicator.csv", 
                               encoding='utf-8', index=False)







