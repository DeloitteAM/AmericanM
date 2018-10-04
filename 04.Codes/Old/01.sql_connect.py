# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 12:46:02 2018

@author: dmiglani
"""

import pyodbc
import pandas as pd


################# SQL Connect #####################
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=USHYDINDCH5\LOCALSERVER, 1434; \
                      DATABASE=ClaimCenter;UID=ccUser;PWD=#1American')
  
sql3 = "

sql = "SELECT TOP (1000) [CreateUserID] \
      ,[PublicID] \
      ,[AccountHolderID] \
      ,[UpdateTime] \
      ,[AccountNumber] \
      ,[BeanVersion] \
      ,[CreateTime] \
      ,[Retired] \
      ,[ID] \
      ,[UpdateUserID] \
      FROM [ClaimCenter].[dbo].[cc_account]"
                      
data = pd.read_sql(sql,cnxn)

sql2 = "select ac.subject,ac.*,tr.* from cc_transaction tr inner join cc_claim c on c.id = tr.claimid \
inner join cc_activity ac on ac.ClaimID = c.id where tr.Subtype = 1 and c.ClaimNumber = '000-00-000115'"

data2 = pd.read_sql(sql2,cnxn)
                      
cnxn.close()

#import plotly.plotly as py
#import plotly.graph_objs as go
#plotly.tools.set_credentials_file(username='Miglani', api_key='PHGVblQidKqoPoJCYkQm')
#
#trace0 = go.Scatter(
#    x=[1, 2, 3, 4],
#    y=[10, 11, 12, 13],
#    mode='markers',
#    marker=dict(
#        size=[40, 60, 80, 100],
#    )
#)
#
#data = [trace0]
#py.iplot(data, filename='bubblechart-size')
#py.plot(data, filename='bubblechart-size')


################# Plotly ###############################
from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
from plotly.graph_objs import *
init_notebook_mode()

trace0 = Scatter(
    x=[1, 2, 3, 4],
    y=[10, 11, 12, 13],
    mode='markers',
    marker=dict(
        size=[40, 60, 80, 100],
    )
)
data = [trace0]
layout = Layout(
    showlegend=False,
    height=600,
    width=600,
)

fig = dict( data=data, layout=layout )

plot(fig)  

####################### Dummy #######################################

from collections import Counter
dummy = pd.read_csv('C:\\Users\\dmiglani\\Desktop\\ModernAmerican\\07.Temporary\\dummy.csv')

dummy_entries = dummy.groupby('ClaimID', \
                              as_index=False).agg({"Stage" : "count", "Amount Claimed": "max", \
                                            "Amount Passed" : "max"})

dummy_entries2 = dummy.groupby(['ClaimID', 'Authorizer'], \
                              as_index=False).agg({"Amount Claimed": "max", \
                                            "Amount Passed" : "max"})

dummy_entries2['perc_claim'] = dummy_entries2['Amount Passed']/dummy_entries2['Amount Claimed']

