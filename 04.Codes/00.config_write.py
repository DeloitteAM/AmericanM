# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 14:16:06 2018

@author: dmiglani
"""
%reset -f

import os
cwd = os.getcwd()
os.chdir('C:\\Users\\dmiglani\\Desktop\\ModernAmerican')

import configparser

config = configparser.ConfigParser()
#config['DEFAULT'] = {'ServerAliveInterval': '45'}

config['PATH'] = {}
config['PATH']['Project Directory'] = 'C:/Users/dmiglani/Desktop/ModernAmerican'

#config['SQL_SERVER'] = {}
#config['SQL_SERVER']['DRIVER'] = 'SQL Server'
#config['SQL_SERVER']['SERVER'] = 'USHYDINDCH5\LOCALSERVER, 1434'
#config['SQL_SERVER']['DATABASE'] = 'ClaimCenter'
#config['SQL_SERVER']['User_ID'] = 'ccUser'
#config['SQL_SERVER']['Password'] = '#1American'


config['Oracle_Connect'] = {}
config['Oracle_Connect']['Host'] = '10.14.230.44'
config['Oracle_Connect']['Port'] = '1521'
config['Oracle_Connect']['Database'] = 'ClaimCenterDatabase'
config['Oracle_Connect']['User_ID'] = 'CCUSER'
config['Oracle_Connect']['Password'] = 'Oracle123'


with open('config_modern_am_oracle.ini', 'w') as configfile:
    config.write(configfile)