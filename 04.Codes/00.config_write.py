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

config['SQL_SERVER'] = {}
config['SQL_SERVER']['DRIVER'] = 'SQL Server'
config['SQL_SERVER']['SERVER'] = 'USHYDINDCH5\LOCALSERVER, 1434'
config['SQL_SERVER']['DATABASE'] = 'ClaimCenter'
config['SQL_SERVER']['User_ID'] = 'ccUser'
config['SQL_SERVER']['Password'] = '#1American'


with open('config_modern_american.ini', 'w') as configfile:
    config.write(configfile)