# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 13:02:44 2018

@author: dmiglani
"""

def column_preprocess(dat,cols):
    for i in cols:
        dat[i] = dat[i].astype(str)
        dat[i] = dat[i].str.upper()
        dat[i] = dat[i].str.replace('[^A-Za-z]+\s', '')
        dat[i] = dat[i].str.replace('.', '')
        
    return(dat)
