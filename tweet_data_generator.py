# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 20:02:18 2021

@author: Kunal
"""

from faker import Faker


fake= Faker()

def reg_users():
    
    return {
        'name':fake.name(),
        
        'tweet':fake.text()
            }

if __name__  == '__main__' :
    
    print(reg_users())
