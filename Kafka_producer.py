# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 19:57:58 2021

@author: Kunal
"""

from kafka import KafkaProducer
import json
import time
from data import reg_users

def jason_serializer(data):
    return json.dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer =jason_serializer
                         )

if __name__ == '__main__':
    
    while True:
        
        reg_user = reg_users()
        #print(reg_user)
        producer.send('tweet1', reg_user)
        time.sleep(3)