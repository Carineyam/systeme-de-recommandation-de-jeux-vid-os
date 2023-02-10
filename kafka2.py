#!/usr/bin/python3                                                                                                      
                                                                                                                        
from kafka import  KafkaConsumer
import pydoop.hdfs as hdfs
import subprocess
import sys
import json
from json import loads

### Setting up the Python consumer
bootstrap_servers = ['localhost:9092']
topicName = 'users_reviews'
consumer = KafkaConsumer (topicName, group_id = 'my_group_id1',bootstrap_servers = bootstrap_servers,
    auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))  ## You can also set it as latest

hdfs.mkdir('hdfs://localhost:9000/steam')
file='/home/carine/users_reviews_fixed.json'
args_list=['hdfs','dfs','-put',file,'/steam']    
print('running system command:{}'.format(' '.join(args_list)))
proc=subprocess.Popen(args_list,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
proc.communicate()
hdfs_path='hdfs://localhost:9000/steam/users_reviews_fixed.json'
