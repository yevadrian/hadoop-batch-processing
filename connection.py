#!/usr/bin/python3

import os
import json
from hdfs import InsecureClient
from sqlalchemy import create_engine

def database():
    path = open(os.getcwd()+'/config.json')
    conf = json.load(path)['database']
    engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}/{conf['database']}")
    return engine

def warehouse():
    path = open(os.getcwd()+'/config.json')
    conf = json.load(path)['warehouse']
    engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}/{conf['database']}")
    return engine

def hadoop():
    path = open(os.getcwd()+'/config.json')
    conf = json.load(path)['hadoop']
    client = InsecureClient(conf['client'])
    return client