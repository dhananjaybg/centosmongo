#!/usr/bin/python3

import os
import time
from pymongo import MongoClient


mongo_url = "mongodb+srv://main_user:muser@cvs-dest-cluster-pri.nyx9j.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
db = "claims"
collection = "cvsaudit"


class TestConnection:
    def __init__(self):
        self.client = MongoClient(mongo_url)
        self.collection = self.client[db][collection]
        print(f"'connection initiated. \n'")

    def insert(self, **kwargs):
        self.collection.insert_one(kwargs["r"])
        #print(f'record inserted: {kwargs} \n')

    def find(self, **kwargs):
        self.collection = self.client[db][collection]
        return self.collection.find_one(kwargs)

    def findQ(self,collxn ,**kwargs):
        self.collection = self.client[db][collxn]
        records = self.collection.find(kwargs["q"], kwargs["p"]).limit(400)
        return records

if __name__ == '__main__':
    client = TestConnection()
    t1 = time.perf_counter()
    iter_count = 1000
    vx = client.insert(r=record)
    
tx = time.perf_counter()
tt = (tx-t1)*1000

print(f"'****** EACH INSERT total time (ms) *********{tt/iter_count}")
print(f"'****** End of Script - total time (ms) *********{tt}")

