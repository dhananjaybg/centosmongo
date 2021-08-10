#!/usr/bin/python3

import os
import time
import json
from pymongo import MongoClient
from faker import Faker
from random import randint


mongo_url = "mongodb+srv://main_user:muser@cvs-dest-cluster.nyx9j.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
db = "claims"
collection = "cvsaudit"
fake = Faker()


class TestConnection:
    def __init__(self):
        self.client = MongoClient(mongo_url)
        self.collection = self.client[db][collection]
        print(f'connection initiated connected\n')

    def insert(self, **kwargs):
        self.collection.insert_one(kwargs["r"])
        #print(f'record inserted: {kwargs} \n')

    def find(self, **kwargs):
        self.collection = self.client[db][collection]
        return self.collection.find_one(kwargs)

    def findQ(self, collxn, **kwargs):
        self.collection = self.client[db][collxn]
        records = self.collection.find(kwargs["q"], kwargs["p"]).limit(400)
        return records


    def input_data(self, x):
            student_data = {}
            for i in range(0, x):
                student_data[i] = {}
                student_data[i]['student_id'] = randint(1, 100)
                student_data[i]['name'] = fake.name()
                student_data[i]['address'] = fake.address()
                student_data[i]['latitude'] = str(fake.latitude())
                student_data[i]['longitude'] = str(fake.longitude())
            return student_data

    def input_data2(self, x):
        students = []

        strx = ""

        for j in range(14):
            strx = strx + fake.text()

        for i in range(0, x):
            student_data = {}
            student_data['student_id'] = randint(1, 100)
            student_data['name'] = fake.name()
            student_data['address'] = fake.address()
            student_data['latitude'] = str(fake.latitude())
            student_data['longitude'] = str(fake.longitude())
            student_data['text'] = str(strx)
            students.append(student_data)
        return students


if __name__ == '__main__':
    client = TestConnection()

    iter_count = 100
    json_array = client.input_data2(iter_count)

    t1 = time.perf_counter()
    for x2 in json_array:
        vx = client.insert(r=x2)
    tx = time.perf_counter()
    tt = (tx-t1)*1000

    print(f'****** EACH INSERT total time (ms) *********{tt/iter_count}')
    print(f'****** End of Script - total time (ms) *********{tt}')
