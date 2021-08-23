#!/usr/bin/python3

import os
import time
import json
import sys
import getopt
from pymongo import MongoClient
from faker import Faker
from random import randint
import pathlib



#mongo_url = "mongodb+srv://main_user:muser@cvs-dest-cluster.nyx9j.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
mongo_url = "mongodb+srv://main_user:muser@democluster.c1xrj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
db = "claims2"
collection = "cvsaudit"
fake = Faker()


class TestConnection:
    def __init__(self):
        self.client = MongoClient(mongo_url)
        self.collection = self.client[db][collection]
        self.inputfile = ""
        self.outputfile = ""
        self.fakedata = ""
        print(f'connection initiated connected\n')

    def main_v(self, argv):
        inputfile = ''
        outputfile = ''
        try:
            opts, args = getopt.getopt(
                argv, "hi:o:n:", ["ifile=", "ofile=", "onumber="])
        except getopt.GetoptError:
            print('test.py -i <inputfile> -o <outputfile>')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print('test.py -i <inputfile> -o <outputfile>')
                sys.exit()
            elif opt in ("-i", "--ifile"):
                c_path = pathlib.Path().resolve()
                self.inputfile = f"{c_path}/{arg}"
            elif opt in ("-o", "--ofile"):
                self.outputfile = f"{c_path}/{arg}"
            elif opt in ("-n", "--onumber"):
                self.docs = arg
        print('Input file is : = ', {self.inputfile})
        print('Output file is : = ', {self.outputfile})


    def insert(self, **kwargs):
        #print(f'record inserting : {kwargs["r"]} \n')
        self.collection.insert_one(kwargs["r"])

    def find(self, **kwargs):
        self.collection = self.client[db][collection]
        return self.collection.find_one(kwargs)

    def findQ(self, collxn, **kwargs):
        self.collection = self.client[db][collxn]
        records = self.collection.find(kwargs["q"], kwargs["p"]).limit(400)
        return records

    def fakedatagen(self, **kwargs):
        cmdline = f"mgeneratejs -n 5 {self.inputfile} > {self.outputfile} "
        #cmdline = f"mgeneratejs -n 5 {self.inputfile} "
        print(cmdline)
        os.system(cmdline)

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
            student_data['student_id'] = randint(1, 10000)
            student_data['name'] = fake.name()
            student_data['address'] = fake.address()
            student_data['latitude'] = str(fake.latitude())
            student_data['longitude'] = str(fake.longitude())
            student_data['text'] = str(strx)
            students.append(student_data)
        return students


if __name__ == '__main__':

    client = TestConnection()
    client.main_v(sys.argv[1:])
    client.fakedatagen()

    ##iter_count = 1000
    ##json_array = client.input_data2(iter_count)
    ##tt=0
    ##for x2 in json_array:
    ##    t1 = time.perf_counter()
    ##    vx = client.insert(r=x2)
    ##    tx = time.perf_counter()
    ##    tt = tt + (tx-t1)*1000
    
    iter_count = 5
    tt = 0
    file1 = open(client.outputfile, 'r')
    Lines = file1.readlines()

    count = 0
    # Strips the newline character
    for line in Lines:
        t1 = time.perf_counter()
        x2 = json.loads(line)
        vx = client.insert(r=x2)
        tx = time.perf_counter()
        tt = tt + (tx-t1)*1000

    print(f'****** EACH INSERT total time (ms) *********{tt/iter_count}')
    print(f'****** End of Script - total time (ms) *********{tt}')
