#!/usr/bin/python

import sys

airid_dict ={}
flights_dict={}
for line in sys.stdin:
    line = line.strip()
    id, airlineid, resto= line.split('\t')

   


    if id =='-1':
        airid_dict[airlineid] = resto.split(',')[0]
        #print(airid_dict)

    

    else:
        flights_dict[id] = [resto.split(',')[5],resto]

#print(airid_dict)

for id in flights_dict.keys():
    if id!='1':
       airline = airid_dict[flights_dict[id][0]]
       resto=flights_dict[id][1]


       print(airline+',',resto)

     
