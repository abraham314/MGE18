#!/usr/bin/python3

import sys

pref_dict ={}
flights_dict=[]

for line in sys.stdin:
    line = line.strip()
    fields = line.split('\t')

    id = int(fields[0])
    key = fields[4]
    value = fields[31]

    if id ==-1:
        pref_dict[key] = value

    else:
        flights_dict.append(fields)

for element in flights_dict:
    airline_name = pref_dict[element[4]]
    element[31] = airline_name
    print(*element, sep='\t')    
