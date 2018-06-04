#!/usr/bin/env python

import sys

temp_list = []

for line in sys.stdin:

   line = line.strip()
   splits = line.split(",")

   if len(splits) > 30:
        airport_key = splits.pop(7) + '_2'
   else:
        airport_key = splits.pop(0) + '_1'
        
   value = ','.join(splits)
   temp_list.append(airport_key + '|' + value)



last_key_airline = None

for line in temp_list:

    line = line.strip()

    key_airline,value = line.split("|")
    key_airline,tag = key_airline.split("_")

    if not last_key_airline or last_key_airline != key_airline:
        if tag == '1':
            last_key_airline = key_airline
            airline = value
        if tag == '2':
            airline = ",,,,,"
            flight = value
            print(key_airline + ',' + airline + ',' + flight)
    elif key_airline == last_key_airline:
        if tag == '2':
            flight = value
            print(key_airline + ',' + airline + ',' + flight)