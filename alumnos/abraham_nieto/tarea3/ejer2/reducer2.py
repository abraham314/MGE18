#!/usr/bin/python
import sys
import string

last_airline_id = None
cur_airline = "-"

for line in sys.stdin:
    line = line.strip()
    airline_id,resto = line.split("\t")
    airline=resto.split(',')[0]
    #resto=','.join(splits) 
    if not last_airline_id or last_airline_id != airline_id:
        last_airline_id = airline_id
        cur_airline = airline
    elif airline_id == last_airline_id:
        airline = cur_airline
        print (airline_id+','+airline+','+resto)
