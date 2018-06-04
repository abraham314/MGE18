#!/usr/bin/env python

import sys

last_key_airline = None

for line in sys.stdin:
    line = line.strip()
    key_airline,value = line.split("|")
    key_airline,tag = key_airline.split("*")

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
