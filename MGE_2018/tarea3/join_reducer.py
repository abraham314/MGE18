#!/usr/bin/env python


import sys

last_key_airport = None

for line in sys.stdin:
    line = line.strip()
    key_airport,value = line.split("|")
    key_airport,tag = key_airport.split("*")

    if not last_key_airport or last_key_airport != key_airport:
        if tag == '1':
            last_key_airport = key_airport
            airport = value
        if tag == '2':
            airport = ",,,,,"
            flight = value
            print(key_airport + ',' + airport + ',' + flight)

    elif key_airport == last_key_airport:
        if tag == '2':
            flight = value
            print(key_airport + ',' + airport + ',' + flight)
