#!/usr/bin/env python3
# @author: 2018 (c) Ariel Vallarino, Jorge Altamirano
# @description: JOIN entre archivos en mapper: airlines.csv, flights.csv
# @license: GPL v2
# airlines.csv: IATA_CODE       AIRLINE
# airports.csv: IATA_CODE       AIRPORT CITY    STATE   COUNTRY LATITUDE        LONGITUDE
# flights.csv:  YEAR    MONTH   DAY     DAY_OF_WEEK     AIRLINE FLIGHT_NUMBER   TAIL_NUMBER     ORIGIN_AIRPORT
#                               DESTINATION_AIRPORT     SCHEDULED_DEPARTURE     DEPARTURE_TIME  DEPARTURE_DELAY TAXI_OUT
#                               WHEELS_OFF      SCHEDULED_TIME  ELAPSED_TIME    AIR_TIME        DISTANCE        WHEELS_ON       TAXI_IN
#                               SCHEDULED_ARRIVAL       ARRIVAL_TIME    ARRIVAL_DELAY   DIVERTED        CANCELLED       CANCELLATION_REASON
import sys

lines = sys.stdin.readlines()
lines.sort()

prev = prevName = None
sum = 1

print('CODE\tAEROLINEA\tCANT.CANCELACIONES')

# Para cada aerolinea se cuenta cuantos vuelos cancelados tuvo
for line in lines:
    line = line.strip()
    code, name = line.split('\t')
    if code == prev:
        sum += 1
    elif prev is not None:
        print("%s\t%s\t%d"%(prev, prevName, sum))
        sum = 1
    prev = code
    prevName = name
