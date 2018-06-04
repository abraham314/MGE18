#!/usr/bin/python3
# @author: 2018 (c) Ariel Vallarino, Jorge Altamirano
# @description: JOIN entre archivos:  airlines.csv  y  flights.csv
# @license: GPL v2
# airlines.csv: IATA_CODE       AIRLINE
# airports.csv: IATA_CODE       AIRPORT CITY    STATE   COUNTRY LATITUDE        LONGITUDE
# flights.csv:  YEAR    MONTH   DAY     DAY_OF_WEEK     AIRLINE FLIGHT_NUMBER   TAIL_NUMBER   ORIGIN_AIRPORT
#                               DESTINATION_AIRPORT     SCHEDULED_DEPARTURE     DEPARTURE_TIME        DEPARTURE_DELAY TAXI_OUT
#                               WHEELS_OFF      SCHEDULED_TIME  ELAPSED_TIME    AIR_TIME      DISTANCE        WHEELS_ON       TAXI_IN
#                               SCHEDULED_ARRIVAL       ARRIVAL_TIME    ARRIVAL_DELAYDIVERTED CANCELLED       CANCELLATION_REASON
#                               AIR_SYSTEM_DELAY        SECURITY_DELAY  AIRLINE_DELAYLATE_AIRCRAFT_DELAY      WEATHER_DELAY
import sys

lines = sys.stdin.readlines()
lines.sort()

previous = None
name_prev = None
sum = 0

print('IATA_CODE\tAIRLINE\tCANCEL_COUNT')

for line in lines:
    line = line.strip()
    line2 = line.split('\u00AC')
    #print("%s (%d)"% (line, len(line2)))
    if len(line2) == 1:
        sum += 1
    elif len(line2) == 2 and line2[0] != "IATA_CODE":
        print("%s\t%s\t%d"% (line2[0], line2[1], sum))
        sum = 0
