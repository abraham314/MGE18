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
#                               AIR_SYSTEM_DELAY        SECURITY_DELAY  AIRLINE_DELAY   LATE_AIRCRAFT_DELAY     WEATHER_DELAY
import sys

airline = dict()
flights = list()

airlinestxt = open('airlines.csv', 'r')
for line in airlinestxt:
    line = line.strip() #hacer trim
    line2 = line.split(',') #separar
    if len(line2) == 2: #detectar airlines
        airline[line2[0]] = line2[1] #guardar en dict las aerolíneas
airlinestxt.close()

lines = sys.stdin.readlines()
lines.sort()
for line in lines:
    line = line.strip() #hacer trim nuevamente
    line2 = line.split(',') #separar nuevamente
    if len(line2) == 31 and line2[24] == "1": #detectar cancelled flights 
        flights.append([line2[4],airline[line2[4]]]) #hacer join sólo de vuelos cancelados

for flight in flights:
    print("%s\t%s"%(flight[0],flight[1])) #imprimir vuelos cancelados
