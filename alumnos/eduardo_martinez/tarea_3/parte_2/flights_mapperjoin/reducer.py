#!/usr/bin/python3
# @author: 2018 (c) Ariel Vallarino, Jorge Altamirano
# @description: JOIN entre archivos en mapper: airlines.csv, flights.csv
# @license: GPL v2
# airlines.csv: IATA_CODE	AIRLINE
# airports.csv: IATA_CODE	AIRPORT	CITY	STATE	COUNTRY	LATITUDE	LONGITUDE
# flights.csv: 	YEAR	MONTH	DAY	DAY_OF_WEEK	AIRLINE	FLIGHT_NUMBER	TAIL_NUMBER	ORIGIN_AIRPORT
#				DESTINATION_AIRPORT	SCHEDULED_DEPARTURE	DEPARTURE_TIME	DEPARTURE_DELAY	TAXI_OUT
#				WHEELS_OFF	SCHEDULED_TIME	ELAPSED_TIME	AIR_TIME	DISTANCE	WHEELS_ON	TAXI_IN
# 				SCHEDULED_ARRIVAL	ARRIVAL_TIME	ARRIVAL_DELAY	DIVERTED	CANCELLED	CANCELLATION_REASON
import sys

lines = sys.stdin.readlines()
lines.sort()

previous = None
name_prev = None
sum = 0

print('CODE\tAEROLINEA\tCANT.CANCELACIONES')

# Para cada aerolinea se cuenta cuantos vuelos canceladso tuvo
for line in lines:

        code, name, cancel = line.split('\t')
        if name != "Unknown_Airline" and name_prev == "Unknown_Airline":
                #name: Virgin America, cancel: -, code: VX, name_prev: Unknown_Airline
                name_prev = name
        #print("code: %s, name: %s, cancel: %s, code: %s, name_prev: %s"%(code, name, cancel.strip(), code, name_prev))
        if code != previous:
                if previous is not None and previous != "IATA_CODE":
                        print(previous + '\t' + name_prev + '\t' + str(sum))
                previous = code
                name_prev = name
                #print("Previous: %s, Name: %s, Valor: %s"%(name_prev,name,cancel.strip()))
                sum = 0

        valor = cancel.strip()
        if valor != "-":
                valor = int(valor)
        if valor == 1:
                sum += valor

# Retorna nombde de la aerolinea y cantidad de vuelos cancelados
if code != "IATA_CODE":
        print(code + '\t' + name_prev + '\t' + str(sum))

