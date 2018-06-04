#!/usr/bin/python
import sys
import string

last_airport_id = None
cur_airport = "-"
cur_city="-" 
cur_state="-" 
cur_country="-" 
cur_latitud="-" 
cur_longitud="-" 

for line in sys.stdin:
    line = line.strip()
    airport_id,resto = line.split("\t")
    airport=resto.split(',')[0]
    city=resto.split(',')[1] 
    state=resto.split(',')[2]
    country=resto.split(',')[3] 
    latitud=resto.split(',')[4]
    longitud=resto.split(',')[5] 
    #resto=','.join(splits) 
    if not last_airport_id or last_airport_id != airport_id:
        last_airport_id = airport_id
        cur_airport = airport
        cur_city=city 
        cur_state=state 
        cur_country=country 
        cur_latitud=latitud 
        cur_longitud=longitud 
    elif airport_id == last_airport_id:
        airport = cur_airport
        city=cur_city 
        state=cur_state 
        country=cur_country 
        latitud=cur_latitud 
        longitud=cur_longitud 
        print (airport_id+','+airport+','+city+','+state+','+country+','+latitud+','+longitud+','+resto)
#cat /home/abraham/MGE_2018/tarea3/ejer2/outaux.csv /home/abraham/MGE_2018/tarea3/airports.csv | python /home/abraham/MGE?2018/tarea3/ejer2/mapper2_1.py | sort --version-sort  | python /home/abraham/MGE?2018/tarea3/ejer2/reducer2_1.py

