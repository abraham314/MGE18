#!/usr/bin/python

import sys

i=0 
for line in sys.stdin:
    line = line.strip()
    line = line.split(",")

    id = -1
    
    airport_id = "-1"
    airport = "-1"
    city='-1'
    state='-1'
    country="-1"
    latitud="-1"
    longitud="-1"
    



    if len(line)>7:
        i=i+1 
        id = i
        
        airport_id= line[9]
        vals= ','.join(line)
        

    else:
        airport_id = line[0]
        airport= line[1]
        city=line[2]
        state=line[3]
        country=line[4]
        latitud=line[5]
        longitud=line[6]


    print ('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (id,airport_id, airport,city,state,country,latitud,longitud+','+vals))

#cat /home/abraham/MGE_2018/tarea3/ejer3/outaux3.csv /home/abraham/MGE_2018/tarea3/airports.csv | python /home/abraham/MGE?2018/tarea3/ejer3/mapper3_1.py | python /home/abraham/MGE?2018/tarea3/ejer3/reducer3_1.py
#cat /home/abraham/MGE_2018/tarea3/flights.csv | python3 /home/abraham/MGE_2018/tarea3/airlines.csv /home/abraham/MGE_2018/tarea3/airports.csv 
