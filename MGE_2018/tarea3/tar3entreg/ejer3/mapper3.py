
#!/usr/bin/python

import sys

i=0 
for line in sys.stdin:
    line = line.strip()
    line = line.split(",")

    id = -1
    
    airline_id = "-1"
    airline = "-1"
    



    if len(line)>2:
        i=i+1 
        id = i
        
        airline_id= line[4]
        vals= ','.join(line)
        

    else:
        airline_id = line[0]
        airline= line[1]


    print ('%s\t%s\t%s' % (id,airline_id, airline+','+vals))

#cat /home/abraham/MGE_2018/tarea3/flights.csv /home/abraham/MGE_2018/tarea3/airlines.csv | python /home/abraham/MGE?2018/tarea3/ejer3/mapper3.py | python /home/abraham/MGE?2018/tarea3/ejer3/reducer3.py
#cat /home/abraham/MGE_2018/tarea3/flights.csv | python3 /home/abraham/MGE_2018/tarea3/airlines.csv /home/abraham/MGE_2018/tarea3/airports.csv 
