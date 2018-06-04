#!/usr/bin/python

import sys
for line in sys.stdin:
    
	airport_id = ""
	
	airport = "-"

	line = line.strip()
	splits = line.split(",")
	if len(splits)>7: # flights+airlines

                airport_id = splits[10] 
                val= ','.join(splits)
                  
		
	else:
                airport_id = splits[0] 
                airport=splits[1]
                val = ','.join(splits)    
               
	print ('%s\t%s' % (airport_id,airport+','+val))


#cat /home/abraham/MGE_2018/tarea3/flights.csv /home/abraham/MGE_2018/tarea3/airlines.csv | python /home/abraham/MGE?2018/tarea3/mapper2.py | sort -r | python /home/abraham/MGE?2018/tarea3/reducer2.py>/home/abraham/MGE_2018/tarea3/ejer2/outaux.csv


