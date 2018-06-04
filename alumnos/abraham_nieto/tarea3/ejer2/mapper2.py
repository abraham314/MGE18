#!/usr/bin/python

import sys
for line in sys.stdin:
    
	airline_id = ""
	
	airline = "-"

	line = line.strip()
	splits = line.split(",")
	if len(splits)>2: # flights

                airline_id = splits[4] 
                vals= ','.join(splits)
                  
		
	else:
		airline_id = splits[0] 
		airline = splits[1]    
               
	print ('%s\t%s' % (airline_id,airline+','+vals))
