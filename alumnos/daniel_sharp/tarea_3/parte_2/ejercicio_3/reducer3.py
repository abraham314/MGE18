#!/usr/bin/env python3
import sys
import string

last_airline_id = None
cur_airline_name = "-"

for line in sys.stdin:
	line = line.strip()
	linea = line.split(',')
	if not last_airline_id or last_airline_id != linea[0]:
		last_airline_id = linea[0]
		cur_airline_name = linea[2]
	elif linea[0] == last_airline_id:
		linea[1] = cur_airline_name
		print(linea[1],sep='',end='')
		for i in range(2,len(linea)):
			print(',',linea[i],sep='',end='')
		print('')
