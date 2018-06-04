#!/usr/bin/env python3
import sys
import string

last_airport_id = None
cur_1 = "-"
cur_2 = "-"
cur_3 = "-"
cur_4 = "-"
cur_5 = "-"
cur_6 = "-"

for line in sys.stdin:
	line = line.strip()
	linea = line.split(',')
	if not last_airport_id or last_airport_id != linea[0]:
		last_airport_id = linea[0]
		cur_1 = linea[2]
		cur_2 = linea[3]
		cur_3 = linea[4]
		cur_4 = linea[5]
		cur_5 = linea[6]
		cur_6 = linea[7]
	elif linea[0] == last_airport_id:
		linea[1] = cur_1
		linea[2] = cur_2
		linea[3] = cur_3
		linea[4] = cur_4
		linea[5] = cur_5
		linea[6] = cur_6
		print(linea[1],sep='',end='')
		for i in range(2,len(linea)):
			print(',',linea[i],sep='',end='')
		print('')
