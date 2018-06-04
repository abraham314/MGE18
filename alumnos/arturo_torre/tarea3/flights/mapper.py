#!/usr/bin/python3

import sys
import time

cont = 0
cont1 = 0

for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    
    datos = [-1]*32
    
    if len(line) > 2:
        if cont > 0:
            datos = line
            datos.append(-1)
        else:
            cont +=1

    else:
        if cont1 > 0:
            datos[4] = line[0]
            datos[31] = line[1]
        else:
            cont1 +=1
    
    print(*datos, sep='\t')
