#!/usr/bin/python3

import sys
import time

airlines = {}

cont = 0
cont_flights = 0

for line in sys.stdin:
    lista = line.split(',')
    if cont > 0:
        if len(lista)==2:
            airlines[lista[0]] = lista[1]

        elif len(lista) > 2:
            if cont_flights > 0:
                lista.append(airlines[lista[4]])
                lista = list(map(lambda s: s.strip(), lista))
                print(*lista, sep='\t')
            else:
                cont_flights+=1
    cont+=1
