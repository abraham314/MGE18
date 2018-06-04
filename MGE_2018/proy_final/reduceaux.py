#!/usr/bin/python

import sys

lines = sys.stdin.readlines()
lines.sort()

previous = None
mes_prev = None
sum = 0



for line in lines:

        key, mes, val = line.split('\t')
        if mes != "-" and mes_prev == "-":
     
                mes_prev = mes
      
        if key != previous:
                if previous is not None:
                        print(previous + '\t' + mes_prev + '\t' + str(sum))
                previous = key
                mes_prev = mes
            
                sum = 0

        valor = val.strip()
        if valor != "-":
                valor = int(valor)
        if valor == 1:
                sum += valor


print(key + '\t' + mes_prev + '\t' + str(sum))
