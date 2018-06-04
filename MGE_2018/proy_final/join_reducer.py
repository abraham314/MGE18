#!/usr/bin/env python


import sys

last_key_mes = None
#val=0
#sum = 0
#previous = None

for line in sys.stdin:
    line = line.strip()
    key_mes,value = line.split("|")
    key_mes,tag = key_mes.split("00")

    if not last_key_mes or last_key_mes != key_mes:
        if tag == '1':
            last_key_mes = key_mes
            mes = value
            #val=0
        if tag == '2':
            mes = value
            #val = 0
            print(key_mes+ ',' + mes)
            

    elif key_mes == last_key_mes:
        if tag == '2':
            #val = 1
            print(key_mes+ ',' + mes)


    #if mes!=previous:
     #  if previous is not None:
     #     print(previous + '\t' + str(sum))
     #  previous = mes 
     #  sum=0
    #sum+=int(val)

