#!/usr/bin/python

import sys

for line in sys.stdin:
        val = 0
        mes = "-"

        line = line.strip()
        splits = line.split(",")
        if len(splits) == 2:
                
                key = splits[0]        
                mes = splits[1]        
                print( '%s\t%s\t%s' % (key, mes,val))
        else:
               
                key = splits[15]       
                val = 1    
                if val == 1:
                        print( '%s\t%s\t%s' % (key, mes, val))
