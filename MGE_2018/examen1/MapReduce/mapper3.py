#!/usr/bin/python

import sys

 
for line in sys.stdin:

    if line.split(',')[3]=="":
       miss=1
    else:
       miss=0

    print(line.split(',')[2]+","+line.split(',')[0][6:10]+"\t1"+"\t"+str(miss))
