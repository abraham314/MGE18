#!/usr/bin/python

import sys

 
for line in sys.stdin:

	print(line.split(',')[0][6:10]+","+line.split(',')[1]+"\t1")
