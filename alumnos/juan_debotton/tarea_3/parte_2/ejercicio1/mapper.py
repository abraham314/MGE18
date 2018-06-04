#!/usr/bin/env python

import sys
file = sys.stdin

for line in sys.stdin:
	print("".join(line.split()) + " 1") #mandamos a la salida la palabra separada por un tab y un 1 
