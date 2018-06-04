#!/usr/bin/env python

import sys
import re
 
for line in sys.stdin:
	linea = re.split(',',line)
	print(linea[13] + "\t1")
