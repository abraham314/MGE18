#!/usr/bin/python3
# @author: 2018 (c) Ariel Vallarino
# @description: ecobici mapper file
# @license: GPL v2
import sys
import re
 
for line in sys.stdin:
	words = re.split(',', line)
	print(words[13] + "\t1")
