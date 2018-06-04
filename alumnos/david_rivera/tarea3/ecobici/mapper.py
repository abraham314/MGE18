#!/usr/bin/python3

import sys
import re

count = 0

for word in sys.stdin:
    argumento = word.split(',')[13]
    if count > 0:
        print(argumento,"\t",1)
    count = count + 1


