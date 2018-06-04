#!/usr/bin/env python

import sys

previous = None
sum = 0
file= sys.stdin

for line in file:
	key, value = line.split(' ')
	if key != previous:
		if previous is not None:
			print(previous + ' ' + str(sum))
		previous = key
		sum = 0

	sum += int(value)

print(previous + ' ' + str(sum))
