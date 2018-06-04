#!/usr/bin/python

import sys

previous = None
prev_an=None 

sum = 0

for line in sys.stdin:
  key, value = line.split('\t')  
  if (key != previous):
      if previous is not None:
        if key.split(',')[0]!=prev_an:
           print(prev_an + '\t' + str(sum)) 
           sum=1 
        else:
           sum+=1 
      previous = key
      prev_an=key.split(',')[0] 

      #sum = 1


  sum += 0

print(prev_an + '\t' + str(sum))
