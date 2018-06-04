#!/usr/bin/python

import sys

airpid_dict ={}
flights_dict={}
for line in sys.stdin:
    line = line.strip()
    id, airportid,airport,city,state,country,latitud,resto= line.split('\t')

   


    if id =='-1':
        airpid_dict[airportid] = [airport,city,state,country,latitud,resto.split(',')[0]]
        #print(airid_dict)

    

    else:
        flights_dict[id] = [resto.split(',')[10],resto]

#print(airpid_dict[flights_dict['14747'][0]][0])

for id in flights_dict.keys():
    if id!='1':
       try:

           airport = airpid_dict[flights_dict[id][0]][0]
           city=airpid_dict[flights_dict[id][0]][1]
           state=airpid_dict[flights_dict[id][0]][2]
           country=airpid_dict[flights_dict[id][0]][3] 
           latitud=airpid_dict[flights_dict[id][0]][4]

           resto=flights_dict[id][1]


           print(airport,city,state,country,latitud+',',resto)
        
       except:
           pass

     
