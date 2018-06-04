import sys

for line in sys.stdin:
	line = line.strip()
	splits = line.split(',')
	if len(splits) == 7:
		print(splits[0],',','0',',',splits[1],',',splits[2],',',splits[3],',',splits[4],',',splits[5],',',splits[6],sep='')
	else:
		print(splits[7],',','NA,','NA,','NA,','NA,','NA,','NA',end='',sep='')
		for i in range(31):
			if i == 7:
				continue
			print(',',splits[i],sep='',end='')
		print('')