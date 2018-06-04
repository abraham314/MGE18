import sys

for line in sys.stdin:
	line = line.strip()
	splits = line.split(',')
	if len(splits) == 2:
		print(splits[0],',','0',',',splits[1],sep='')
	else:
		print(splits[4],',','NA',end='',sep='')
		for i in range(31):
			if i == 4:
				continue
			print(',',splits[i],sep='',end='')
		print('')