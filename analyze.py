with open('s3-concurrent-move.txt', 'r') as f:
	total_time = 0
	copy_operations = 0
	for l in f:
		if 'copy operations' in l:
			split = l.split(' ')
			copy_operations += int(split[0])
			minutes, seconds = split[4].split(':')
			minutes, seconds = int(minutes), int(seconds)
			total_time += (minutes * 60) + seconds

print(f'Total copies:\t{copy_operations:,}')
print(f'Total seconds:\t{total_time:,}')
print(f'Total minutes:\t{total_time / 60:0.2f}')
print(f'Total hours:\t{total_time / 60 / 60:0.2f}')
print(f'Total days:\t{total_time / 60 / 60 / 24:0.2f}')
