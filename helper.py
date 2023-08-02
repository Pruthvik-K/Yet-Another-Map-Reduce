
def splitByLine(path, splitSize):
	file = open(path, 'r')
	
	flag = True
	while flag:
		t = ""
		out = ""
		for i in range(splitSize):
			t=file.readline().strip()
			if not t:
				yield out
				flag=False
				break
			out+=t+'\n'
		if flag:
			yield out

		

def hashFunc(s):
	val = 0
	n = len(s)
	
	for i in range(n):
		val = (val + ord(s[i])) % 26

	return val
