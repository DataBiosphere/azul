text = []
with open('temp_err.txt','r') as infile:
	for line in infile:
		text.append(line)

with open('error.txt','a') as outfile:
	for line in text:
		outfile.write(line)