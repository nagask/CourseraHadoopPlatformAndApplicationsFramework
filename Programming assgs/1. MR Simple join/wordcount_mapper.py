#Input: a line of text from stdin
#output: <word, 1> for each line

import sys

for line in sys.stdin:  
    line = line.strip()
    keys = line.split()
    for key in keys:
        value = 1        
        print('{0}\t{1}'.format(key, value))

