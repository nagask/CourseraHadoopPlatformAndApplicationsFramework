# ---------------------------------------------------------------
# Input: <word, 1>
# Output: <word, total-count>
# ---------------------------------------------------------------

import sys

last_key = None
running_total = 0

for input_line in sys.stdin:
    input_line = input_line.strip()
    this_key, value = input_line.split("\t", 1)
    value = int(value)
    
    #Key Check part
    if last_key == this_key:     
        running_total += value   
    else:
        #if this key that was just read in is different, and the previous (ie last) key is not empy, then output the previous <key running-count>
        if last_key:             
            print( "{0}\t{1}".format(last_key, running_total))
        #reset values
        running_total = value
        last_key = this_key

if last_key == this_key:
    print( "{0}\t{1}".format(last_key, running_total)) 
	
