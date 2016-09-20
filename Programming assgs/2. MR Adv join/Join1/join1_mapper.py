import sys

# --------------------------------------------------------------------------
#This mapper code will input a <date word, value> input file, and move date into 
#the value field for output
# --------------------------------------------------------------------------

for line in sys.stdin:
    line = line.strip()   		
    key_value = line.split(",")                 #program assumes input file has ',' separating key value
    key_in = key_value[0].split()	
    value_in = key_value[1]			

    #print key_in
    if len(key_in)>=2:                          #if entry has <date word> in key
        date = key_in[0]      
        word = key_in[1]
        value_out = date+" "+value_in     
        print('%s\t%s' % (word, value_out))     #Hadoop expects a tab to separate key value
    else:   #key is only <word> 
        print('%s\t%s' % (key_in[0], value_in))

