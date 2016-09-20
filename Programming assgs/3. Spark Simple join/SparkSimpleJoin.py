#SIMPLE JOIN IN SPARK
#In this programming assignment we will perform a join of 2 different
#wordcount datasets using pySpark

#STEP 0
#Open Terminal on Cloudera VM first!

#STEP 1
#Move files from local machine to HDFS 
hdfs dfs -mkdir input
hdfs dfs -put join1_FileA input
hdfs dfs -put join1_FileB input
#to check files have been moved...
hdfs dfs -ls input/
#to check content of file for say, join1_FileA.txt
hdfs dfs -cat join1_File.txt

#STEP 2
#Run following command to open pySpark console
PYSPARK_DRIVER_PYTHON=ipython pyspark
#Once in...

#STEP 3
#Wordcount dataset 1
fileA = sc.textFile("input/join1_FileA.txt") 
#to make sure file content is correct
#the collect() function brings back the results from HDFS to the driver
fileA.collect()

#Wordcount dataset 2
fileB = sc.textFile("input/join1_FileB.txt") 
#to make sure file content is correct
fileB.collect()

#STEP 4
#Mapper for FileA
#For example - you have "able,1991". You want ('able',1991), where 1991
#is of type int
def split_fileA(line):
    l=line.split(",")
    return (l[0],int(l[1]))

#Mapper for FileB
#For example - you have "Feb-02 about,3". You want ('about', 'Feb-02 3')
#want return (word, date + " " + count_string)...('able', 'Jan-01 5')
def split_fileB(line):
    parts=line.split(",")
    date_word=parts[0].split()
    date=date_word[0]
    word=date_word[1]
    count_string=parts[1]
    return (word, date + " " + count_string)

#STEP 5
#Run mapper functions on respective files
fileA_data = fileA.map(split_fileA)
fileB_data = fileB.map(split_fileB)

#STEP 6
#Run join
#Spark implements the join transformation that given a RDD of (K, V)
#pairs to be joined with another RDD of (K, W) pairs, returns a dataset
#that contains (K, (V, W)) pairs.
fileB_joined_fileA = fileB_data.join(fileA_data)

#to check result -
fileB_joined_fileA.collect()
