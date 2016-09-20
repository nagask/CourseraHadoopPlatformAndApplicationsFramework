#ADVANCED JOIN IN SPARK	
#The gennum files contain show names and their viewers, genchan files contain
#show names and their channel. We want to find out the total number of viewers
#across all shows for the channel BAT

#STEP 0 - OPEN TERMINAL

#STEP 1 - MOVE FILES FROM LOCAL MACHINE TO HDFS
hdfs dfs -mkdir input
hdfs dfs -put join2_* input
#to check all files are there
hdfs dfs -ls input/

#STEP 2 - RUN PYSPARK
PYSPARK_DRIVER_PYTHON =ipython pyspark

#STEP 3 - READ SHOW AND CHANNEL FILES
#The gennum files contain show names and number of viewers. You can
#read them into Spark with a pattern matching, see the ? which will match
#either A, B or C:
show_views_file = sc.textFile("input/join2_gennum?.txt")

#Remember you can check what Spark is doing by copying some elements of an RDD back to the driver:
show_views_file.take(2)

show_channel_file = sc.textFile("input/join2_genchan?.txt")

#STEP 4 - PARSE SHOW AND CHANNEL FILES
def split_show_views(line):
	#Given 'Hourly_Sports,21', return ('Hourly_Sports', 21)
	l=line.split(",")
	return (l[0], int(l[1]))

def split_show_channel(line):
	#Given "PostModern_News,BAT", return (PostModern_News, BAT)
	l=line.split(",")
	return (l[0], l[1])

#STEP 5 - USE MAPPER FUNCTIONS
show_views = show_views_file.map(split_show_views)
show_channel = show_channel_file.map(split_show_channel)

#STEP 6 - JOIN THE TWO DATASETS
joined_dataset = show_views.join(show_channel)

#STEP 7 - EXTRACT CHANNEL AS KEY
#You want to find the total viewers by
#channel, so you need to create an RDD with the channel as key and all the
#viewer counts, whichever is the show.
def extract_channel_views(show_views_channel): 
	#Given (PostModern_Cooking, (1038, MAN)), return  (MAN, 1038)
	views_channel=list(list(show_views_channel)[1])
	views=views_channel[0]
	channel=views_channel[1]
    return (channel, views)

#Now you can apply this function to the joined dataset to create an RDD of channel and views:
channel_views = joined_dataset.map(extract_channel_views)

#STEP 8 - SUM ACROSS ALL CHANNELS
#Finally, we need to sum all of the viewers for each channel:
#HOW DOES THIS EVEN MAKE SENSE!!!
def some_function(a, b):
	return a+b

channel_views.reduceByKey(some_function).collect()
