#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 2")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year-month),(temperature)
temperature = lines.map(lambda x: ((x[1][0:7]), (float(x[3]))))

#filter years
temperature = temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)

# filter temp
temperature = temperature.filter(lambda x: float(x[1])>10)

temperature = temperature.map(lambda x: (x[0],1)) # adding a 1 in the tuple to count
                              
# counter 
count_1 = temperature.reduceByKey(lambda a,b: a+b)

count_1.saveAsTextFile("BDA/output/ex2first")


# SECOND PART 
# (key, value) = (year-month,station),(temperature)
temperature2 = lines.map(lambda x: ((x[1][0:7]+x[0]), (float(x[3]))))

#filter years
temperature2 = temperature2.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)

# filter temp
temperature2 = temperature2.filter(lambda x: float(x[1])>10)

# mapping to add a 1 for our counter
temperature2 = temperature2.map(lambda x:(x[0],int(1)))

# reducing so we get unique keys with station
count_2 =  temperature2.reduceByKey(lambda a,b: a)

# mapping to remove stations
count_2  = count_2.map(lambda x:(x[0][0:7],x[1]))

# now counting the values
count_2 =  count_2.reduceByKey(lambda a,b: a+b)

count_2.saveAsTextFile("BDA/output/ex2second")

