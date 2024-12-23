#!/usr/bin/env python3

"""
Calculate the average precipiation for Östergötland for the period 1993-2016

1) Calculate the total monthly precipitation for each station
2) Calculate the monthly average (by averaging over stations)

Hint: dont use joins. If distributing, either repartition stations RDD to 1 partition and use collect() to get list and broadcast function to nodes.

Answer: Year, month, average precipitation

precipitation-readings.csv: 
(ID, Date, Time, Precipitation, Quality)
(99280; 2016-06-30; 21:00:00; 0.0; G)

stations-Ostergotland.csv:
(ID, Name, Measurement height, Lat, Long, Readings from, Readings to, Elevation)
(85270; Västerlösa; 2.0; 58.4447; 15.3772; 2002-05-01 00:00:00; 2011-02-28 23:59:59:59; 75.0)
"""

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")

# This path is to the file on hdfs
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
s_lines = station_file.map(lambda line: line.split(";"))
ostergotland = s_lines.map(lambda x: int(x[0])).collect() # selectout only station id and put into sequence

"""
(85270)

Condition on this -> only keep ID in other where this one is found (dont join)
"""

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
p_lines = precipitation_file.map(lambda line: line.split(";"))
precip = p_lines.map(lambda x: (x[0], x[1], x[3])) # map ID, Time and precipitation


"""
(99280, 2016-06-30, 0.0) 
"""
bc = sc.broadcast(ostergotland)


precip = precip.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016) # filter to years 1993-2016
precip = precip.filter(lambda x: int(x[0]) in bc.value)


"""
(99280, 2016-06-30, 0.0)
"""

precip = precip.map(lambda x: ((x[1],x[2]), int(x[0])))

"""
((2016-06-30, 0.0), 99280)
"""

total_prec = precip.map(lambda x: (  (x[1], x[0][0][0:4], x[0][0][5:7]), float(x[0][1]) )).reduceByKey(lambda x,y: x + y)

avg_prec = total_prec.map(lambda x: ((x[0][1], x[0][2]), x[1])).groupByKey()

avg_prec = avg_prec.mapValues(lambda x: sum(x) / len(x))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg_prec.saveAsTextFile("BDA/output")
