#!/usr/bin/env python3

from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext


sc = SparkContext(appName = "lab_kernel")

def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a))
	km = 6367 * c
	return km
	
#h_distance = # Up to you
#h_date = # Up to you
#h_time = # Up to you
a = 62.2857
b = 15.3735
date = "2013-06-24"

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

# Your code here
stations_lines = stations.map(lambda line: line.split(";"))

stations = stations_lines.map(lambda x: (x[0], (float(x[3]), float(x[4]))))

# stations is smaller than temps, broadcast it to nodes and return kv pairs in RDD as dictionary
stations_bc = sc.broadcast(stations.collectAsMap())

temps_lines = temps.map(lambda line: line.split(";"))
temps = temps_lines.map(lambda x: (x[0],( x[1], x[1][0:4], x[1][5:7], x[1][8:10], x[2], stations_bc.value[x[0]][0],stations_bc.value[x[0]][1], float(x[3]))))


"""
('133250', ('2007-08-13', '2007', '08', '13', '04:00:00', 63.37375, 13.16067, 13.4))

"""

# filter out data that is after the chosen date
temps = temps.filter(lambda x: datetime(x[2],x[3],x[4]) <= datetime(date[0:4], date[5:7], date[8:10]))



test = temps
test.saveAsTextFile("BDA/output")
