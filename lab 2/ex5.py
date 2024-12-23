#!/usr/bin/env python3

"""
precipitation-readings.csv: 
(ID, Date, Time, Precipitation, Quality)
(99280; 2016-06-30; 21:00:00; 0.0; G)

stations-Ostergotland.csv:
(ID, Name, Measurement height, Lat, Long, Readings from, Readings to, Elevation)
(85270; Västerlösa; 2.0; 58.4447; 15.3772; 2002-05-01 00:00:00; 2011-02-28 23:59:59:59; 75.0)
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
# read station data and split
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
s_lines = station_file.map(lambda line: line.split(";"))
s_table = s_lines.map(lambda p: Row(stationID = p[0]))  # select id

stationsDf = sqlContext.createDataFrame(s_table)
stationsDf.registerTempTable("s_table")


# reading precipitation data
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
p_lines = precipitation_file.map(lambda line: line.split(";"))
p_table = p_lines.map(lambda p: Row(station = p[0], year = p[1].split("-")[0], month = p[1].split("-")[1], day = p[1].split("-")[2], precip = float(p[3])))


precipDf = sqlContext.createDataFrame(p_table)
precipDf.registerTempTable("p_table")

precip = stationsDf.join(precipDf, stationsDf["stationID"] == precipDf["station"], 'inner')

"""
Station ID, YYYY, MM, DD, Precip
"""


# filter to years 1993-2016
precip = precip.where((precip['year'] >= 1993) & (precip["year"] <= 2016))

tot_prec = precip.groupBy('year', 'month', 'station').agg(F.sum('precip').alias('totMonthlyPrecipitation'))

avg_prec = tot_prec.groupBy('year', 'month').agg(F.avg('totMonthlyPrecipitation').alias('avgMonthlyPrecipitation'))

avg_prec = avg_prec.orderBy(["year", "month"], ascending=[0, 0])

# Save the result
avg_prec.rdd.saveAsTextFile("BDA/output")

