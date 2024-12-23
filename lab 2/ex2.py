#!/usr/bin/env python3

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlCon = SQLContext(sc) # obtaining sql content from sc object

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))

# mapping the data we want
tempReads = lines.map(lambda p : Row(year= int(p[1][0:4]),month=int(p[1][5:7]),temp=float(p[3])))

# creating the data frame
schematemp = sqlCon.createDataFrame(tempReads)

schematemp.registerTempTable("yearmon_temps")

# filtering the years

schematemp = schematemp.filter((schematemp.year<=2014) & (schematemp.year >=1950))

# filter temp

schematemp = schematemp.filter((schematemp.temp>10))

# count year-months
count_first = schematemp.select('year','month').groupBy(['year','month']).count()

# sort by count and change name to value

count_first = count_first.orderBy(F.desc('count'))

count_first.rdd.saveAsTextFile("BDA/output/first")


# mapping the data we want
tempReads = lines.map(lambda p : Row(year= int(p[1][0:4]),month=int(p[1][5:7]),station=p[0],temp=float(p[3])))

# creating the data frame
schematemp = sqlCon.createDataFrame(tempReads)

schematemp.registerTempTable("yearmon_temps")

# filtering the years

schematemp = schematemp.filter((schematemp.year<=2014) & (schematemp.year >=1950))

# filter temp

schematemp = schematemp.filter((schematemp.temp>10))

# pickoing ot unique values for year month and station
count_sec = schematemp.select('year','month','station').distinct()

# count year-months
count_sec = count_sec.groupBy(['year','month']).count()

# sort by count and change name to value, also remove station from output

count_sec = count_sec.select('year','month','count').orderBy(F.desc('count'))

count_sec.rdd.saveAsTextFile("BDA/output/sec")
