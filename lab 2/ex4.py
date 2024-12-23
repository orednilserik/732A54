#!/usr/bin/env python3

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlCon = SQLContext(sc) # obtaining sql content from sc object

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))

lines_pre = precipitation_file.map(lambda line: line.split(";"))

# mapping the data we want, station as int so we can order by it
tempReads = lines.map(lambda p : Row(station =int(p[0]),temp=float(p[3])))

precipitation = lines_pre.map(lambda x:Row(station=int(x[0]),date=x[1],rain=float(x[3])))

# creating the data frame
schematemp = sqlCon.createDataFrame(tempReads)

schematemp.registerTempTable("yearmon_temps")

schemapre = sqlCon.createDataFrame(precipitation)

schemapre.registerTempTable('pre_station')


# picking out max for each station
schematemp = schematemp.groupBy('station').agg(F.max('temp').alias('maxTemp'))
# filtering the temps
schematemp = schematemp.filter((schematemp.maxTemp<=30) & (schematemp.maxTemp >=25))

# sum of the daily rain
schemapre = schemapre.groupBy(['station','date']).agg(F.sum('rain').alias('rain'))

# picking ot max rain, and removing the date with select

schemapre = schemapre.select('station', 'rain').groupBy('station').agg(F.max('rain').alias('maxDailyPrecipitation'))
# filter the rain

schemapre = schemapre.filter((schemapre.maxDailyPrecipitation>=100)&(schemapre.maxDailyPrecipitation <=200))


# joining the dataframes on station

merge = schematemp.join(schemapre,schematemp['station'] == schemapre['station'],'inner')

# sorting
#merge = merge.orderBy(F.desc('station'))

merge.rdd.saveAsTextFile("BDA/output/first")
