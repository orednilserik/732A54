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
tempReads = lines.map(lambda p : Row(year= int(p[1][0:4]),station=p[0],temp=float(p[3])))

# creating the data frame
schematemp = sqlCon.createDataFrame(tempReads)

schematemp.registerTempTable("year_temps")

# filtering the years

schematemp = schematemp.filter((schematemp.year<=2014) & (schematemp.year >=1950))

# picking out max temp for each year
schematemp_max = schematemp.groupBy('year').agg(F.max('temp').alias('temp'))
# min temp
schematemp_min = schematemp.groupBy('year').agg(F.min('temp').alias('temp'))


# Joining them to get the station in the dataframe
schematemp_max = schematemp.join(schematemp_max,['year','temp'])
schematemp_min = schematemp.join(schematemp_min,['year','temp']) 

# selecting to get correct order of the columns then sort
schematemp_max = schematemp_max.select('year','station','temp').orderBy(F.desc('temp'))
schematemp_min = schematemp_min.select('year','station','temp').orderBy(F.desc('temp'))

schematemp_max.rdd.saveAsTextFile("BDA/output/max")
schematemp_min.rdd.saveAsTextFile("BDA/output/min")

#schematemp.select('year','station','temp').filter(1959<= 'year'<=2014).groupBy('year').agg(F.max('temp')).orderby(desc('temp'))