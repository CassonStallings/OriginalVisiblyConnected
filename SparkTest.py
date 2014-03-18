#-------------------------------------------------------------------------------
# Name:        SimpleApp.py
# Purpose:     Sample Spark App
# Author:      Casson
# Created:     06/03/2014
#-------------------------------------------------------------------------------

"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "C:\Users\Casson\Desktop\Zipfian\techcrunch.py"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)