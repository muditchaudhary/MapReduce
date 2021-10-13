import sys
from operator import add

from pyspark import SparkContext


# Test 1 WordCount
sc = SparkContext("local[*]", "Test")

lines = sc.textFile("./Input_files/wordCount.txt", 1)
counts = lines.flatMap(lambda x: x.split(' '))\
.filter(lambda x: x != '')\
.map(lambda x: (x, 1))\
.reduceByKey(add)\
.saveAsTextFile("./Test_outputs/wordCount_testOut")


# Test 2 getAverageStockPrice
lines = sc.textFile("./Input_files/getAverageStockPrice.txt", 1)
averageStockPrice = lines.map(lambda x: (x.split(' ')[1], float(x.split(' ')[2])))\
    .mapValues(lambda v: (v,1))\
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
    .mapValues(lambda v:v[0]/v[1])\
    .saveAsTextFile("./Test_outputs/getAverageStockPrice_testOut")


# Test 3 searchWord
lines = sc.textFile("./Input_files/searchWord.txt", 1)
searchWord = "searchMe"
averageStockPrice = lines.flatMap(lambda x: x.split(' '))\
    .filter(lambda x: x != '')\
    .map(lambda x: (searchWord, 1 if x==searchWord else 0))\
    .reduceByKey(add)\
    .mapValues(lambda v:"True" if v>0 else "False")\
    .saveAsTextFile("./Test_outputs/searchWord_testOut")