import sys
import csv

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from operator import add, mul

conf = SparkConf().setAppName("Task2")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

rddLines = sc.textFile("/Users/Ludvig/Documents/NTNU/BigData/data/geotweets.tsv")
inputTweet = sc.textFile("/Users/Ludvig/Documents/NTNU/BigData/project/phase2/TDT4305/halla.txt")
sc.setLogLevel("WARN")

def placeAndTweetText(rddLines):
    placeText = rddLines.map(lambda line: line.split('\t')) \
        .map(lambda line: (line[4], line[10]))
    return placeText

def placeAndNumberOfTweets(rddLines):
    placeAndTweet = rddLines.map(lambda line: line.split('\t')).map(lambda line: (line[4], 1)).reduceByKey(add)
    return placeAndTweet

def findDistinctPlaces(rddLines):
    distinctPlaces = rddLines.map(lambda line: line.split('\t')) \
        .map(lambda x: x[4]) \
        .distinct()
    return distinctPlaces

def probabilityCalculation(line):
    T = float (totalNumber)
    Tc = float (line[1][0])
    #print(type(T))
    #print(T)
    #print(Tc)
    wordCountList = line[1][1]
    prob = Tc / T
    #print(type(prob))
    #print(prob)
    for count in wordCountList:
        prob = prob * count / Tc
    return prob


def allCitiesInputTweet(distinctPlaces, inputTweet):
    #Make inputTweet to list
    inputList = inputTweet.map(lambda x: x.split(' ')).first()
    #Connect the places with the inputTweet
    cityInputTweet = distinctPlaces.map(lambda place: (place, inputList))
    #Splits up the list so it is on the form: (city, word)
    cityWord = cityInputTweet.flatMapValues(lambda x: x)
    return cityWord

totalNumber = rddLines.count()
distinctPlaces = findDistinctPlaces(rddLines)
placeTextRDD = placeAndTweetText(rddLines)
cityWord = allCitiesInputTweet(distinctPlaces, inputTweet)
joinedRDD = placeTextRDD.join(cityWord)
filteredRDD = joinedRDD.filter(lambda line: ((" " + line[1][1]+ " ").lower() in (line[1][0])).lower())
mappedRDD = filteredRDD.map(lambda line: ((line[0], line[1][1]), 1))
reducedRDD = mappedRDD.reduceByKey(add)
newMappedRDD = reducedRDD.map(lambda line: (line[0][0], line[1]))
groupedRDD = newMappedRDD.groupByKey()
testMap = groupedRDD.map(lambda x: (x[0], list(x[1])))
placeNumber = placeAndNumberOfTweets(rddLines)
totalJoinedRDD = placeNumber.join(testMap)
placeProb = totalJoinedRDD.map(lambda x: (x[0], probabilityCalculation(x)))
sortedPlaceProb = placeProb.sortBy(lambda x: -x[1])
topFive = sortedPlaceProb.take(5)

sortedPlaceProb.coalesce(1).saveAsTextFile("/Users/Ludvig/Desktop/legendebiltyareneRTBanken")

#for i in topFive:
#   print(i)
