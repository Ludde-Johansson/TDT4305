import sys
import csv
import List

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from operator import add, mul

conf = SparkConf().setAppName("Task2")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

rddLines = sc.textFile(str(sys.argv[1]))
rddLines = rddLines.sample(False, 0.1, 5)
inputTweet = sc.textFile(str(sys.argv[2]))
outputFile = str(sys.argv[3])

totalNumber = rddLines.count()
distinctPlaces = findDistinctPlaces(rddLines)
placeTextRDD = placeAndTweetText(rddLines)


def placeAndNumberOfTweets(rddLines):
    placeAndTweet = rddLines.map(lambda line: line.split('\t')).map(lambda line: (line[4], 1)).reduceByKey(add)
    return placeAndTweet

def placeInputTweetWords(placeTextRDD, inputTweet):
    placeTextRDD.map(lambda line: line[0] )

def tweetsPerPlace(place):
    tpp = rddLines.map(lambda line: line.split('\t')).map(lambda line: (line[4], 1)).reduceByKey(add).filter(lambda x: x[0].lower() == place.lower())
    return tpp.first()[1]

def findDistinctPlaces(rddLines):
    distinctPlaces = rddLines.map(lambda line: line.split('\t')) \
        .map(lambda x: x[4]) \
        .distinct()
    return distinctPlaces

def placeAndTweetText(rddLines):
    placeText = rddLines.map(lambda line: line.split('\t')) \
        .map(lambda line: (line[4], line[10])) \
        .map(lambda x: (x[0], x[1].split(" "))) \
        .map(lambda y: (y[0], list(set(y[1])))) \
        .flatMapValues(lambda x: x)
    return placeText

def countPlaceWordProbability(place, word):
    placeWordCount = placeTextRDD.filter(lambda line: ((line[0].lower() == place.lower()) and (word.lower() == line[1].lower()))).count()
    prob = placeWordCount / tweetsPerPlace(place)
    return prob

def calc(place, tweetText):
    prob = tweetsPerPlace(place) / totalNumber;
    wordProbRDD = tweetText.flatMap(lambda x: x.split(" ")).map(lambda x: countPlaceWordProbability(place, x))
    probSum = wordProbRDD.foreach(mul)
    return probSum

def calculateProbability(distinctPlaces, inputTweet):
    test = placeTextRDD.map(lambda placeAndText: (placeAndText[0], calc(placeAndText[1], inputTweet)))
    return test
    #prob = 1;
    #for each place in distinctPlaces:
    #    for each word in inputTweet:
    #        prob = prob * countPlaceWord(place, word) / tweetsPerPlace(place)
    #    prob = prob * tweetsPerPlace(place) / totalNumber
