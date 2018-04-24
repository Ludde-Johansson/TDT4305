import sys, getopt
import csv

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from operator import add

conf = SparkConf().setAppName("Task2")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

#Transforms the training set into a smaller set containing only the place and the tweet text
def placeAndTweetText(rddLines):
    placeText = rddLines.map(lambda line: line.split('\t')) \
        .map(lambda line: (line[4], line[10]))
    return placeText

#Maps the training set into a new RDD, containing the place along with
#the amount of tweets tweeted from the given place by using reduceByKey.
def placeAndNumberOfTweets(rddLines):
    placeAndTweet = rddLines.map(lambda line: line.split('\t')).map(lambda line: (line[4], 1)).reduceByKey(add)
    return placeAndTweet

#Transforms the training set to a new RDD containing only distinct places and nothing else
def findDistinctPlaces(rddLines):
    distinctPlaces = rddLines.map(lambda line: line.split('\t')) \
        .map(lambda x: x[4]) \
        .distinct()
    return distinctPlaces

#Calculates the probability of the location of a new tweet, using
#the Naive Bayes classifier, as given in the task description
def probabilityCalculation(totalNumber, line):
    T = float(totalNumber)
    Tc = float(line[1][0])
    wordCountList = line[1][1]
    prob = Tc / T
    for count in wordCountList:
        prob = prob * count / Tc
    return prob

#Connects the words in the inputTweet to all of the places in the
#training set, and uses flatMapValues in order to get the words
#separated
def allCitiesInputTweet(distinctPlaces, inputTweet):
    #Make inputTweet to list
    inputList = inputTweet.map(lambda x: x.split(' ')).first()
    #Connect the places with the inputTweet
    cityInputTweet = distinctPlaces.map(lambda place: (place, inputList))
    #Splits up the list so it is on the form: (city, word)
    cityWord = cityInputTweet.flatMapValues(lambda x: x)
    return cityWord

def main(argv):
    trainingFile = ''
    inputFile = ''
    outputFile = ''
    try:
      opts, args = getopt.getopt(argv,"t:i:o:",["training=","input=","output="])
    except getopt.GetoptError:
      print("Something went wrong when taking in the file paths!")
      sys.exit(2)

    for opt, arg in opts:
        if opt in ('--training'):
            trainingFile = arg
        elif opt in ('--input'):
            inputFile = arg
        elif opt in ('--output'):
            outputFile = arg

    #Takes in the parameters and assigns them to variables
    rddLines = sc.textFile(trainingFile)
    inputTweet = sc.textFile(inputFile)
    outputPath = outputFile

    #Calculates the total number of tweets in the training set
    totalNumber = rddLines.count()
    #Gives us an RDD of all of the distinct places in the training set
    distinctPlaces = findDistinctPlaces(rddLines)
    #Gives us an RDD where the places are the keys and the tweet texts are the value
    placeTextRDD = placeAndTweetText(rddLines)
    #Connects all places with the words in the input tweet
    cityWord = allCitiesInputTweet(distinctPlaces, inputTweet)
    #Joins the two RDDs, giving a RDD with place as key,
    #tweet text and input tweet words are value
    joinedRDD = placeTextRDD.join(cityWord)
    #Filters out tuples where the words from the input tweet text are
    #not present in the tweet text from the training set
    filteredRDD = joinedRDD.filter(lambda line: ((" " + line[1][1]+ " ").lower() in (line[1][0]).lower()))
    #Transforms the filteredRDD into a new RDD containing the key (place, word) and a value 1
    mappedRDD = filteredRDD.map(lambda line: ((line[0], line[1][1]), 1))
    #Uses reduceByKey in order to calculate how many tweets from each place contains each word in the input tweet
    #from a place
    reducedRDD = mappedRDD.reduceByKey(add)
    #Transforms the RDD and 'removes' the word, as it is no longer needed
    newMappedRDD = reducedRDD.map(lambda line: (line[0][0], line[1]))
    #Groups the frequency of each word in each city in order to make calculations afterwards
    groupedRDD = newMappedRDD.groupByKey()
    #Transform the values to a list, so it can be calculated
    testMap = groupedRDD.map(lambda x: (x[0], list(x[1])))
    #Assigns the placeNumber variable
    placeNumber = placeAndNumberOfTweets(rddLines)
    #Joins the two RDDs in order to get a new RDD on the format:
    #(place, (Tc, [Tc_w1, Tc_w2, Tc_w3, ..]))
    totalJoinedRDD = placeNumber.join(testMap)
    #Calculated the probability for each place
    placeProb = totalJoinedRDD.map(lambda x: (x[0], probabilityCalculation(totalNumber, x)))
    #Finds the maximum value
    maxPlaceProb = placeProb.takeOrdered(1, key=lambda x: -x[1])
    #Checks if there are multiple maximum tuples
    maximumPlaceProb = placeProb.filter(lambda x: (x[1] == maxPlaceProb[0][1]))
    #Saves result to the given outputPath
    maximumPlaceProb.coalesce(1).saveAsTextFile(outputPath)

if __name__ == "__main__":
    main(sys.argv[1:])
