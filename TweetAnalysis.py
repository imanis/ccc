# Databricks notebook source exported at Sat, 24 Sep 2016 00:31:15 UTC

import time
import calendar
import codecs
import datetime
import json
import sys
import gzip
import string
import glob
import os
import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql.types import *
if ( sys.version_info.major == 3 ):
    from functools import reduce


# COMMAND ----------

df = sqlContext.sql("SELECT * FROM tweets_1")

# COMMAND ----------

frequencyMap = {}
globalTweetCounter = 0

timeFormat = "%d/%m/%Y %H:%M"

reader = codecs.getreader("utf-8")

for line in df.rdd.top(100): #.collect()
    
    # Try to read tweet JSON into object
    tweetObj_text = line['TWEET_TEXT']
    tweetObj_date = line['TWEET_DATE']
    
    # Try to extract the time of the tweet
    try:
        currentTime = datetime.datetime.strptime(tweetObj_date, timeFormat)
    except:
        print (line)
        raise

    tweetObj = {'text':tweetObj_text, 'date' : currentTime }

    currentTime = currentTime.replace(second=0)

    # Increment tweet count
    globalTweetCounter += 1

    # If our frequency map already has this time, use it, otherwise add
    if ( currentTime in frequencyMap.keys() ):
        timeMap = frequencyMap[currentTime]
        timeMap["count"] += 1
        timeMap["list"].append(tweetObj_text)
    else:
        frequencyMap[currentTime] = {"count":1, "list":[tweetObj]}

# Fill in any gaps
times = sorted(frequencyMap.keys())
firstTime = times[0]
lastTime = times[-1]
thisTime = firstTime

timeIntervalStep = datetime.timedelta(0, 60)    # Time step in seconds
while ( thisTime <= lastTime ):
    if ( thisTime not in frequencyMap.keys() ):
        frequencyMap[thisTime] = {"count":0, "list":[]}
        
    thisTime = thisTime + timeIntervalStep

print ("Processed Tweet Count:", globalTweetCounter)

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
fig.set_size_inches(18.5,10.5)

plt.title("Tweet Frequency")

# Sort the times into an array for future use
sortedTimes = sorted(frequencyMap.keys())

# What time span do these tweets cover?
print ("Time Frame:", sortedTimes[0], sortedTimes[-1])

# Get a count of tweets per minute
postFreqList = [frequencyMap[x]["count"] for x in sortedTimes]

# We'll have ticks every thirty minutes (much more clutters the graph)
smallerXTicks = range(0, len(sortedTimes), 30)
plt.xticks(smallerXTicks, [sortedTimes[x] for x in smallerXTicks], rotation=90)

# Plot the post frequency
ax.plot(range(len(frequencyMap)), [x if x > 0 else 0 for x in postFreqList], color="blue", label="Posts")
ax.grid(b=True, which=u'major')
ax.legend()

display(fig)

# COMMAND ----------

display(fig)

# COMMAND ----------


