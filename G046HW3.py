# coding=utf-8
#------------------------------------------------------------------
# Padova                                                 30-05-2023
#                     Big Data 2022 - 2023
#          Homework 3: frequencies on streaming 
#
# Group 46
# Group Members: Lorenzon Nicola, Manfè Alessandro, Mickel Mauro
#------------------------------------------------------------------

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random as rand
import numpy as np


# Prime number constant for hashing
P = 8191

# After how many items stop
THRESHOLD = 10000000

class HashPartitioner:
    def __init__(self,p,c):
        self.p = p
        self.c = c
        self.a = rand.randint(1,self.p-1)
        self.b = rand.randint(0,self.p-1)

    # Hash function ((A*u + B) mod P) mod C
    def getHash(self,u):
        return ((self.a*u + self.b) % self.p) % self.c
    
    
def process_batch_CountSketch(time,batch):
    #global variables to pass values
    global streamLength, frequencies, counters, hashListH, hashListG, left, right

	#to avoid numbers after the threshold
    if streamLength[0] >= THRESHOLD:
        return
    
	#get stream size
    batch_size = batch.count()
    streamLength[0] += batch_size
    
    
    batch_items = (batch
                    .filter(lambda x: int(x)>=left and int(x)<=right)	#filtering
                    .map(lambda x: (int(x),1))							#M
					.reduceByKey(lambda i1,i2: i1+i2)					#R
					.collectAsMap()										#collect
					)
    
    for key in batch_items:
        if key not in frequencies:	#if item not in the hashmap add it
            frequencies[key] = 0
        frequencies[key] += batch_items[key]	#sum the true frequency of item in the batch

        # Compute the counter value for the item
        for i,(H,G) in enumerate(zip(hashListH,hashListG)):
            counters[i][H.getHash(key)] += batch_items[key]*(G.getHash(key)*2-1) 
	
	#stopping condition
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()


if __name__ == '__main__':

    # Check the number of parameters
    assert len(sys.argv) == 7, "Need 6 param"

    conf = SparkConf().setMaster("local[*]").setAppName("G046HW3")
    conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    # stopping condition
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    #Number of rows
    D = int(sys.argv[1])
    #Number of columns
    W = int(sys.argv[2])
    #Left endpoint
    left = int(sys.argv[3])
    #Right endpoint
    right = int(sys.argv[4])
    #Number of top frequent items
    K = int(sys.argv[5])
    #Port number
    portExp = int(sys.argv[6])

	#print input values
    print("D = {} W = {} [left,right] = [{},{}]  K = {} Port = {}".format(D,W,left,right,K,portExp))
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    # Stream length
    streamLength = [0] 

    #hash table for items frequency
    frequencies = {} 
    #array (table) of counters
    counters = np.zeros((D,W), dtype=np.int64)
    #list of hash function for filling the table
    hashListH = [HashPartitioner(P,W) for i in range(D)]
    #list of unbiasing hash function
    hashListG = [HashPartitioner(P,2) for i in range(D)]
    

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    stream.foreachRDD(lambda time,batch : process_batch_CountSketch(time,batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    # Starting streaming engine
    ssc.start()
    # Waiting for shutdown condition
    stopping_condition.wait()
    # Stopping the streaming engine
    ssc.stop(False, True)
    # Streaming engine stopped


    # COMPUTE AND PRINT FINAL STATISTICS

	#number of distinct items in R
    distinctItemCount = len(frequencies.keys())

	#number of items in R
    subsetCount = sum(frequencies.values())

	#the K highest frequencies items
    sortedFreq = sorted(frequencies.items(), key=lambda item: item[1], reverse=True)

    #value of the Kth-highest frequency
    KThreshold = sortedFreq[K - 1][1]
    
    #Items with frequencies >= KThreshold
    topFreItems = dict(filter(lambda item: item[1] >= KThreshold, sortedFreq))

	#compute true second moment
    trueSecondMoment = 0
    for key in frequencies:
        trueSecondMoment += frequencies[key]**2
    trueSecondMoment /= (subsetCount**2)
	
	#compute approximate second moment
    approxSecondMoment = np.median(np.power(counters,2).sum(axis=1))/subsetCount**2

	#compute approximate freq for each item with freq > freq(K) 
    approxFrequencies = {}
    relativeErrors = []
    for key in topFreItems:
        #compute the approximate frequent of the item and save it
        approxFreq = np.median(np.array([(G.getHash(key)*2-1)*counters[i][H.getHash(key)] for i,(H,G) in enumerate(zip(hashListH,hashListG))]))
        approxFrequencies[key] = approxFreq

        #calculate the error and save it
        relativeErrors.append(abs(frequencies[key]-approxFreq)/frequencies[key])

    #calculate the average error 
    avgRelativeError = np.mean(np.array(relativeErrors))
	

    #prints
    print("Total number of items = ",streamLength[0])
    print("Total number of items in [{},{}] = {}".format(left,right,subsetCount))
    print("Number of distinct items in [{},{}] = {}".format(left,right,distinctItemCount))

    #print the item if K<=20
    if K <= 20:
        for key in topFreItems:
            print("Item {} Freq = {} Est. Freq = {}".format(key,frequencies[key],int(approxFrequencies[key])))
    
    print("Avg err for top K = ", avgRelativeError)
    print("F2 {} F2 Estimate {}".format(trueSecondMoment,approxSecondMoment))
