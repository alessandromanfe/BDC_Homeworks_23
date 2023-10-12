#------------------------------------------------------------------
# Padova                                                 04-12-2023
#                     Big Data 2022 - 2023
#                  Homework 1: Triangle Counting
#
# Group 46
# Group Members: Lorenzon Nicola, Manfè Alessandro, Mickel Mauro
#------------------------------------------------------------------

from pyspark import SparkContext, SparkConf, RDD
import time
import sys
import os
import numpy as np
import random as rand
from CountTriangles import CountTriangles


# Prime number constant for hashing
P = 8191

# Helper class to hash and compare tuple according to their indices
class HashPartitioner:
    def __init__(self,p,c):
        self.p = p
        self.c = c
        self.a = rand.randint(1,self.p-1)
        self.b = rand.randint(1,self.p-1)

    # Hash function ((A*u + B) mod P) mod C
    def getHash(self,u):
        return ((self.a*u + self.b) % self.p) % self.c
    
    # Return true if both elements in tuple have the same hash
    def cmpTuple(self,t: tuple):
        if len(t) != 2:
            return False
        return self.getHash(t[0]) == self.getHash(t[1])

def MR_ApproxTCwithNodeColors(docs: RDD, C: int):
    hp = HashPartitioner(P,C)
    tri_count = (docs
                 .filter(hp.cmpTuple)                       # MAP Phase: M1
                 .map(lambda x: (hp.getHash(x[0]),x))       # 
                 .groupByKey()                              # Shuffle + Grouping
                 .map(lambda x: (1,CountTriangles(x[1])))   # MAP Phase: M2
                 .reduceByKey(lambda x, y: x + y)           # REDUCE Phase: R2 
                 )
    val = tri_count.collect()[0][1]
    return (C**2)*val

# Execute second algorithm on the RDD
def MR_ApproxTCwithSparkPartitions(docs: RDD, C: int ):
    tri_count = (docs
                 .mapPartitions(lambda x: [(1,CountTriangles(x))])  # REDUCE Phase: R1
                 .reduceByKey(lambda x, y: x + y))                  # REDUCE Phase: R2
    val = tri_count.collect()[0][1]
    return (C**2)*val

def main():

    # Check the number of parameters
    if not (len(sys.argv) == 4):
        raise TypeError(f"You must pass 4 param")

    # Number of partitions
    C = int(sys.argv[1])
    # Number of rounds
    R = int(sys.argv[2])

    
    # Check R > 0
    if not (R > 0):
        raise TypeError(f"R must be greater than 0")
    
    data_path = str(sys.argv[3])
    
    # Spark app name and configuration
    conf = SparkConf().setAppName('TriangleCountExercise')
    sc = SparkContext(conf=conf)

    # Check if file exists
    assert os.path.isfile(data_path), "File or folder not found"
    docs = sc.textFile(data_path,minPartitions=C).cache()
    
    # Creates the RDD of edges 
    edges_df  = docs.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).repartition(C) # MAP Phase: M1
    
    # Print input parameters
    print(f"Dataset = {data_path}\nNumber of Edges = {edges_df.count()}\nNumber of Colors = {C}\nNumber of Repetitions = {R}")

    res_one_time = []
    res_one_res = []

    
    # Runs the first algorithm R times and takes the time intervall between the start and the end
    for i in range(R):
        start_time = time.time()
        tri_number = MR_ApproxTCwithNodeColors(edges_df,C)
        end_time = time.time()
        res_one_time.append(end_time - start_time)
        res_one_res.append(tri_number)

    # Prints data about the runs
    print("Approximation through node coloring")
    print(f"- Number of triangles (median over {R} runs) = {int(np.median(res_one_res))}")
    print(f"- Running time (average over {R} runs) = {int(np.average(res_one_time) * 1000)} ms")

    
    # Runs the second algorithm and takes the time intervall between the start and the end
    start_time = time.time()
    tri_number = MR_ApproxTCwithSparkPartitions(edges_df,C)
    end_time = time.time()
    res_two_time = (end_time - start_time)
    res_two_res = (tri_number)

    # Prints data about the run
    print("Approximation through Spark partitions")
    print(f"- Number of triangles = {res_two_res}")
    print(f"- Running time = {int(res_two_time * 1000)} ms")
    
if __name__ == "__main__":
    main()
