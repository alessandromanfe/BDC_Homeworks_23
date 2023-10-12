#------------------------------------------------------------------
# Padova                                                 04-12-2023
#                     Big Data 2022 - 2023
#          Homework 2: Triangle Counting on ClusterVeneto
#
# Group 46
# Group Members: Lorenzon Nicola, Manfè Alessandro, Mickel Mauro
#------------------------------------------------------------------

from pyspark import SparkContext, SparkConf, RDD
import time
import sys
import os
import numpy as np
from collections import defaultdict
import random as rand


# Prime number constant for hashing
P = 8191

def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:

        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count


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

# create_trile
def create_triple(color, x, C):
    kvList = []
    for i in range(C):
        kvList.append(( (*sorted((*color,i)),) , x ))
    return kvList
        
# Execute second algorithm on the RDD
def MR_ExactTC(docs: RDD, C: int):
    hp = HashPartitioner(P,C)
    tri_count = (docs
                .flatMap(lambda x: create_triple((hp.getHash(x[0]), hp.getHash(x[1])), x, C)) # MAP Phase: M1
                .groupByKey()
                .map(lambda x: countTriangles2(x[0], x[1], hp.a, hp.b, hp.p, C))    # Map phase M1
                .sum()
                )
    print(tri_count)
    return tri_count

def main(argv):

    # Check the number of parameters
    if not (len(argv) == 4):
        raise TypeError("You must pass 4 param")

    # Number of partitions
    C = int(argv[0])
    # Number of rounds
    R = int(argv[1])
    # Binary flag 
    F = bool(int(argv[2]))

    
    # Check R > 0
    if not (R > 0):
        raise TypeError("R must be greater than 0")
    
    data_path = str(argv[3])
    
    # Spark app name and configuration
    conf = SparkConf().setAppName('TriangleCountExercise')
    conf.set("spark.locality.wait","0s");
    sc = SparkContext(conf=conf)

    # Check if file exists
    #os.path.isfile(data_path)
    docs = sc.textFile(data_path,minPartitions=32).cache()
    
    # Creates the RDD of edges 
    edges_df  = docs.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).repartition(32) # MAP Phase: M1
    
    # Print input parameters
    print("Dataset = {0}\nNumber of Edges = {1}\nNumber of Colors = {2}\nNumber of Repetitions = {3}".format(data_path, edges_df.count(), C, R))
    
    if F is True:
        res_two_time = []
        # Runs the second algorithm R times and takes the time intervall between the start and the end each time
        for i in range(R):

            start_time = time.time()
            tri_number = MR_ExactTC(edges_df, C)
            end_time = time.time()
            res_two_time.append(end_time - start_time)
            res_two_res = (tri_number)

        # Prints data about the run
        print("Exact algorithm with node coloring")
        print("- Number of triangles = {}".format(res_two_res))
        print("- Running time (average over {} runs) = {} ms".format(R, int(np.average(res_two_time) * 1000)))
   
    elif F is False:
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
        print("Approximation algorithm with node coloring")
        print("- Number of triangles (median over {} runs) = {}".format(R, int(np.median(res_one_res))))
        print("- Running time (average over {} runs) = {} ms".format(R, int(np.average(res_one_time) * 1000)))

if __name__ == "__main__":
    main(sys.argv[1:])
