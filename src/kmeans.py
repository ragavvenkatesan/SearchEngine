# from https://gist.github.com/betzerra/8744068

import sys
import math
import random
import subprocess   
  
class Point:
    def __init__(self, coords, id = None):
        self.coords = coords
        self.n = len(coords)
        if not id is None:
            self.id = id
        
    def __repr__(self):
        return str(self.coords)

class Cluster:
    def __init__(self, points):
        if len(points) == 0: raise Exception("ILLEGAL: empty cluster")
        self.points = points
        self.n = points[0].n
        for p in points:
            if p.n != self.n: raise Exception("ILLEGAL: wrong dimensions")
            
        self.centroid = self.calculateCentroid()
        
    def __repr__(self):
        return str(self.points)
    
    def update(self, points):
        old_centroid = self.centroid
        self.points = points
        self.centroid = self.calculateCentroid()
        shift = getDistance(old_centroid, self.centroid) 
        return shift
    
    def calculateCentroid(self):
        numPoints = len(self.points)
        coords = [p.coords for p in self.points]
        unzipped = zip(*coords)
        centroid_coords = [math.fsum(dList)/numPoints for dList in unzipped]        
        return Point(centroid_coords)

def kmeans(points, k, cutoff, verbose = True):
    
    initial = random.sample(points, k)   
    clusters = [Cluster([p]) for p in initial]
    print "clustering results" 
    loopCounter = 0
    while True:
        lists = [ [] for c in clusters]
        clusterCount = len(clusters)
        
        loopCounter += 1
        if verbose is True:
            print "k means iter: ", loopCounter
        for p in points:
            smallest_distance = getDistance(p, clusters[0].centroid)
            clusterIndex = 0
        
            for i in range(clusterCount - 1):
                distance = getDistance(p, clusters[i+1].centroid)
                if distance < smallest_distance:
                    smallest_distance = distance
                    clusterIndex = i+1
            lists[clusterIndex].append(p)
        
        biggest_shift = 0.0
        
        for i in range(clusterCount):
            shift = clusters[i].update(lists[i])
            biggest_shift = max(biggest_shift, shift)
        
        if biggest_shift < cutoff:
            print "Converged after %s iterations" % loopCounter
            break
    return clusters

def getDistance(a, b):
    if a.n != b.n:
        raise Exception("ILLEGAL: non comparable points")
    
    ret = reduce(lambda x,y: x + pow((a.coords[y]-b.coords[y]), 2),range(a.n),0.0)
    return math.sqrt(ret)
