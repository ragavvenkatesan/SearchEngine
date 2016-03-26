#!/usr/bin/python
import sys, os 
sys.path.append("../lib/lucene-core-3.6.2.jar")
sys.path.append("../lib/lucene-core-3.6.2-javadoc.jar")


from java.io import *
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.index import IndexReader 

import pdb

def readfile (reader, file, verbose):
    """ create a hash table with the string of docids as keys """ 
    
    if verbose is True:
        print "reading file " + file                         
    numDocs = reader.maxDoc()   
    if verbose is True:
        print "number of documents in corpus is " + str(numDocs)
    # Read links file 
    br = BufferedReader(FileReader(file))
    dictionary = {}          
    s = br.readLine()
    while s is not None:        
        words = s.split("->")
        src = words[0]
        dictionary[int(src)] = []                        
        if (len(words) > 1 and len(words[1]) > 0):
            dest = words[1].split(",")
            for des in dest:
                if verbose is True:
                    print "link " + src + " -> " + des + " added" 
                dictionary[int(src)].append(int(des))
        else:
            dictionary[int(src)].append(0)
        s = br.readLine()
    br.close()
    if verbose is True:
        print "reading of " + file + " complete"       
    return dictionary
        
        
class LinkAnalysis(object):
    def __init__ (  self,
                    linksFile,
                    citationsFile,
                    reader,
                    verbose = True  ):
    
        if verbose is True:
            print "loading link analyzer"
        self.links = readfile( reader = reader, file = linksFile, verbose = verbose ) 
        self.citations = readfile( reader = reader, file =citationsFile, verbose = verbose )    
        
      
    def getLinks (self, doc, verbose = False):
        if verbose is True:
            print "getting links for document " +str(doc)
        return self.links[int(doc)] 
        
    def getCitations (self, doc, verbose = False):
        """ Supply only str for doc"""
        if verbose is True:
            print "getting citations for document " +str(doc)        
        return self.citations[int(doc)] 
        
if __name__ == "__main__":

    linksFile = "../index/IntLinks.txt"
    citationsFile = "../index/IntCitations.txt"
    r = IndexReader.open(SimpleFSDirectory(File('../index')))
    verbose = False
    
    l = LinkAnalysis(
                        linksFile       = linksFile,
                        citationsFile   = citationsFile,
                        reader          = r,
                        verbose         = verbose
                    )		
                    
    # Find all the document numbers that doc #3 points to
    print "document number 3 links to: ",
    links3 = l.getLinks(str(3),verbose)
    for pb in links3:
        print str(pb) + ", ",
    print " "
    
    print "document number 3 is cited by: ",
    links3 = l.getCitations(str(3),verbose)
    for pb in links3:
        print str(pb) + ", ",