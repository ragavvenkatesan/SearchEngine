#!/usr/bin/python
# Code written by Ragav Venkatesan 
# Code extracts TF-IDF from a lucene index and saves it in a csv file.
#
# run as 
#
# 			jython <filename.py> 
#
#
# Pre-requisite:
# 
# 	must have jython and pylucene installed
#########################################################################################

import os 
import sys
reload(sys)
sys.path.append("../lib/lucene-core-3.6.2.jar")
sys.path.append("../lib/lucene-core-3.6.2-javadoc.jar")
sys.setdefaultencoding('utf-8') # this helps with characters that give frustrating errors.

from java.io import File
from java.util import Scanner

import json

from org.apache.lucene.index import IndexReader, Term 
from org.apache.lucene.store import SimpleFSDirectory 

import cPickle, csv 
import math
import time

def idf(reader, term, verbose = False):
	""" for every Term in the format of lucene term, this returns its idf. It returns 1 if there is no idf """
	if verbose is True:
		print "extracting the inverse document frequency of term: " + term.term().text()
	return math.log(float(reader.maxDoc()) / (0.0000001 + reader.docFreq(Term("contents",term.term().text()))))

def pickle_down(filename, obj):	
	f = open(filename +".pkl", 'wb')	
	cPickle.dump(obj, f)
	f.close()	
	
def json_down(filename, obj):	
	f = open(filename + '.json', 'w')	
	json.dump(obj, f)
	f.close()

def pickle_up(filename):
	f = open(filename +".pkl", 'rb')  
	return cPickle.load(f)

def json_up(filename):
	f = open(filename+".json", 'r')   
	return json.load(f)	

def calculateNormalizer(reader, verbose = True):
	""" Precalculate normalizers for all documents
		When you calculate term raw_frequency, normalize them with these.
	""" 
	nDocs = reader.maxDoc()
	nf = [float(0)] * nDocs
	norm = [float(0)] * nDocs
	term = reader.terms()
	while term.next():
		td = reader.termDocs(term.term())
		while td.next():
			norm [int(td.doc())] = norm [int(td.doc())] + td.freq() ** 2
	for i in xrange(nDocs):
		norm[i] = math.sqrt(norm[i])

	return norm
	
def tf(reader, term, norm, verbose = True):
	""" Returns a dictionary.
		out["doc_id"] = frequncy of the terms in the doc_id in the reader object
	"""
	if verbose is True:
		print "extracting term-frequency of term : " + term.term().text() 

	te = Term("contents",term.term().text())	
	totalDocs = reader.maxDoc()

	# This one makes it a non-sparse array
	# out = [0] * totalDocs
	out = []
	docs = reader.termDocs(te)
	while docs.next():
		# out[docs.doc()] = docs.freq()
		normal = norm[docs.doc()] if norm is not None else 1
		out.append(  (  str(docs.doc()), docs.freq() / normal)  )
	return out


def tf_idf (reader, term, norm, verbose = False):
	""" Returns a dictionary.
		out["doc_id"] = frequncy of the terms in the doc_id in the reader object / its idf
	"""		
	if verbose is True:
		print "extracting term-frequency / inverse-document frequency of term : " + term.term().text() 
	te = Term("contents",term.term().text())	
	out = []
	docs = reader.termDocs(te)
	term_idf = idf(reader, term, verbose = verbose)
	while docs.next():
		norm_now = norm[docs.doc()] if norm is not None else 1
		out.append ( (str(docs.doc()), docs.freq() * term_idf / (norm_now) ) )
	return out	

def createLexicon (reader, filename, norm = None, tf_idf_flag = False, verbose = True):
	""" Returns nothing, but saves down a pickle file of lexicon
		Saved Down Lexicon Contains the TF only. Not IDF. 
	"""

	term_frequency = {}
	if verbose is True:
		print "creating the lexicon from the index files "	
	term = reader.terms()	
	count = 0 
	while term.next():
		if tf_idf_flag is False:
			raw_frequency = tf(reader = reader, term = term, norm = norm, verbose = verbose )		
		else:
			raw_frequency = tf_idf (reader = reader, term = term, norm = norm, verbose = verbose )
		term_frequency[str(term.term().text())] = raw_frequency 
		if verbose is True:
			print "extracting lexicon for term " + str(count)
		count = count + 1 
	return term_frequency

def sort_by_idf(reader, n):
    terms = reader.terms()
    idf_list = []
    while terms.next():
        idf_list.append(idf(reader,terms))
    idx = sorted(range(len(idf_list)), key=lambda k: idf_list[k])

    for i in xrange(n):
        count = 0
        terms = reader.terms()        
        while terms.next():
            if count == idx[i]:
                print str(terms.term().text()) + " " + str(idf_list[idx[i]])
            count = count + 1

if __name__ == "__main__":

	verbose = False 
	normalize = True
	tf_idf_flag = True

    
	directory = SimpleFSDirectory(File('../index'))	
	reader = IndexReader.open(directory)
    # sort_by_idf(reader,100)

	if len(sys.argv) < 2:
		filename = 'temp'
	else:
		filename = sys.argv[1]

	if normalize is True:
		print "extracting all the norms of docs"
		start_time = time.clock()
		norms = calculateNormalizer(reader = reader, verbose = verbose)
		end_time = time.clock()		
		print "time taken for calculating norms is : " + str(end_time - start_time) + " seconds"
		pickle_down(filename = filename + '_norms', obj = norms)

	lexicon  = createLexicon(	
					filename = filename,
					reader = reader,
					norm = norms if normalize else None,
					tf_idf_flag = tf_idf_flag, 
					verbose = verbose)