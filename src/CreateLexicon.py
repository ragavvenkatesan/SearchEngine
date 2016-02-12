#!/usr/bin/python
# Code written by Ragav Venkatesan 
# Code extracts TF-IDF from a lucene index and saves it in a csv file.
# 
# to run the code first set up the lucene path 
#
#
# 			export CLASSPATH=/Users/ragav/Desktop/IR/Project/lib/lucene-core-3.6.2.jar
#
#
# then run as 
#
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
sys.setdefaultencoding('utf-8') # this helps with characters that give frustrating errors.

from java.io import File
from java.util import Scanner

import pdb

from org.apache.lucene.index import IndexReader, Term 
from org.apache.lucene.store import SimpleFSDirectory 

import cPickle, gzip, csv 
import math
import time

def idf(reader, term, verbose = False):
	""" for every Term in the format of lucene term, this returns its idf. It returns 1 if there is no idf """
	if verbose is True:
		print "... extracting the inverse document frequency of term: " + term.term().text()
	return math.log(float(reader.maxDoc()) / (0.0000001 + reader.docFreq(Term("contents",term.term().text()))))

def pickle_down(filename, obj):
	""" Write down a dictionary as a pkl """ 
	
	f = gzip.open(filename + '.pkl.gz', 'wb')	
	cPickle.dump(obj, f)
	f.close()
	"""
	writer = csv.writer(open(filename + '.csv', 'wb'))
	for key, value in obj.items():
		writer.writerow([key, value])
	"""
"""
def json_down(filename, obj):
	
	f = open(filename + '.json', 'w')	
	json.dump(obj, f)
	f.close()
"""
def pickle_up(filename):
	f = gzip.open(filename, 'rb')
	"""
	reader = csv.reader(open(filename, 'rb'))
	mydict = dict(reader)	
	return mydict
	"""    
	return cPickle.load(f)

def json_up(filename):
	f = open(filename, 'r')
	"""
	reader = csv.reader(open(filename, 'rb'))
	mydict = dict(reader)	
	return mydict
	"""    
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

	"""    
	for i in xrange(nDocs): # For every doc,
		max_freq = 0 	
		term = reader.terms()
		norm_temp = 0		
		print "... calculating NF for doc " + str(i)	
		while term.next():  # For every term, 
			td = reader.termDocs(term.term())   # collect all the document in which the term is present.
			while td.next():
				if td.doc() == i:				# if the term is present in the current document
					norm_temp = norm_temp + td.freq() ** 2
					if td.freq() > max_freq:    # if the term has a higher frequency than the current maxFreq
						max_freq = td.freq()    # this is the term with the highest freqency in the doc						
		if verbose is True:
			if max_freq > 0: # Dummy check. 
				print "... NF of doc " + str(i) + " is " + str(max_freq)
		nf.append(max_freq)
		norm.append(math.sqrt(norm_temp))
	"""	
	return norm
	
def tf(reader, term, norm, verbose = True):
	""" Returns a dictionary.
		out["doc_id"] = frequncy of the terms in the doc_id in the reader object
	"""
	if verbose is True:
		print "... extracting term-frequency of term : " + term.term().text() 

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
		print "... extracting term-frequency / inverse-document frequency of term : " + term.term().text() 
	te = Term("contents",term.term().text())	
	out = []
	docs = reader.termDocs(te)
	term_idf = idf(reader, term, verbose = verbose)
	while docs.next():
		norm_now = norm[docs.doc()] if norm is not None else 1
		out.append ( (str(docs.doc()), docs.freq() / (norm_now * term_idf) ) )
	return out	

def createLexicon (reader, norm = None, tf_idf_flag = False, verbose = True):
	""" Returns nothing, but saves down a pickle file of lexicon
		Saved Down Lexicon Contains the TF only. Not IDF. 
	"""

	term_frequency = {}
	print "... creating the lexicon from the index files "	
	term = reader.terms()	
	count = 0 
	while term.next():
		if tf_idf_flag is False:
			raw_frequency = tf(reader = reader, term = term, norm = norm, verbose = verbose )		
		else:
			raw_frequency = tf_idf (reader = reader, term = term, norm = norm, verbose = verbose )
		term_frequency[str(term.term().text())] = raw_frequency 
		if verbose is True:
			print "... extracting lexicon for term " + str(count)
		count = count + 1 
	pickle_down(filename = sys.argv[1] +'_lexicon', obj = term_frequency)
	# json_down(filename = sys.argv[1] +'_lexicon', obj = term_frequency)

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
    pdb.set_trace()
    	    
if __name__ == "__main__":

	verbose = False 
	normalize = True
	tf_idf_flag = True

    
	directory = SimpleFSDirectory(File('../index'))	
	reader = IndexReader.open(directory)
    # sort_by_idf(reader,100)
	if normalize is True:
		print "... extracting all the norms of docs"
		start_time = time.clock()
		norms = calculateNormalizer(reader = reader, verbose = verbose)
		end_time = time.clock()		
		print "... time taken for calculating norms is : " + str(end_time - start_time) + " seconds"
		pickle_down(filename = sys.argv[1] + '_norms', obj = norms)

	lexicon  = createLexicon(	reader = reader,
					norm = norms if normalize else None,
					tf_idf_flag = tf_idf_flag, 
					verbose = verbose)