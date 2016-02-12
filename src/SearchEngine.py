#!/usr/bin/python
import os 
import sys
import time
reload(sys)
sys.setdefaultencoding('utf-8') # this helps with characters that give frustrating errors.

from java.io import File
from java.util import Scanner

from org.apache.lucene.index import IndexReader, Term 
from org.apache.lucene.store import SimpleFSDirectory 
from org.apache.lucene import document

import cPickle, gzip, csv 
import math

from CreateLexicon import pickle_up, calculateNormalizer, createLexicon, tf

class parse_query(object):
	""" This class defines the query parser. """
	def __init__(self, query, reader, normalizer = None, verbose = False):
		""" initializes the query parser. Basically creates a lucene Term for each query term and creates a list of them """ 
		terms = query.split(" ")
		self.query  =  []
		self.reader = reader 
		self.normalizer = normalizer

		self.norm = 0
		for word in terms:
			term = Term("contents",word)
			self.query.append(term)
			self.norm = self.norm + 1    # norm is jus the length of the query itself.

	def term_idf(self, term, verbose = False):
		""" for every Term in the format of lucene term, this returns its idf. It returns 1 if there is no idf """
		if verbose is True:
			print "... extracting the inverse document frequency of term: " + term.term().text()
		return math.log(float(self.reader.maxDoc()) / (0.0000001 + self.reader.docFreq(term)))

	def tf (self, verbose = False):
		""" outputs a dictionary of tf. 
		dictionary keys are the document numbers and its value is the tf of the query in the document 
		this is normalized by the document norm if flagged and loaded. 
		""" 
		if verbose is True:
			print "... extracting term-frequency of term : " + term.term().text() 
		out = []
		for term in self.query:   # all the words repeat only once .. ( Assumption : even if they repeat more than once, ignore it)
			out.append(1)
		return out
	def tf_idf (self, verbose = False):
		""" outputs a dictionary of tf. 
		dictionary keys are the document numbers and its value is the tf of the query in the document 
		this is normalized by the document norm if flagged and loaded. 
		this is also normalized by the idf of the respective terms.
		""" 		
		if verbose is True:
			print "... extracting term-frequency of term : " + term.term().text() 
		out = []
		for term in self.query:		
			term_idf = self.term_idf(term, verbose = verbose)
			out.append(term_idf)
		return out		

class search(object):
	""" This is the main class that is going to initiate the search engine """ 
	def __init__(self, create_lexicon_flag = False, tf_idf_flag = True, normalize = True, directory = '../index', n_retrieves = 10, verbose = False):
		""" The init function loads up the pickled tf lexicon, normalizers and the norms of all the documents. 
			Also this is the function that setsup the entire class incuding all its flags 
		""" 
		self.normalize = normalize
		if self.normalize is True:
			self.norm = pickle_up(filename = sys.argv[1] +'_norms.pkl.gz')
		self.tf_idf_flag = tf_idf_flag
		self.n_retrieves = n_retrieves
		directory = SimpleFSDirectory(File('../index'))	
		self.reader = IndexReader.open(directory)
		if self.normalize is False:
			self.norm = None
		if create_lexicon_flag is True:
			if normalize is True and False:
				print "... extracting all the norms of docs"
				start_time = time.clock()
				self.norm = calculateNormalizer(reader = self.reader, verbose = verbose)
				end_time = time.clock()
				print "... time taken for calculating norms is : " + str(end_time - start_time) + " seconds"
				pickle_down(filename = sys.argv[1] + '_norms', obj = self.norm)					
				# Need to write a loader function for normalize in case it is already run and pickled.... 
			self.lexicon = createLexicon(	   reader = self.reader,
										       norm = self.norm if self.normalize else None,
										       verbose = verbose)
		else:
			self.lexicon = pickle_up(sys.argv[1] + '_lexicon.pkl.gz')	
			#self.lexicon = json_up(sys.argv[1] + '_lexicon.json')


	def retrieve(self):
		""" This function retrieves
		Performs the steps in slide 31 lecture 3 
		"""
		## .. 
		sim = [float(0)]*self.reader.maxDoc()

		# initialized all sims to 0

		q_feat = self.query.tf_idf() if tf_idf_flag is True else self.query.tf()
		i = 0

		for term in self.query.query:
			I = self.lexicon[str(term.text())] # extract the lexicon of the term
			for doc_id, doc_feat in I:    # for every document that carries the term
				sim [int(doc_id)] = sim[int(doc_id)] + q_feat[i] * doc_feat
			i = i + 1
		if self.normalize is True:
			for doc_id, feat in I:
				sim[int(doc_id)] = sim[int(doc_id)] / self.norm[int(doc_id)]

		idx = sorted(range(len(sim)), key=lambda k: sim[k], reverse = True)	
		return (idx, sim) 

	def run(self, query, print_urls = True):
		"""this function basically runs a query""" 
		self.query = parse_query(query, self.reader)
		start_time = time.clock()
		doc_ids, score = self.retrieve()
		end_time = time.clock()
		print "... took " + str(end_time - start_time) + " seconds to retrieve"
		if print_urls is True:
			for i in xrange(self.n_retrieves):
				d = self.reader.document(doc_ids[i])			
				print "doc: [" + str(doc_ids[i]) +"], score: [" + str(score[doc_ids[i]]) +"], url: " + d.getFieldable("path").stringValue().replace("%%", "/")
			print  "... retrieval complete. "
			print  "..........................................................................." 
 

if __name__ == "__main__":

	""" setting up flags in this section """
	verbose = False					# if true prints a lot of stuff.. if false goes a little quiter
	create_lexicon_flag = True  	# if true will rebuild lexicon from scratch, if false will load a pre-created one as supplied in sys_arg[1]
	normalize = True 				# will use document norms and normalized tf-idf, false will not.
	n_retrieves = 10   				# number of documents to retreive
	tf_idf_flag = False 				# True retrieves based on Tf/idf, False retrieves based on only Tf. 
	directory = '../index'

	print "... loading please wait"
	start_time = time.clock()
	engine = search (   create_lexicon_flag = create_lexicon_flag, 
						normalize = normalize,
						directory = directory,
						n_retrieves = n_retrieves,
						tf_idf_flag = tf_idf_flag,
						verbose = verbose )
	end_time = time.clock()

	if (end_time - start_time) > 60:
		print "... finished loading in " + str((end_time - start_time)/60.) + " minutes"
	else:
		print "... finished loading in " + str(end_time - start_time) + " seconds"

	query = raw_input("... query> ")
	while not query == 'quit':
		engine.run (query = query, print_urls = True)
		query = raw_input("... query> ")