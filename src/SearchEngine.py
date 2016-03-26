#!/usr/bin/python
# Code written by Ragav Venkatesan 
# Code extracts TF-IDF from a lucene index and saves it in a csv file.
# The it runs a search engine over the index based on the parameters supplied.
# 
#
#  run as 
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
import pdb

import time
import cPickle
import csv 
import math 

reload(sys)
sys.path.append("../lib/lucene-core-3.6.2.jar")
sys.path.append("../lib/lucene-core-3.6.2-javadoc.jar")
sys.setdefaultencoding('utf-8') # this helps with characters that give frustrating errors.

from random import randint

from java.io import File
from java.util import Scanner

from org.apache.lucene.index import IndexReader, Term 
from org.apache.lucene.store import SimpleFSDirectory 
from org.apache.lucene import document

from CreateLexicon import pickle_up, pickle_down
from CreateLexicon import calculateNormalizer, createLexicon
from CreateLexicon import tf, json_up, json_down

from LinkAnalysis import LinkAnalysis

def normalizer ( l ):
	minList = min(l)
	maxList = max(l)
	for item in xrange(len(l)):
		l[item] = (l[item] - minList) / (maxList - minList)	
	return l

def page_rank  (   	alpha,
					maxIter,
					numDocs,
					graph,
					saver,
					epsilon = 1e-7,
					verbose = True
				):

	start_time = time.clock()
	if verbose is True:
		print "estimating page rank"
		print "creating M matrix"	
	
	M = {}
	k2 = (1-alpha)*(1./numDocs)
	k1 = 1./numDocs 

	keys = {}
	for doc in xrange(numDocs):
		links = graph.getLinks(doc)
		keys[doc] = []
		if links == []:
			for link in xrange(numDocs):
				M[str(link)+","+str(doc)] = k1
				keys[doc].append(link)
		else:
			prob = 1./len(links)
			for link in links:
				M[str(link)+","+str(doc)] = alpha * prob + k2
				keys[doc].append(link)

	rank = list([k1] * numDocs)
	iteration = 0
	old_rank = list(rank)
	key_exist = M.keys()
	print "total non-zero entries in M " + str(len(key_exist))

	if verbose is True:
		print "power iterating"
	terminate = False		

	while iteration < maxIter and terminate is False:

		start_iter_time = time.clock()
		terminate = True
		if verbose is True:
				print "iteration " + str(iteration)

		for doc in xrange(numDocs):
			rank[doc] = 0.
			for link in keys[doc]:
				rank[doc] = rank[doc] + rank[link] * M[str(link)+","+str(doc)]

		rank = normalizer(rank)
		

		for node in xrange(numDocs):
			if terminate is True:
				if abs(rank[node] - old_rank[node]) > epsilon:
					terminate = False	

		iteration = iteration + 1		
		if terminate is True:
			print "converged after " + str(iteration) + " iterations"
		old_rank = list(rank)		
		end_iter_time = time.clock()
		print "iteration took " + str((end_iter_time - start_iter_time)/60.) + " minutes"
		# print "saving temp data"
		# saver('rank_temp', rank)
	saver('rank', rank)
	end_time = time.clock()
	print "creating the page ranks took " +str((end_time - start_time)/60.) + " minutes"

	return rank
		

def page_rank_score (	
						similarities,
						pr_val,
						weight = 0.4,						
						verbose = False
					):


	"""compute page rank weighted score"""
	similarities = list(normalizer(similarities))

	for i in xrange(len(pr_val)):
		similarities[i]  = weight  * pr_val[i] + (1-weight) * similarities[i]

	return similarities

# hits algorithm.
def authorities_hubs	(
						numDocs,
				  		nodes,
				  		adj,
				  		maxIter = 20,
				  		epsilon = 1e-7,
					 	verbose = True
		 	  		):
	""" This function calculates authorities and hubs given a set of nodes, root nodes and adjacency matrix """
	if verbose is True:
		print "estimating authorities and hubs"

	authority = list([0.]   * numDocs)
	hub 	  = list([0.]   * numDocs)

	link_adj, citation_adj = adj
	iteration = 0

	terminate = False
	old_authority = list(authority)
	old_hub  = list(hub)

	for node in nodes:
		authority[node] = 1.
		hub[node] 	= 1.

	while iteration < maxIter and terminate is False:

		terminate = True
		auth_norm = 0.
		hub_norm = 0.		
		if verbose is True:
			print "iteration " + str(iteration)

		for node in nodes:
			authority[node] = 0.
			for linkNode in citation_adj[node]:
				authority[node] = authority[node] + hub[linkNode]					
				auth_norm = auth_norm + (authority[node] ** 2)

		for node in nodes:
			hub[node] = 0.
			for linkNode in link_adj[node]:		
				hub[node] = hub[node] + authority[linkNode]				
				hub_norm = hub_norm + (hub[node] ** 2)

		auth_norm = math.sqrt(auth_norm)
		hub_norm  = math.sqrt(hub_norm)

		for node in nodes:	
			authority[node] = authority[node] / auth_norm
			hub[node] = hub[node] / hub_norm

		# check convergence
		for node in nodes:
			if terminate is True:
				if abs(authority[node] - old_authority[node]) > epsilon:
					terminate = False
				if abs(hub[node] - old_hub[node]) > epsilon:
					terminate = False

		if terminate is True and verbose is True:
			print "converged after " + str(iteration+1) + " iterations"

		old_authority = list(authority)
		old_hub  = list(hub) 				
		iteration = iteration + 1

	saver('authorities',authority)
	saver('hubs',hub)

	return authority, hub




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
			print "extracting the inverse document frequency of term: " + term.term().text()
		return math.log(float(self.reader.maxDoc()) / (0.0000001 + self.reader.docFreq(term)))

	def tf (self, verbose = False):
		""" outputs a dictionary of tf. 
		dictionary keys are the document numbers and its value is the tf of the query in the document 
		this is normalized by the document norm if flagged and loaded. 
		""" 
		if verbose is True:
			print "extracting term-frequency of term : " + term.term().text() 
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
			print "extracting term-frequency of term : " + term.term().text() 
		out = []
		for term in self.query:		
			term_idf = self.term_idf(term, verbose = verbose)
			out.append(term_idf)
		return out		

class search(object):
	""" This is the main class that is going to initiate the search engine """ 
	def __init__(   self, 
                    filename, 
                    saver,
                    loader,
                    create_lexicon_flag = False, 
                    tf_idf_flag = True, 
                    ah_flag = True,
                    pr_flag = False,
                    normalize = True, 
                    create_page_rank_flag = False,
                    directory = '../index', 
                    linksFile = "../index/IntLinks.txt",
                    citationsFile = "../index/IntCitations.txt",
                    n_retrieves = 10, 
                    root_set_size = 10,
                    maxIter = 10,
                    verbose = False
                ):
		""" The init function loads up the pickled tf lexicon, normalizers and the norms of all the documents. 
		Also this is the function that setsup the entire class incuding all its flags  """ 

		self.normalize = normalize
		self.tf_idf_flag = tf_idf_flag
		self.ah_flag = ah_flag
		self.pr_flag = pr_flag
		self.n_retrieves = n_retrieves
		self.root_set_size = root_set_size
		self.maxIter = maxIter
		assert self.root_set_size >= self.n_retrieves

		directory = SimpleFSDirectory(File('../index'))	
		self.reader = IndexReader.open(directory)
		self.numDocs = self.reader.maxDoc()

		if self.normalize is False:
			self.norm = None
		    
		# TF and/or TF IDF part.     
		if create_lexicon_flag is True:
			if normalize is True:    # make second flag True if you want to create a normalizer also. 
						             # but assumed created from Create Lexicon file	
				if verbose is True:
					print "extracting all the norms of docs"
					start_time = time.clock()
				self.norm = calculateNormalizer(reader = self.reader, verbose = verbose)
				if verbose is True:                
					end_time = time.clock()
					print "time taken for calculating norms is : " + str(end_time - start_time) + " seconds"
				saver(filename =filename + '_norms', obj = self.norm)					
			self.lexicon = createLexicon(	   	filename = filename,
				 								reader = self.reader,
										       	norm = self.norm if self.normalize else None,
                                                tf_idf_flag = self.tf_idf_flag,   
										       	verbose = verbose)
			saver(filename =filename + '_lexicon', obj = self.lexicon)					

		else:
			self.lexicon = loader(filename + '_lexicon')	
			if normalize is True:
				if verbose is True:
					print "loading norms"
					start_time = time.clock()                
				self.norm = loader(filename = filename +'_norms')	
				if verbose is True:	
					end_time = time.clock()	
					print "time taken for loading norms is : " + str(end_time - start_time) + " seconds"

		# Authorties and Hubs part.
		if self.ah_flag or self.pr_flag is True:
			self.graph = LinkAnalysis(
				                    linksFile       = linksFile,
				                    citationsFile   = citationsFile,
				                    reader          = self.reader,
				                    verbose         = verbose
                				)
			if pr_flag is True:
				if create_page_rank_flag is True:    # make this a create_page_rank_flag 
					self.pr_values = page_rank (
									alpha   = 0.1,
									maxIter = self.maxIter,
									numDocs = self.numDocs,
									graph   = self.graph,
									saver  	= saver,
									verbose = True
								  )	
				else:
					self.pr_values = loader('rank')


	def retrieve(self, pr_weight =0.4, verbose = True):
		""" This function retrieves
		Performs the steps in slide 31 lecture 3 
		"""
		## .. 
		start_time = time.clock()
		sim = [float(0)]*self.reader.maxDoc()

		# initialized all sims to 0
		if verbose is True:
			print "estimating query features"
		q_feat = self.query.tf_idf() if tf_idf_flag is True else self.query.tf()
		i = 0

		if verbose is True:
			print "calculating similarities"

		for term in self.query.query:
			if verbose is True:
				print "calculating similarities for term " + str(term.text())

			if str(term.text()) not in self.lexicon.keys():
				print str(term.text()) + " is not in the index, so skipping it."			
				break

			I = self.lexicon[str(term.text())] # extract the lexicon of the term
			for doc_id, doc_feat in I:    # for every document that carries the term
				sim [int(doc_id)] = sim[int(doc_id)] + q_feat[i] * doc_feat
			i = i + 1

		if self.normalize is True:
			if verbose is True:
				print "normalizing "		
			for doc_id, feat in I:
				sim[int(doc_id)] = sim[int(doc_id)] / self.norm[int(doc_id)]
		
		if verbose is True:
			print "sorting"
		idx = sorted(range(len(sim)), key=lambda k: sim[k], reverse = True)
		end_time = time.clock()
		print "vector spcae time " + str(end_time - start_time) + " seconds"


		if self.ah_flag is True:
			# re rank idx by authorities and hubs and create a new idx
			start_time = time.clock()
			if verbose is True:
				print "estimating root set"
			assert pr_flag is False

			link_adj = {}
			citation_adj = {}

			root_set = list(idx[0:self.root_set_size])
			base_set = list(root_set)

			if verbose is True:
				print "growing base set"	
				count = 0

			for root_node in root_set:
				if verbose is True:
					count = count + 1
					print "growing base for node " + str(count) + " - " + str(root_node)	



				if not root_node in link_adj.keys():
					link_adj[root_node] = list([])
				if not root_node in citation_adj.keys():
					citation_adj[root_node] = list([])

				fwd_links = list(self.graph.getLinks(root_node))				
				if verbose is True:
					print "number of forward links for " + str(root_node) + " is " +str(len(fwd_links))	

				for fwd_link in fwd_links:

					if verbose is True:
						print "processing link " + str(fwd_link)

					if not fwd_link in base_set:
						base_set.append(fwd_link)	

					if not fwd_link in citation_adj.keys():					
						citation_adj[fwd_link] = list([])						
					if not fwd_link in link_adj.keys():
						link_adj[fwd_link] = list([])

					#citation_adj[fwd_link].append(root_node)					
					link_adj[root_node].append(fwd_link) 


				back_links = list(self.graph.getCitations(root_node))
				if verbose is True:
					print "number of backward links for " + str(root_node) + " is " +str(len(back_links))	

				for back_link in back_links:		

					if verbose is True:
						print "processing link " + str(back_link)

					if not back_link in base_set:
						base_set.append(back_link)

					if not back_link in link_adj.keys():
						link_adj[back_link] = list([])
					if not back_link in citation_adj.keys():
						citation_adj[back_link] = list([])

					#link_adj[back_link].append(root_node)
					citation_adj[root_node].append(back_link)

			if verbose is True:
				print "size of base set is " 			+ str(len(base_set))	
				print "size of citation adjacency is "	+ str(len(citation_adj.keys()))	
				print "size of link adjacency is "		+ str(len(link_adj.keys()))

			auth_score, hub_score = authorities_hubs (		
													numDocs = self.numDocs,
				 									adj = (link_adj, citation_adj),
												 	nodes = base_set,
												 	maxIter = self.maxIter,
												 	verbose = False
											  )
			
			auth_idx = sorted(range(len(auth_score)), key=lambda k: auth_score[k], reverse = True)
			hub_idx = sorted(range(len(hub_score)), key=lambda k: hub_score[k], reverse = True)		

			end_time = time.clock()
			print "authorities and hubs time " + str(end_time - start_time) + " seconds"

		elif self.pr_flag is True:
			start_time = time.clock()
			if verbose is True:
				print "estimating root set"
			root_set = list(idx[0:self.root_set_size])
			sim_new = page_rank_score (
											weight = pr_weight,
											similarities = sim,
											pr_val = self.pr_values,
											verbose = verbose	
										)		

			idx_new = sorted(range(len(sim_new)), key=lambda k: sim_new[k], reverse = True)


		if self.ah_flag is True:
			return (idx, sim, auth_idx, auth_score, hub_idx, hub_score)

		elif self.pr_flag is True:
			return (idx, sim, idx_new, sim_new)

		else:
			return (idx, sim) 


	def run(self, query, print_urls = True, pr_weight =0.4, verbose = False):
		"""this function basically runs a query""" 
		self.query = parse_query(query, self.reader)
		start_time = time.clock()

		if self.ah_flag is True:
			doc_ids, score, auth_ids, auth_score, hub_ids, hub_score = self.retrieve(verbose = verbose)
		elif self.pr_flag is True:
			doc_ids, score, pr_ids, pr = self.retrieve(pr_weight = pr_weight, verbose = verbose)
		else:
			doc_ids, score = self.retrieve(verbose = verbose)		

		end_time = time.clock()
		print "took " + str(end_time - start_time) + " seconds to retrieve"
		if print_urls is True:
			print "vector space retreival"
			for i in xrange(self.n_retrieves):
				d = self.reader.document(doc_ids[i])			
				print "doc: [" + str(doc_ids[i]) +"], score: [" + str(score[doc_ids[i]]) +"], url: " + d.getFieldable("path").stringValue().replace("%%", "/")

			if self.ah_flag is True:
				print "authorities based retreival"
				for i in xrange(self.n_retrieves):
					d = self.reader.document(auth_ids[i])			
					print "doc: [" + str(auth_ids[i]) +"], score: [" + str(auth_score[auth_ids[i]]) +"], url: " + d.getFieldable("path").stringValue().replace("%%", "/")

				print "hubs based retreival"
				for i in xrange(self.n_retrieves):
					d = self.reader.document(hub_ids[i])			
					print "doc: [" + str(hub_ids[i]) +"], score: [" + str(hub_score[hub_ids[i]]) +"], url: " + d.getFieldable("path").stringValue().replace("%%", "/")


			elif self.pr_flag is True:
				print "page rank based retreival"
				for i in xrange(self.n_retrieves):
					d = self.reader.document(pr_ids[i])			
					print "doc: [" + str(pr_ids[i]) +"], score: [" + str(pr[pr_ids[i]]) +"], url: " + d.getFieldable("path").stringValue().replace("%%", "/")



		print  "retrieval complete. "
		print  "..........................................................................." 

 

if __name__ == "__main__":

	""" setting up flags in this section """
	verbose = False					# if true prints a lot of stuff.. if false goes a little quiter
	create_lexicon_flag = True  	# if true will rebuild lexicon from scratch, if false will load a pre-created one as supplied in sys_arg[1]
	create_page_rank_flag = True    # same as for create page rank... default load file is 'page_rank' with loader extension
	normalize = True 				# will use document norms and normalized tf-idf, false will not.
	n_retrieves = 10   				# number of documents to retreive
	root_set_size = 10
	tf_idf_flag = True 		    # True retrieves based on Tf/idf, False retrieves based on only Tf. 
	directory = '../index'			# directory of index
	linksFile = "../index/IntLinks.txt"
	citationsFile = "../index/IntCitations.txt"    
	maxIter = 100    

	ah_flag = False
	pr_flag = True

	saver = json_down
	loader = json_up

	if len(sys.argv) < 2:
		filename = 'temp'
	else:
		filename = sys.argv[1]

	print "loading please wait"
	start_time = time.clock()
    
	engine = search (   filename = filename,
                        create_lexicon_flag = create_lexicon_flag, 
						normalize = normalize,
						directory = directory,
						n_retrieves = n_retrieves,
						maxIter = maxIter,
						root_set_size = root_set_size,
						tf_idf_flag = tf_idf_flag,
                        ah_flag = ah_flag,
                        pr_flag = pr_flag,
                        linksFile = linksFile,
                        create_page_rank_flag = create_page_rank_flag,
                        citationsFile = citationsFile,
                        saver = saver,
                        loader = loader,
						verbose = verbose )
	end_time = time.clock()

	if (end_time - start_time) > 60:
		print "finished loading in " + str((end_time - start_time)/60.) + " minutes"
	else:
		print "finished loading in " + str(end_time - start_time) + " seconds"

	query = raw_input("query> ")
	while not query == 'quit':
		engine.run (query = query, print_urls = True, pr_weight = 0.4, verbose = verbose)
		query = raw_input("query> ")