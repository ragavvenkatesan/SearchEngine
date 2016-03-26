#!/usr/bin/python
import sys, os 
sys.path.append("../lib/lucene-core-3.6.2.jar")
sys.path.append("../lib/lucene-core-3.6.2-javadoc.jar")

from java.io import File
from java.util import Scanner
from org.apache.lucene.index import IndexReader, Term
from org.apache.lucene.store import SimpleFSDirectory
import pdb
 
if __name__ == "__main__":

	r = IndexReader.open(SimpleFSDirectory(File('../index')))
	print "... total number of documents in the index is " + str(r.maxDoc())
	t = r.terms() 
	i = 0
	count_add = 0		
	while t.next():
		i = i + 1 
		if i > 100010: 
			break
		if i > 100000:
			print "[" + str(i) + "]" + t.term().text()

	te = Term("contents","brute")
	print "... number of documents with the word brute is : " +str(r.docFreq(te))
	td = r.termDocs(te)

	while td.next():
		print "... document number " +str(td.doc()) + " contains the term brute " + str(td.freq()) + " time(s)."

	d = r.document(14191);
	url = d.getFieldable("path").stringValue()
	print "url of 14191 : " + url.replace("%%", "/")

	sc = raw_input("... query> ")
	while not sc == 'quit':
		terms = sc.split(" ")
		for word in terms:
			term = Term("contents",word)
			tdocs = r.termDocs(term)
			pdb.set_trace()
			while tdocs.next():
				d_url = r.document(tdocs.doc()).getFieldable("path").stringValue().replace("%%", "/")
				print "["+ str(tdocs.doc()) +"] " + d_url 
		sc = raw_input("... query> ")