#!/usr/bin/python
import sys 
import time 

import functools
from javax.swing import JButton, JFrame, JPanel, JTextField, JLabel
from java.awt import Component, GridLayout
from java.net import URL
from java.lang import Runnable

from SearchEngine import search, parse_query
from CreateLexicon import json_up, json_down

class search_gui(search):

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
        frame = JFrame('Ragav\'s Search Engine',
                        defaultCloseOperation = JFrame.EXIT_ON_CLOSE,
                        size = (800, 800)
                        )  
        panel = JPanel(GridLayout(0,2))
        frame.add(panel)                           

        print "in total  " + str(end_time - start_time) + " seconds for retrieval"
        
        if print_urls is True:
            panel.add ( JLabel("vector space retreival" ) )
            for i in xrange(self.n_retrieves):
                d = self.reader.document(doc_ids[i])			
                panel.add ( JLabel (d.getFieldable("path").stringValue().replace("%%", "/") ) )

            if self.ah_flag is True:
                panel.add ( Jlabel("authorities based retreival" ) )
                for i in xrange(self.n_retrieves):
                    d = self.reader.document(auth_ids[i])			
                    panel.add (  JLabel (d.getFieldable("path").stringValue().replace("%%", "/") ) )

                panel.add ( JLabel("hubs based retreival" ) )
                for i in xrange(self.n_retrieves):
                    d = self.reader.document(hub_ids[i])			
                    panel.add (  JLabel  ( d.getFieldable("path").stringValue().replace("%%", "/") ) )


            elif self.pr_flag is True:
                panel.add ( JLabel("page rank based retreival" ) )
                for i in xrange(self.n_retrieves):
                    d = self.reader.document(pr_ids[i])			
                    panel.add ( JLabel ( d.getFieldable("path").stringValue().replace("%%", "/") ) )


        print  "retrieval complete. "
        print  "..........................................................................." 
        frame.visible = True            
        return d 

class gui_run (object):

    def __init__(self):
        """ setting up flags in this section """
        verbose = False					# if true prints a lot of stuff.. if false goes a little quiter
        create_lexicon_flag = True  	# if true will rebuild lexicon from scratch, if false will load a pre-created one as supplied in sys_arg[1]
        create_page_rank_flag = False    # same as for create page rank... default load file is 'page_rank' with loader extension
        normalize = False 				# will use document norms and normalized tf-idf, false will not.
        n_retrieves = 50   				# number of documents to retreive
        root_set_size = 50
        tf_idf_flag = False 		    # True retrieves based on Tf/idf, False retrieves based on only Tf. 
        directory = '../index'			# directory of index
        linksFile = "../index/IntLinks.txt"
        citationsFile = "../index/IntCitations.txt"    
        maxIter = 100    

        ah_flag = False
        pr_flag = False

        saver = json_down
        loader = json_up

        cluster_results = False
        num_clusters = 3


        if len(sys.argv) < 2:
            filename = 'temp'
        else:
            filename = sys.argv[1]

        frame = JFrame('Ragav\'s Search Engine',
                defaultCloseOperation = JFrame.EXIT_ON_CLOSE,
                size = (100, 100)
            )        
        panel = JPanel(GridLayout(0,2)) 
        frame.add(panel)           
        panel.add (JLabel("Loading please wait ... "))    
        frame.pack()
        frame.visible = True 
            
        start_time = time.clock()

        self.engine = search_gui ( 
                            filename = filename,
                            create_lexicon_flag = create_lexicon_flag, 
                            normalize = normalize,
                            directory = directory,
                            n_retrieves = n_retrieves,
                            cluster_results = cluster_results,
                            num_clusters = num_clusters,
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

        frame.visible = False
        if (end_time - start_time) > 60:
            message =  "finished loading in " + str((end_time - start_time)/60.) + " minutes" + "\n"
        else:
            message =  "finished loading in " + str(end_time - start_time) + " seconds" + "\n"
        
        print message
        
        frame = JFrame('Ragav\'s Search Engine',
                defaultCloseOperation = JFrame.EXIT_ON_CLOSE,
                size = (500, 50)
            )  
        panel = JPanel(GridLayout(0,2))
        frame.add(panel)                           
        self.query = JTextField('', 30)
        panel.add(self.query)       
        searchButton = JButton('Search', actionPerformed = self.run)
        panel.add(searchButton)    
        frame.visible = True   
                    
    def run(self, event):
        print "query : " + self.query.text 
        d = self.engine.run (query = self.query.text, print_urls = True, pr_weight = 0.4, verbose = False)        
        
if __name__ == "__main__":
    gui_run()
