# SearchEngine
This is the repository for my Information Retrieval course project from ASU Spring 2015 course.
It was implemented progressively. 

update 0: TF-IDF based retrieval

update 1: added options for Authorites and Hubs

update 2: added options for page rank   

To run the code. First you need to setup jython and install pylucene.
Then run the code as `jython SearchEngine.py <lexicon_filename>`

The code `CreateLexicon.py` running as `jython CreateLexicon.py <lexicon_filname>` will just create an index and save it down in a `.pkl.gz` file. This also can save document norms pre-saved. 
The code `SearchFiles.py` is the re-writing of the code `SearchFiles.java` into python.
The code `LinkAnalysis.py` is the re-writing of the code `LinkAnalysis.java` into python. This can be used to understand how `pylucene` works. 
These can be used to understand how `pylucene` works. 

Setting up SearchFiles.py

- `verbose = False` will run the code silently.
- `create_lexicon_flag = True`. if `True` will rebuild lexicon from scratch, if `False` will load a pre-created one as supplied in `sys_arg[1]`
- `normalize = False`. if `True` will use document norms and normalized tf-idf, `False` will not.
- `n_retrieves = 10`  number of documents to retreive
- `tf_idf_flag = True` `True` retrieves based on Tf/idf, False retrieves based on only Tf.     
- `ah_flag = True` will run the code using Authorites and Hubs weighting.
- `pr_flag = True` will run the code using Page Rank weighting.  
- `directory = '../index'` will setup the location of index.    
- having both the above `False` will simply run just TF-IDF. 
- `linksFile` and `citationsFile` are text files containing links and citations needed for HITS and pagerank algorithms.
- `root_set_size` is a variable that determines the root set for hits algorithm.
- `maxIter` determines how many iterations for power iterating to get eigen values.   
