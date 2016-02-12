# SearchEngine
This is the repository for my Information Retrieval course project from ASU Spring 2015 course.

To run the code. First you need to setup jython and install pylucene.
Then run the code as jython SearchEngine.py <lexicon_filename>

Setting up:

	* `verbose = False` will run the code silently.
	* `create_lexicon_flag = True`. if `True` will rebuild lexicon from scratch, if `False` will load a pre-created one as supplied in `sys_arg[1]`
	* `normalize = False`. if `True` will use document norms and normalized tf-idf, `False` will not.
	* `n_retrieves = 10`  number of documents to retreive
	* `tf_idf_flag = True` `True` retrieves based on Tf/idf, False retrieves based on only Tf. 
	* `directory = '../index'` will setup the location of index. 
