'''
Created on 29.03.2016

@author: anlausch
'''
import os
import codecs

class DataImporter(object):
    '''
    This class is for importing textual data from the file system
    '''


    def __init__(self, path, number_docs):
        '''
        Constructor
        @param {String} path: path to the directory
        @param {Integer} number_docs: number of docs to be imported 
        '''
        self.path = path
        self.number_docs = number_docs
    
    
    def gen_read_data(self):
        '''
        Generator that reads data from the file system and yields single docs
        '''
        data = dict();
        for root, dirs, files in os.walk(self.path):  # @UnusedVariable
            for f in files:
                # take first n
                if (len(data) < self.number_docs) or (self.number_docs==-1):
                    path = os.path.join(root, f)
                    content = codecs.open(path, "r", "utf-8", 'ignore').read()
                    doc_id = f
                    # insert content into db
                    yield dict([("id", doc_id),("path", path),("content", content)])
                    data[doc_id] = "";
                elif len(data) == self.number_docs:
                    return
        return