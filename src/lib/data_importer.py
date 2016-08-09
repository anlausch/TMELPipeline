'''
Created on 29.03.2016

@author: anlausch
'''
# for traversing file system
import os
import codecs
import datetime
from lib import database_connection

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
        # traverse file system
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
    
    def read_data(self, database_connection):
        '''
        Reads the data from the file system and inserts it into the database
        '''
        data = dict();
        doc_id = ""
        
        # traverse file system
        for root, dirs, files in os.walk(self.path):  # @UnusedVariable
            for f in files:
                # take first n
                if (len(data) < self.number_docs) or (self.number_docs==-1):
                    path = os.path.join(root, f)
                    content = codecs.open(path, "r", "utf-8", 'ignore').read()
                    doc_id = f
                    # insert content into db
                    database_connection.insert_doc(doc_id, path, content)
                    data[doc_id] = "";
                elif len(data) == self.number_docs:
                    return data
        return data

def main():
    data_importer = DataImporter('C:\\Users\\D056937\\Desktop\\Masterthesis\\EuroParl\\Split_EuroParl_Speeches', 1000, True);
    try:
        data = data_importer.read_data_europarl();
        print(data)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
    
        