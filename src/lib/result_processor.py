'''
Created on 25.04.2016

@author: anlausch
'''
from lib.database_connection import DatabaseConnection
import codecs
import settings as s


class ResultProcessor(object):
    '''
    This class provides post processing of the topic modeling output
    '''


    def __init__(self):
        '''
        Constructor
        '''
    
    def getTopics(self, a, labels):
        return [labels[int(elem)] for elem in a[1::2]]
    
    
    def getProportions(self, a):
        return [float(elem) for elem in a[2::2]]
    
    
    def link_document_topic(self, database_connection):
        label_index = [line.replace("\n","") for line in open(s.ROOT + "\\output\\llda-output\\01000\\label-index.txt").readlines()]
        print(label_index)
        with open(s.ROOT + "\\output\\llda-output\\document-topic-distributions.csv", "r") as f:
            for line in f:
                line_array = line.split(",")
                document = line_array[0]
                topics = self.getTopics(line_array, label_index)
                proportions = self.getProportions(line_array)
                topic_distribution = [dict({"document": document, "topic": topics[i], "fraction": proportions[i]}) for i in range(0, len(topics))]
                print(topic_distribution)
                database_connection.insert_document_topic_distribution(topic_distribution)
    
    
    def link_topic_term(self, database_connection):
        file_lines = codecs.open(".\\..\\..\\output\\llda-output\\01000\\summary.txt", "r", "utf-8").readlines()
        is_label = True;
        for line in file_lines:
            if is_label == True and (line != "\n" and line != "\r\n"):
                label, label_weight = line.strip().split()
                is_label = False
            elif is_label == False and (line != "\n" and line != "\r\n"):
                term, term_weight = line.strip().split()
                record = dict([('entity_title', label), ('entity_weight', label_weight), ('term', term), ('term_weight', term_weight)])
                database_connection.insert_topic(record)
            elif line == "\n" or line == "\r\n":
                is_label = True
    
    
def main():
    rp = ResultProcessor("LLDA")
    dc = DatabaseConnection("EmailForensics", "root", "root")
    rp.insert_topics(dc) 

if __name__ == "__main__":
    main()