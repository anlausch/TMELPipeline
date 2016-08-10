'''
Created on 25.04.2016

@author: anlausch
'''
import codecs


class ResultProcessor(object):
    '''
    This class provides post processing of the topic modeling output
    '''
    
    def getTopics(self, line_list, label_list):
        '''
        Returns the corresponding label of every second element (=topic) in a given list starting by 1
        @param {List} line_list, list that contains topic indices and proportions
        @param {List} label_list, label index
        '''
        return [label_list[int(elem)] for elem in line_list[1::2]]
    
    
    def getProportions(self, line_list):
        '''
        Returns every second element (=proportion) in a given list starting by 2
        @param {List} line_list, list that contains topic indices and proportions
        '''
        return [float(elem) for elem in line_list[2::2]]
    
    
    def link_document_topic(self, database_connection):
        '''
        Links documents and topic labels by processing the label-index.txt 
        and document-topic-distributions.csv output files of the stanford tmt
        and inserts the information into the database
        @param {DatabaseConnection} database_connection, connection to the db
        '''
        label_index = [line.replace("\n","") for line in open(".\\..\\output\\llda-output\\01000\\label-index.txt").readlines()]
        with open(".\\..\\output\\llda-output\\document-topic-distributions.csv", "r") as f:
            for line in f:
                line_array = line.split(",")
                document = line_array[0]
                topics = self.getTopics(line_array, label_index)
                proportions = self.getProportions(line_array)
                topic_distribution = [dict({"document": document, "topic": topics[i], "fraction": proportions[i]}) for i in range(0, len(topics))]
                database_connection.insert_document_topic_distribution(topic_distribution)
    
    
    def link_topic_term(self, database_connection):
        '''
        Links topics (=entities) and terms by processing the summary.txt output file 
        of the stanford tmt and inserts the information into the database
        @param {DatabaseConnection} database_connection, connection to the db
        '''
        file_lines = codecs.open(".\\..\\output\\llda-output\\01000\\summary.txt", "r", "utf-8").readlines()
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