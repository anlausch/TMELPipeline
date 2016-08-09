'''
Created on 01.04.2016

@author: anlausch
'''
import ast
import math
from collections import Counter

class StatsEngine(object):
    '''
    This class is responsible for calculating stats like tf-idf
    '''


    def __init__(self):
        '''
        Constructor
        '''
    
    def tf(self, data):
        for email in data:
            entities = []
            for sentence in email['sentences']:
                for tagged_sentence in sentence['tagged_sentence']:
                    entities.append(tagged_sentence['title'])
            counts = Counter(entities)
            data[data.index(email)] = dict([('body', email['body']),
                                            ('sentences', email['sentences']), 
                                            ('entity_stats', counts),
                                            ('filename', email['filename'])])
        return data
    

    def n_containing(self, data, entity):
        return sum(1 for email in data if entity in email['entity_stats'])

    def idf(self, data, entity):
        return math.log(len(data) / self.n_containing(data, entity))

    def tfidf(self, data, entity, tf):
        idf = self.idf(data, entity)
        return tf * idf, idf
    
    def calculate_stats(self, data):
        data = self.tf(data)
        for email in data:
            entity_stats = []
            for entity in email['entity_stats']:
                tf = email['entity_stats'][entity]
                tfidf, idf = self.tfidf(data, entity, tf)
                entity_stats.append(dict([('entity', entity),('tf', tf), 
                                          ('tfidf', tfidf), ('idf', idf)]));
            data[data.index(email)] = dict([('body', email['body']),
                                            ('sentences', email['sentences']), 
                                            ('entity_stats', entity_stats),
                                            ('filename', email['filename'])])
        return data
    
def main():
    stats_engine = StatsEngine()
    data = ast.literal_eval(open("./../example_entity_linker_output.txt", "r").read())
    data = stats_engine.tf(data)
    print(data)
    for email in data:
        entity_stats = []
        for entity in email['entity_stats']:
            tf = email['entity_stats'][entity]
            tfidf, idf = stats_engine.tfidf(data, entity, tf)
            entity_stats.append(dict([('entity', entity),('tf', tf), 
                                      ('tfidf', tfidf), ('idf', idf)]));
        data[data.index(email)] = dict([('body', email['body']),
                                        ('sentences', email['sentences']), 
                                        ('entity_stats', entity_stats),
                                        ('filename', email['filename'])])
    print(data)
    print()
    print()
    print("max tf-idf entities for each email:")
    for email in data:
        max_tfidf_entity = max(email['entity_stats'], key=lambda x:x['tfidf'])
        print(max_tfidf_entity)

if __name__ == "__main__":
    main()