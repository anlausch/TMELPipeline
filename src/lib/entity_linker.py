'''
Created on 29.03.2016

@author: anlausch
'''
import nltk.data
import sys
import requests

class EntityLinker(object):
    '''
    This class is for tagging all documents 
    with wikipedia entities found in the text
    '''
    

    def __init__(self, url, key, tokenizer, rho_threshold):
        '''
        Constructor
        @param {String} url, url to tagme api
        @param {String} key, authorization token
        @param {String} tokenizer, path to tokenizer in a certain language
        @param {Double} rho_threshold, threshold for rho
        '''
        self.url = url
        self.key = key
        self.tokenizer = tokenizer
        self.rho_threshold = rho_threshold
    
    
    def gen_split_into_sentences(self, data):
        '''
        Generator that uses nltk for splitting textual data into sentences
        @param {Generator} data, sequence of docs
        '''
        sentence_detector = nltk.data.load(self.tokenizer)
        for doc in data:
            sentences = sentence_detector.tokenize(doc['content'])
            yield dict([('id', doc['id']), 
                        ('content', doc['content']),
                        ('sentences', sentences),
                        ('path', doc['path'])])


    def gen_build_snippets(self,data):
        '''
        Generator that builds snippets out of the sentences in a doc,
        such that the api can handle the amount of data
        @param {Generator} data, sequence of docs
        '''
        for doc in data:
            snippet_list = []
            snippet = ""
            if sys.getsizeof(doc['content']) < 4194000:
                snippet = doc['content']
            else:
                for sentence in doc['sentences']:
                    if sys.getsizeof(snippet+sentence) < 4194000:
                        snippet += sentence
                    else:
                        snippet_list.append(snippet)
                        snippet = "";
            snippet_list.append(snippet)
            yield dict([('id', doc['id']), 
                        ('content', doc['content']),
                        ('sentences', doc['sentences']),
                        ('snippets', snippet_list),
                        ('path', doc['path'])])
            

    def gen_tag_snippets(self, data):
        '''
        Generator that sends textual snippets to the api of the entity linker
        and adds the result to the data structure if the rho is above a given
        threshold
        @param {Generator} data, sequence of docs
        '''
        default = ''
        for doc in data:
            tagged_snippets = []
            for snippet in doc['snippets']:
                tags = []
                try:
                    payload = {'gcube-token': self.key, 'text': snippet, 'long_text' : 0}
                    r = requests.post(self.url, data=payload)
                    if r.status_code == requests.codes.ok and r is not None and len(r.text) > 0:        # @UndefinedVariable
                        r_json = r.json()
                        for annotation in r_json['annotations']:
                            if float(annotation['rho']) >= self.rho_threshold:
                                tags.append(dict([('spot', annotation.setdefault('spot', default)), 
                                                  ('title', annotation.setdefault('title', default)), 
                                                  ('rho', annotation.setdefault('rho', default))]))
                        tagged_snippets.append(dict([('raw_snippet', snippet), 
                                                      ('tagged_snippet', tags)]))
                except Exception as e:
                    print("------------", e)
                    print(payload)
                    print(doc['path'])
            yield dict([('id', doc['id']), 
                        ('content', doc['content']),
                        ('snippets', tagged_snippets),
                        ('sentences', doc['sentences']),
                        ('path', doc['path'])])