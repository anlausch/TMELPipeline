'''
Created on 29.03.2016

@author: anlausch
'''
# for tokenization
import nltk.data
import sys
# for http requests
import requests

class EntityLinker(object):
    '''
    This class is for tagging all documents 
    with wikipedia entities found in the text
    '''
    

    def __init__(self, url, key, tokenizer, rho_threshold):
        '''
        Constructor
        '''
        self.url = url
        self.key = key
        self.tokenizer = tokenizer
        self.rho_threshold = rho_threshold
        self.no_emails_to_tag = 1000
        self.no_emails_tagged = 0
    
    
    def gen_split_into_sentences(self, data):
        sentence_detector = nltk.data.load(self.tokenizer)
        for doc in data:
            sentences = sentence_detector.tokenize(doc['content'])
            yield dict([('id', doc['id']), ('content', doc['content']),
                                            ('sentences', sentences),
                                            ('path', doc['path'])])


    def gen_build_snippets(self,data):
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
            yield dict([('id', doc['id']), ('content', doc['content']),
                                            ('sentences', doc['sentences']), ('snippets', snippet_list),
                                            ('path', doc['path'])])
            
                
    
    
    def gen_tag_snippets(self, data):
        proxies = {
                   'http': 'http://proxy.wdf.sap.corp:8080',
                   'https': 'http://proxy.wdf.sap.corp:8080',
                   }
        default = ''
        for doc in data:
            if self.no_emails_tagged < self.no_emails_to_tag:
                tagged_snippets = []
                for snippet in doc['snippets']:
                    tags = []
                    try:
                        payload = {'gcube-token': 'f13bacba-d11b-4402-9543-eca22cad3601', 'text': snippet, 'long_text' : 0}
                        r = requests.post(self.url, data=payload, proxies=proxies)
                        if r.status_code == requests.codes.ok and r is not None and len(r.text) > 0:        # @UndefinedVariable
                            r_json = r.json()
                            for annotation in r_json['annotations']:
                                if float(annotation['rho']) >= self.rho_threshold:
                                    tags.append(dict([('spot', 
                                                       annotation.setdefault('spot', default)), 
                                                      ('title', annotation.setdefault('title', default)), 
                                                      ('rho', annotation.setdefault('rho', default))]))
                            tagged_snippets.append(dict([('raw_snippet', snippet), 
                                                          ('tagged_snippet', tags)]))
                            if len(tags) >=10:
                                self.no_emails_tagged += 1
                    except Exception as e:
                        print("------------", e)
                        print(payload)
                        print(sys.getsizeof(snippet))
                        print(doc['path'])
                yield dict([('id', doc['id']), ('content', doc['content']),
                                                ('snippets', tagged_snippets),
                                                ('sentences', doc['sentences']),
                                                ('path', doc['path'])])
            else:
                return
        return    
    
    
    def tag_sentences(self, data):
        default = ''
        for key in data:
            tagged_sentences = []
            for sentence in data[key]['sentences']:
                tags = []
                try:
                    r = requests.get(self.url + '?gcube-token=' + self.key 
                                 + '&text=' + sentence)
                except Exception as e:
                    print("------------", e)
                    print(data[key]['filename'])
                if r.status_code == requests.codes.ok and r is not None and len(r.text) > 0:        # @UndefinedVariable
                    r_json = r.json()
                    for annotation in r_json['annotations']:
                        if float(annotation['rho']) >= self.rho_threshold:
                            tags.append(dict([('spot', 
                                               annotation.setdefault('spot', default)), 
                                              ('title', annotation.setdefault('title', default)), 
                                              ('rho', annotation.setdefault('rho', default))]))
                    tagged_sentences.append(dict([('raw_sentence', sentence), 
                                                  ('tagged_sentence', tags)]))
            data[key] = dict([('body', data[key]['body']),
                                            ('sentences', tagged_sentences),
                                            ('filename', data[key]['filename']), ('original', data[key]['original']), ('timestamp', data[key]['timestamp'])])
        return data    