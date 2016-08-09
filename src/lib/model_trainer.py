'''
Created on 19.04.2016

@author: anlausch
'''
import subprocess

class ModelTrainer(object):
    '''
    Calls Stanford TMT jar file and runs scala scripts
    '''


    def __init__(self, stanford_tmt_path):
        '''
        Constructor
        '''
        self.stanford_tmt_path = stanford_tmt_path
     
     
    def train_lda_model(self, lda_scala_script_path):
        call = ['java', '-jar', self.stanford_tmt_path, lda_scala_script_path]
        try:
            subprocess.call(call)
        except Exception as e:
            print(e)
            print(call)


    def train_llda_model(self, llda_scala_script_path):
        call = ['java', '-jar', self.stanford_tmt_path, llda_scala_script_path]
        try:
            subprocess.call(call)
        except Exception as e:
            print(e)
            print(call)   