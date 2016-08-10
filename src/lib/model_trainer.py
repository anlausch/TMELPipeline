'''
Created on 19.04.2016

@author: anlausch
'''
import subprocess

class ModelTrainer(object):
    '''
    Calls Stanford TMT jar file and runs scala scripts
    '''


    def __init__(self, stanford_tmt_path, llda_script_path):
        '''
        Constructor
        @param {String} stanford_tmt_path, path to stanford topic modeling toolbox
        @param {String} llda_script_path, path to scala script for running llda
        '''
        self.stanford_tmt_path = stanford_tmt_path
        self.llda_script_path = llda_script_path


    def train_llda_model(self):
        '''
        Calls a subprocess, which runs the stanford tmt
        '''
        call = ['java', '-jar', self.stanford_tmt_path, self.llda_script_path]
        try:
            subprocess.call(call)
        except Exception as e:
            print(e)