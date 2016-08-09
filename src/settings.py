'''
Created on 29.03.2016

@author: anlausch

This file contains some general settings (global variables)
'''
DATA_PATH= 'C:\\Users\\D056937\\Desktop\\Masterthesis\\hillary_fede\\output_fede'
TAGME_KEY = 'f13bacba-d11b-4402-9543-eca22cad3601'
TAGME_URL = 'https://tagme.d4science.org/tagme/tag'
TOKENIZER = 'tokenizers/punkt/english.pickle'
# -1 for all emails
NUMBER_DOCS = 10
RHO_THRESHOLD = 0.15

DB_SCHEMA_NAME = "pipeline"
DB_USER = "root"
DB_PASSWORD="root"
DB_HOST="localhost"
CREATE_SCHEMA=True

STANFORD_TMT_PATH="C:/Users/D056937/Desktop/Masterthesis/tools/stanford_tmt/stanford_tmt.jar"
LDA_SCALA_SCRIPT_PATH = "C:/Users/D056937/Desktop/Masterthesis/tools/stanford_tmt/forensic_tm_lda.scala"
LLDA_SCALA_SCRIPT_PATH = "C:/Users/D056937/Desktop/Masterthesis/tools/stanford_tmt/forensic_tm_llda.scala"

MODE="LLDA"
DATA_IS_INSERTED = False

ROOT="C:\\\\Users\\D056937\\Desktop\\workspaces\\sap\\EmailForensics"
