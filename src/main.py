'''
Created on 29.03.2016

@author: anlausch

Main module; controls creation of instances and calls functions according to
the intended program flow
'''
import settings as s
from lib.data_importer import DataImporter # @UnresolvedImport
from lib.entity_linker import EntityLinker  # @UnresolvedImport
import sys
#from lib.stats_engine import StatsEngine  # @UnresolvedImport
import time
from lib.database_connection import DatabaseConnection
from lib.model_trainer import ModelTrainer
from lib.result_processor import ResultProcessor 


def main():
    print("Encoding:\n\n %s" % sys.stdout.encoding)
    print("\n\n\n")
    # create instances
    data_importer = DataImporter(s.DATA_PATH, s.NUMBER_DOCS);
    entity_linker = EntityLinker(s.TAGME_URL, s.TAGME_KEY, s.TOKENIZER, 
                                 s.RHO_THRESHOLD)
    model_trainer = ModelTrainer(s.STANFORD_TMT_PATH)
    
    if s.DATA_IS_INSERTED == False:
        print("[INFO] Data is not inserted; Running whole pipeline")
        database_connection = DatabaseConnection(s.DB_HOST, s.DB_SCHEMA_NAME, s.DB_USER, s.DB_PASSWORD, s.CREATE_SCHEMA)
        data = data_importer.gen_read_data();
        data = entity_linker.gen_split_into_sentences(data)
        data = entity_linker.gen_build_snippets(data)
        data = entity_linker.gen_tag_snippets(data)
        database_connection.insert_data(data)
        print(list(data))
        database_connection.prepare_email_body()
        database_connection.create_tfidf_materialized_view()
    else:
        print("Data is already inserted; Running offline")
            
    if s.MODE=="LDA":
        print("Running LDA mode")
        database_connection = DatabaseConnection(s.DB_HOST, s.DB_SCHEMA_NAME, s.DB_USER, s.DB_PASSWORD, False)
        database_connection.export_emails_csv()
        model_trainer.train_lda_model(s.LDA_SCALA_SCRIPT_PATH)
    elif s.MODE =="LLDA":
        print("Running LLDA mode")
        #database_connection.prepare_entity_title()
        database_connection = DatabaseConnection(s.DB_HOST ,s.DB_SCHEMA_NAME, s.DB_USER, s.DB_PASSWORD, False)
        database_connection.export_top_tfidf_entities_per_document_csv(5)
        model_trainer.train_llda_model(s.LLDA_SCALA_SCRIPT_PATH)
        result_processor = ResultProcessor(s.MODE)
        result_processor.link_document_topic(database_connection)
        result_processor.link_topic_term(database_connection)


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
