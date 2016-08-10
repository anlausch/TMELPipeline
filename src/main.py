'''
Created on 29.03.2016

@author: anlausch

Main module; controls creation of instances and calls functions according to
the intended program flow
'''
import settings as s
from lib.data_importer import DataImporter # @UnresolvedImport
from lib.entity_linker import EntityLinker  # @UnresolvedImport
from lib.database_connection import DatabaseConnection
from lib.model_trainer import ModelTrainer
from lib.result_processor import ResultProcessor
import time


def main():
    # create instances
    data_importer = DataImporter(s.DATA_PATH, s.NUMBER_DOCS);
    entity_linker = EntityLinker(s.TAGME_URL, s.TAGME_KEY, s.TOKENIZER, s.RHO_THRESHOLD)
    model_trainer = ModelTrainer(s.STANFORD_TMT_PATH, s.LLDA_SCRIPT_PATH)
    
    # if data is not inserted into the db yet, the whole pipeline is executed
    if s.DATA_IS_INSERTED == False:
        print("[INFO] Data is not inserted; Running whole pipeline")
        database_connection = DatabaseConnection(s.DB_HOST, s.DB_SCHEMA_NAME, s.DB_USER, s.DB_PASSWORD, s.CREATE_SCHEMA)
        data = data_importer.gen_read_data();
        data = entity_linker.gen_split_into_sentences(data)
        data = entity_linker.gen_build_snippets(data)
        data = entity_linker.gen_tag_snippets(data)
        database_connection.insert_data(data)
        print("[INFO] Data inserted")
        database_connection.prepare_document_content()
        database_connection.create_tfidf_materialized_view()
    else:
        print("[INFO] Data is already inserted; Running offline")

    database_connection = DatabaseConnection(s.DB_HOST ,s.DB_SCHEMA_NAME, s.DB_USER, s.DB_PASSWORD, False)
    database_connection.export_top_tfidf_entities_per_document_csv(5)
    print("[INFO] Data exported")
    model_trainer.train_llda_model()
    print("[INFO] L-LDA applied")
    result_processor = ResultProcessor()
    result_processor.link_document_topic(database_connection)
    result_processor.link_topic_term(database_connection)


if __name__ == "__main__":
    print("[INFO] Pipeline started")
    start_time = time.time()
    main()
    print("[INFO] Total processing time: %s seconds" % (time.time() - start_time))
