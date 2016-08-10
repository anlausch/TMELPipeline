'''
Created on 29.03.2016

@author: anlausch
'''
from mysql.connector import errorcode
import mysql.connector
import codecs
import re

class DatabaseConnection(object):
    '''
    This class manages connection to database
    '''
    def __init__(self, host, database, user, password, create_schema):
        '''
        Constructor;
        Check whether schema setup is required or not
        @param {String} host: host address
        @param {String} database: schema name
        @param {String} user: user name
        @param {String} password: password
        @param {Boolean} create_schema: indicates whether the schema shall be created or not 
        '''
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        try:
            if create_schema == True:
                self.cnx = mysql.connector.connect(user=self.user, 
                                                   password=self.password,
                                                   host=self.host)
                self.cursor = self.cnx.cursor()
                self.create_schema(self.database)
            else:
                self.cnx = mysql.connector.connect(user=self.user,
                                                   database=self.database, 
                                                   password=self.password)
                self.cursor = self.cnx.cursor()
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("[ERROR] Something is wrong with your user name or password")
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                print("[ERROR] Database does not exist")
            else:
                print("[ERROR] %s" % e)
            
            
    def insert_doc(self, doc_id, path, content):
        '''
        Inserts a single document into the db
        @param {String} doc_id: unique identifier of the document
        @param {String} path: path to the document
        @param {String} content: textual content of the document
        '''
        insert_doc = ("INSERT INTO document (id, path, content) VALUES (%s, %s, %s)")
        data_doc = (doc_id, path, content)
        try:
            self.cursor.execute(insert_doc, data_doc)
            self.cnx.commit()
        except Exception as e:
            print("[ERROR] %s" % e)
    
    
    def insert_entity(self, title):
        '''
        Inserts a single entity into the db in case it is not already inserted
        @param {String} title: title of the entity
        '''
        query_entity = ("SELECT count(*) FROM entity WHERE title =%(title)s")
        data_entity = {"title": title}
        row_no = -1
        try:
            self.cursor.execute(query_entity, data_entity)
            for count in self.cursor:
                if count[0] > 0:
                    break;
                else:
                    insert_entity = ("INSERT INTO Entity (title) VALUES (%(title)s)")
                    self.cursor.execute(insert_entity, data_entity)
                    row_no = self.cursor.lastrowid
                    self.cnx.commit()
        except Exception as e:
            print("[ERROR] %s" % e)
        return row_no
    
    
    def insert_snippet(self, content, doc_id):
        '''
        Inserts a single snippet into the db
        @param {String} content: textual content of the snippet
        @param {String} doc_id: identifier of the document the snippet belongs to 
        '''
        insert_snippet = ("INSERT INTO Snippet (content, documentId) VALUES (%s, %s)")
        data_snippet = (content, doc_id)
        snippet_no = -1
        try:
            self.cursor.execute(insert_snippet, data_snippet)
            snippet_no = self.cursor.lastrowid
            self.cnx.commit()
        except Exception as e:
            print("[ERROR] %s" % e)
        return snippet_no
    
    
    def create_tfidf_materialized_view(self):
        '''
        Calculates tf-idf values and stores it in a new table called mv_tfidf
        '''
        try:
            create_view = ("Create table mv_tfidf as "
                              "(Select (tf * LOG(noDocs / df)) as tfIdf, "
                              "LOG(noDocs / df) as idf, noDocs, "
                              "df_t.title as entityTitle, "
                              "df, tf, tf_t.id as documentId, tf_t.content as documentContent " 
                              "from (Select count(*) as noDocs from document) as noDocs_t , "
                              "(Select t.title, count(*) as df " 
                              "from (Select tag.entityTitle as title, document.id " 
                              "from document, snippet, tag "
                              "where "
                              "document.id = snippet.documentId and snippet.id = tag.snippetId "
                              "group by document.id, tag.entityTitle) "
                              "as t group by t.title) as df_t, "
                              "(SELECT document.id, document.content, tag.entityTitle as title, count(*) as tf FROM document, snippet, tag " 
                              "where document.id = snippet.documentId and snippet.id = tag.snippetId "
                              "group by tag.entityTitle, documentId) as tf_t "
                              "where df_t.title = tf_t.title "
                              "order by df_t.title) ")
            self.cursor.execute(create_view)
        except Exception as e:
            print("[ERROR] %s" % e)
            
        
    def insert_tag(self, snippet_id, entity_title, spot, rho):
        '''
        Inserts a single tag into the db
        @param {String} snippet_id: identifier of the snippet in which the entity was found
        @param {String} entity_title: title of the entity found
        @param {String} spot: spot that was identified
        @param {Double} rho: rho value of the tag
        '''
        insert_tag = ("INSERT INTO Tag (snippetId, entityTitle, spot, rho) VALUES (%s, %s, %s, %s)")
        data_tag= (snippet_id, entity_title, spot, rho)
        try:
            self.cursor.execute(insert_tag, data_tag)
            row_no = self.cursor.lastrowid
            self.cnx.commit()
        except Exception as e:
            print("[ERROR] %s" % e)
            row_no = -1
        return row_no
    
    
    def prepare_document_content(self):
        '''
        Prepares the document content for proper output in .csv format such that
        the .csv file can serve as input to llda
        '''
        config = "SET SQL_SAFE_UPDATES = 0"
        self.cursor.execute(config)
        self.cnx.commit()
        update_line_break = "UPDATE document SET content = REPLACE(content, '\n', ' ') WHERE 1=1"
        self.cursor.execute(update_line_break)
        self.cnx.commit()
        update_semicolon = "UPDATE document SET content = REPLACE(content, ';', '') WHERE 1=1"
        self.cursor.execute(update_semicolon)
        self.cnx.commit()
        update_qm = "UPDATE document SET content = REPLACE(content, '\"', '') WHERE 1=1"
        self.cursor.execute(update_qm)
        self.cnx.commit()
        
    
    def export_top_tfidf_entities_per_document_csv(self, no_entities):
        '''
        Exports the top n entites ranked by tf-idf into a .csv file according
        to the format expected by stanford tmt llda
        @param {Integer} no_entities: number of entities that shall be exported
        '''
        try:
            variables = ("SET @current_doc:=0, @doc_rank:=0")
            self.cursor.execute(variables)
            self.cnx.commit()
            export_stmt = ("SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/data.csv' "
                      "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' " 
                      "ESCAPED BY '\\\\' "
                      "LINES TERMINATED BY '\\r\\n' "
                      "FROM "
                      "(SELECT documentId, GROUP_CONCAT(entityTitle SEPARATOR ' '), documentContent "
                      "FROM (SELECT documentId, documentContent, entityTitle, tfIdf "
                      "FROM "
                      "(SELECT documentId, documentContent, entityTitle, tfIdf, " 
                      "@doc_rank := IF(@current_doc = documentId, @doc_rank + 1, 1) AS doc_rank, "
                      "@current_doc := documentId " 
                      "FROM mv_tfidf "
                      "ORDER BY documentId, tfIdf DESC "
                      ") ranked "
                      "WHERE doc_rank <= %(no_entities)s) as top_tfidf_t GROUP BY documentId) as top_tfidf_concat_t")
            export_data = {"no_entities" : no_entities};
            self.cursor.execute(export_stmt, export_data)
            self.cnx.commit()
        except Exception as e:
            print("[ERROR] %s" % e)
    
    
    def insert_data(self, data):
        '''
        Inserts a sequence of documents into the db
        @param {Generator} data: sequence of docs
        '''
        for doc in data:
            self.insert_doc(doc['id'], doc['path'], doc['content'])
            for snippet in doc['snippets']:
                id_snippet = self.insert_snippet(snippet['raw_snippet'], doc['id'])
                for tag in snippet['tagged_snippet']:
                    entity_title = tag['title'].replace("'", "").replace(";", "").replace(" ","_")
                    self.insert_entity(entity_title)
                    self.insert_tag(id_snippet, entity_title, tag['spot'], tag['rho'])
    
    
    def insert_document_topic_distribution(self, data):
        '''
        Inserts document topic distribution into the db
        @param {List} data: sequence of document-topic proportions
        '''
        for distr in data:
            insert_distribution = ("INSERT INTO DocumentTopicDistribution "
                                   "(entityTitle, documentId, fraction) "
                                   "VALUES (%s, %s, %s)")
            data_distribution = (distr['topic'], distr['document'], distr['fraction'])
            try:
                self.cursor.execute(insert_distribution, data_distribution)
                self.cnx.commit()
            except Exception as e:
                print("[ERROR] %s" % e)
    
    
    def exec_sql_file(self, sql_file):
        '''
        Executes an arbitrary .sql file
        @param {String} sql_file: path to the .sql file that shall be executed
        '''
        print ("[INFO] Executing SQL script file: '%s'" % (sql_file))
        statement = ""
        for line in codecs.open(sql_file, "r", "utf8"):
            if re.match(r'--', line):  # ignore sql comment lines
                continue
            if not re.search(r'[^-;]+;', line):  # keep appending lines that don't end in ';'
                statement = statement + line
            else:  # when you get a line ending in ';' then exec statement and reset for next statement
                statement = statement + line
                try:
                    self.cursor.execute(statement)
                except Exception as e:
                    print("[ERROR] %s" % e)
                statement = ""
    
    
    def insert_topic(self, record):
        '''
        Inserts a single topic entry into the db
        @param {Dict} record: record that shall be inserted
        '''
        insert_topic = "INSERT INTO Topic (entitytitle, term, entityweight, termweight) VALUES (%s, %s, %s, %s)"
        data_topic = (record['entity_title'], record['term'], record['entity_weight'], record['term_weight'])
        try:
            self.cursor.execute(insert_topic, data_topic)
            self.cnx.commit()
        except Exception as e:
            print("[ERROR] %s" % e)
            
            
    def create_schema(self, schema_name):
        '''
        Creates the database schema with a given name
        @param {String} schema_name, name of the schema
        '''
        drop_schema = "DROP SCHEMA IF EXISTS %s ;" % schema_name
        create_schema = "CREATE SCHEMA %s DEFAULT CHARACTER SET utf8 ;" % schema_name
        use_schema = "USE %s ;" % schema_name
        try:
            self.cursor.execute(drop_schema)
            self.cnx.commit()
            self.cursor.execute(create_schema)
            self.cnx.commit()
            self.cursor.execute(use_schema)
            self.cnx.database = schema_name
            self.exec_sql_file("./sql/schema.sql")
            print("[INFO] Schema created.")
        except Exception as e:
            print("[ERROR] %s" % e)