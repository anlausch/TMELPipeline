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
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            
            
    
    def insert_doc(self, doc_id, path, content):
        insert_doc = ("INSERT INTO document (id, path, content) VALUES (%s, %s, %s)")
        data_doc = (doc_id, path, content)
        try:
            self.cursor.execute(insert_doc, data_doc)
            self.cnx.commit()
        except Exception as e:
            print(e)
    
    
    def insert_entity(self, title):
        query = ("SELECT count(*) FROM entity "
         "WHERE title ='%s'" % title)
        self.cursor.execute(query)
        for count in self.cursor:
            if count[0] > 0:
                row_no = -1
                break;
            else:
                try:
                    #TODO: Escape SQL
                    insert_entity = ("INSERT INTO Entity "
                           "(title) "
                           "VALUES ('%s')" % title)
                    self.cursor.execute(insert_entity)
                    row_no = self.cursor.lastrowid
                    self.cnx.commit()
                except Exception as e:
                    print(e)
                    print(insert_entity)
        return row_no
    
    
    def insert_snippet(self, content, doc_id):
        insert_snippet = ("INSERT INTO Snippet "
               "(content, documentId) "
               "VALUES (%s, %s)")
        data_snippet = (content, doc_id)
        self.cursor.execute(insert_snippet, data_snippet)
        snippet_no = self.cursor.lastrowid
        self.cnx.commit()
        return snippet_no
    
    def create_tfidf_materialized_view(self):
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
            #data_snippet = (content, email_id)
            self.cursor.execute(create_view)
        except Exception as e:
            print(e)
            print(create_view)
            
        
    def insert_tag(self, snippet_id, entity_title, spot, rho):
        insert_relationship = ("INSERT INTO Tag "
               "(snippetId, entityTitle, spot, rho) "
               "VALUES (%s, %s, %s, %s)")
        data_relationship = (snippet_id, entity_title, spot, rho)
        try:
            self.cursor.execute(insert_relationship, data_relationship)
            row_no = self.cursor.lastrowid
            self.cnx.commit()
        except Exception as e:
            print(insert_relationship, data_relationship)
            print(e)
            row_no = -1
        return row_no
    
    def prepare_email_body(self):
        config = "SET SQL_SAFE_UPDATES = 0"
        self.cursor.execute(config)
        self.cnx.commit()
        update_line_break = "UPDATE emailforensics.email SET body = REPLACE(body, '\n', ' ') WHERE 1=1"
        self.cursor.execute(update_line_break)
        self.cnx.commit()
        update_semicolon = "UPDATE emailforensics.email SET body = REPLACE(body, ';', '') WHERE 1=1"
        self.cursor.execute(update_semicolon)
        self.cnx.commit()
        update_qm = "UPDATE emailforensics.email SET body = REPLACE(body, '\"', '') WHERE 1=1"
        self.cursor.execute(update_qm)
        self.cnx.commit()


    def export_documents_csv(self):
        try:
            export = ("SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/data.csv' "
                        "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
                        "ESCAPED BY '\\\\' "
                        "LINES TERMINATED BY '\\r\\n' "
                        "FROM document")
            self.cursor.execute(export)
            self.cnx.commit()
        except Exception as e:
            print(export)
            print (e)
    
    
    def export_top_tfidf_entities_per_document_csv(self, no):
        try:
            variables = ("SET @current_doc:=0, @doc_rank:=0")
            print(variables)
            self.cursor.execute(variables)
            self.cnx.commit()
            export = ("SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/data.csv' "
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
                      "WHERE doc_rank <= 5) as top_tfidf_t GROUP BY documentId) as top_tfidf_concat_t")
            self.cursor.execute(export)
            self.cnx.commit()
        except Exception as e:
            print(export)
            print (e)
    
    
    def insert_data(self, data):
        for doc in data:
            print(doc)
            self.insert_doc(doc['id'], doc['path'], doc['content'])
            for snippet in doc['snippets']:
                id_snippet = self.insert_snippet(snippet['raw_snippet'], doc['id'])
                for tag in snippet['tagged_snippet']:
                    entity_title = tag['title'].replace("'", "").replace(";", "").replace(" ","_")
                    self.insert_entity(entity_title)
                    self.insert_tag(id_snippet,entity_title, tag['spot'], tag['rho'])
            print("Document inserted")
    
    
    def insert_document_topic_distribution(self, data):
        for distr in data:
            #entity_id = self.insert_entity(distr['topic'])
            
            insert_distribution = ("INSERT INTO DocumentTopicDistribution "
                   "(entityTitle, documentId, fraction) "
                   "VALUES (%s, %s, %s)")
            data_distribution = (distr['topic'], distr['document'], distr['fraction'])
            try:
                self.cursor.execute(insert_distribution, data_distribution)
                self.cnx.commit()
            except Exception as e:
                print(e)
    
    
    def exec_sql_file(self, cursor, sql_file):
        print ("[INFO] Executing SQL script file: '%s'" % (sql_file))
        statement = ""
        for line in codecs.open(sql_file, "r", "utf8"):
            if re.match(r'--', line):  # ignore sql comment lines
                continue
            if not re.search(r'[^-;]+;', line):  # keep appending lines that don't end in ';'
                statement = statement + line
            else:  # when you get a line ending in ';' then exec statement and reset for next statement
                statement = statement + line
                #print "\n\n[DEBUG] Executing SQL statement:\n%s" % (statement)
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print ("[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args)))
    
                statement = ""
    
    def insert_topic(self, record):
        insert_topic = "INSERT INTO Topic (entity_title, term, entityweight, termweight) VALUES (%s, %s, %s, %s)"
        data_topic = (record['entity_title'], record['term'], record['entity_weight'], record['term_weight'])
        try:
            self.cursor.execute(insert_topic, data_topic)
            self.cnx.commit()
        except Exception as e:
            print(e)
            
            
    def create_schema(self, schema_name):
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
            self.exec_sql_file(self.cursor, "./sql/schema.sql")
            print("[INFO] Schema created.")
        except Exception as e:
            print(e)
    
      
def main():
    database_connection = DatabaseConnection("localhost", "anne","root", "root", True)
    database_connection.insert_doc("123", "123", "123")
    print (database_connection.query_doc("123"))
    
if __name__ == "__main__":
    main()