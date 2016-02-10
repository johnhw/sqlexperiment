import sqlite3
import json
import pseudo
import logging
import time
import os

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename="experiment.log")
                    
stream_logger = logging.StreamHandler() 
stream_logger.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logging.getLogger().addHandler(stream_logger)


class DeJSON(object):
    def __init__(self, in_db, out_db):
        # open the database
        self.in_conn = sqlite3.connect(in_db)                
        self.in_cursor = self.in_conn.cursor()
        
        try:
            os.remove(out_db)
        except:
            pass
        
        self.out_conn = sqlite3.connect(out_db)                
        self.out_cursor = self.out_conn.cursor()
        
        # Dump old database in the new one. 
        self.out_conn.executescript("".join(line for line in self.in_conn.iterdump()))
        logging.debug("Dumping old database '%s' to new database '%s'" % (in_db, out_db))
    
        # deJSON the tables
        # tables = {"users": {"keep": ["id", "name", "description", "type"], "json":"json"},
                  # "log_stream": {"keep": ["id", "name", "description", "type"], "json":"json"},
                  # "log_stream": {"keep": ["id", "name", "description", "type"], "json":"json"},
                  # "user_session": {"keep": ["id", "user", "session", "role"], "json":"json"},
                  # "runs":  {"keep" : ["id", "start_time", "end_time", "experimenter", "clean_exit"], "json":"json"},
                  # "children" : {"keep" : ["id", "parent", "child"], "json":None},
                  # "run_session": {"keep" : ["id", "run", "session"], "json":None},
                  # "paths" : {"keep": ["id", "name", "description", "type"], "json":"json"},                                      
                # }
                    
                    
                    
        for table in ["users", "user_session", "runs"]:
            self.dejson(table)       
            
        # DeJSON each log type separately
        log_stream_tables = self.in_cursor.execute("SELECT name FROM log_stream")
        for table in log_stream_tables:
            self.dejson(table[0])
        
        self.out_conn.commit()    
        self.out_conn.close()
        self.in_conn.close()

    def dejson(self, table, column="json"):
        logging.debug("DeJSONing table '%s', using column '%s'" % (table, column))
        query = self.in_cursor.execute("SELECT %s FROM %s" % (column, table))
        columns = {}
        
        # get the datatype of all entries in this column
        for result in query:
            
            row = result[0]
            if row is not None:
                d = json.loads(row)                               
                if type(d) is dict:
                    for key, value in d.iteritems():
                        # each column is made up of strings, ints, floats or JSON
                        code = 'JSON'
                        if type(value) is str:
                            code = "TEXT"
                        if type(value) is int:
                            code = "INTEGER"
                        if type(value) is float:
                            code = "FLOAT"
                        if key not in columns:
                            columns[key] = code
                        else:
                            # mixed data types :(
                            if columns[key] != code:
                                columns[key] = 'MIXED'
                            
        if len(columns)==0:
            return
            
        logging.debug("Detected column types: \n\t\t%s", "\n\t\t".join(("%s: %s" % (k,v)) for k,v in columns.iteritems()))
        
        sql_type = {"FLOAT":"FLOAT", "INTEGER":"INTEGER", "TEXT":"TEXT", "JSON":"TEXT", "MIXED":"TEXT"}
        column_sql = ", ".join(("%s %s" % (k, sql_type[v]) for k,v in columns.iteritems()))
        
        # rename the old table, create the new table and populate it
        #self.out_cursor.execute("ALTER TABLE %s RENAME TO %s_json" % (table, table))
        self.out_cursor.execute("CREATE TABLE %s_ (id INTEGER PRIMARY KEY, %s)" % (table, column_sql))
        query = self.in_cursor.execute("SELECT id,%s FROM %s" % (column, table))
        id = self.in_cursor.lastrowid
        
        # put each JSON entry into the new table
        for result in query:
            row = result[1]
            id = result[0]
            
            if row is not None:            
                d = json.loads(row)
                if type(d) is dict:
                    columns = ", ".join(d.keys())
                    values = ", ".join("?" * (len(d.keys())))                    
                    table_values = []
                    for k,v in d.iteritems():
                        if k=='JSON' or k=='MIXED':
                            table_values.append(json.dumps(v))
                        else:
                            table_values.append(v)
                    self.out_cursor.execute("INSERT INTO %s_ (id, %s) VALUES (%d, %s)" % (table, columns, id, values), d.values())
                
        # create a joint view
        self.out_cursor.execute("CREATE VIEW %s_aux AS SELECT * FROM %s LEFT JOIN %s_ USING(id)" % (table, table, table))
                
                