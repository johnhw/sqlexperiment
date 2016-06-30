import json
import logging
import os
from collections import defaultdict
import pandas as pd
import base64


def get_logs(cursor, run=None):
    if run is None:
        logs = cursor.execute("SELECT record FROM logging").fetchall()
    else:        
        logs = cursor.execute("SELECT record FROM logging WHERE run=?", str(run)).fetchall()
    return "\n".join([l[0] for l in logs])
    

def dump_json(cursor, file):
    """Dump the **entire** database to a JSON file. This is intended where the DB needs to be archived in a text format.
    
    The JSON has a dictionary of table names -> table data.
    
    Each table data has two entries:
        schema: giving the schema as a JSON column:type dictionary
        rows: The table data as a list of column:value dictionaries
        
    {
        <table>: { 
            "schema":{<column1>:<type1>, ... }
            "row":[{<column1>:<value1>, <column2>:<value2>, ... },  {<column1>:<value1>, <column2>:<value2>, ... }]
            }
    }
    
    Data is recorded in native format, except for BLOBs which are written as base64 encoded strings.    
    
    """    
    # find all tables
    tables = cursor.execute("SELECT * FROM sqlite_master WHERE type='table'").fetchall()
    file.write('{\n')

    first_table = True
    for table in tables:
        name =  table[1]
        # find all columns
        info = cursor.execute("PRAGMA table_info(%s)"%name).fetchall()
        
        if not first_table:
            file.write(",\n")
        first_table = False
                
        column_names = [i[1] for i in info]
        column_types = [i[2] for i in info]
        column_dict = {name:type for name,type in zip(column_names, column_types)}
        
        
        result = cursor.execute("SELECT * FROM %s"%name)
        file.write('"%s":{\n' % name)
        file.write('"schema": %s,\n' % (json.dumps(column_dict)))
        file.write('"rows": [\n')
        first = True
        # write out each row as a JSON dictionary column:value
        for row in result:
            r = result.fetchone()
            
            if r is not None:
                if not first:
                    file.write(",\n")
                first = False
                col_dict = {column:value for column,value in zip(column_names,r)}
                # write binary blobs as base64 encoded strings
                for col in col_dict:
                    if column_dict[col] == 'BLOB':                                            
                        col_dict[col] = base64.b64encode(col_dict[col])
                        
                # dump this row
                file.write(json.dumps(col_dict))
                                               
        file.write("\n]\n}")            
    file.write("}")
    
       

def json_columns(json_seq):
    # get the datatype of all entries in this column
    columns = {}    
    for result in json_seq:                        
        if result is not None:           
                for key, value in result.iteritems():
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
    return columns    

def dejson(in_db, out_db_file):
        """Convert the entire dataset to a new database. 
     This database:
        splits each log stream into a separate table and de-jsons the columns        
        creates views for each metadata type        
        
        Copies the runs, sync_ext, run_session, binary, session, meta and session_meta tables as is.
        """
        out_db = sqlite3.connect(out_db_file)        
        out_cursor = sqlite3.cursor(out_db)
        out_cursor.execute('ATTACH DATABASE "%s" as in_db' % in_db)
                
        # copy existing tables
        for table in ["runs", "sync_ext", "run_session", "binary", "session", "session_meta", "meta"]:
            out_cursor.execute("CREATE TABLE %s AS SELECT * FROM in_db.%s" % (table, table))
            
        # create views for each type of metadata
        mtypes = out_cursor.execute("SELECT UNIQUE(mtype) FROM meta").fetchall()
        for mtype in mtypes:            
            out_cursor.execute("CREATE VIEW %s AS SELECT * FROM meta WHERE mtype=%s" % (mtype[0].lower(), mtype[0]))
        
        
        # create views for each type of metadata
        log_id_types = out_cursor.execute("SELECT UNIQUE(stream) FROM in_db.log").fetchall()
        
        
        # now create the various sub-tables
        for log_id in log_id_types:
            table_name = out_cursor.execute("SELECT name FROM meta WHERE id=?", (log_id,)).fetchone()[0]
          
            
            # create the new table
            out_cursor.execute("""
                    CREATE TABLE %s (id INTEGER PRIMARY KEY, session INT, valid INT, time REAL, tag TEXT, binary INT,                    
                    FOREIGN KEY(session) REFERENCES session(id),
                    FOREIGN KEY(binary) REFERENCES binary(id))""" % (table_name, json_columns))
            
            # copy the existing data
            rows = out_cursor.execute("SELECT session, valid, time, tag, binary, json FROM in_db.log WHERE stream=?", log_id)            
            
            json_cols = {}
            for row in rows:
                r = row.fetchone()
                if r:
                    out_cursor.execute("INSERT INTO %s (session,valid,time,tag,binary) VALUES (?,?,?,?,?)", (r[0], r[1], r[2], r[3], r[4]))
                dejsond = json.loads(r[5])
                for col, val in dejsond.iteritems():
                    if not col in json_cols:                                            
                        out_cursor.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, val, type))
                out_cursor.execute("INSERT INTO %s (%s) VALUES (%s)" % (table, col))
                
            
            
        split_and_dejson("log", split="stream", dejson="json")
        
    
        
        
    
    
class AutoVivification(dict):    
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value



def dump(cursor):    
    c = cursor
    all = AutoVivification()    
    paths = c.execute("SELECT DISTINCT(path) FROM session").fetchall()    
    for path in paths:
        sessions = c.execute("SELECT id, valid FROM session WHERE path=?", path).fetchall()            
        for session, svalid in sessions:                
            rows = c.execute("SELECT log.stream,log.time,log.json,log.valid,stream.name FROM log JOIN stream ON stream.id=log.stream WHERE session=? ", (session,)).fetchall()
            frame = defaultdict(list)
            for stream, time, js, valid, stream_name in rows:
                d = json.loads(js)
                d['t'] = time
                d['valid'] = valid                
                d['session_valid'] = svalid
                frame[stream_name].append(d)
            all[path[0]][session[0]] = frame
    return all

import csv    
def to_csv(cursor):
    structure = dump(cursor)
    for path,session in structure.iteritems():        
        path = path.strip('/') # make sure we don't accidentally write to root!
        try:
            os.makedirs(path)
        except OSError, e:
            logging.debug("Could not create directory %s (%s)"  % (path, e))
            
        # each path has a directory; each entry has a directory within that
        for session_k,streams in session.iteritems():
            fullpath = os.path.join(path, str(session_k))
            try:
                os.makedirs(fullpath)
            except OSError, e:
                logging.debug("Could not create directory %s (%s)"  % (path, e))
                
            # write out each stream independently
            for stream_name, stream_data in streams.iteritems():
                
                with open(os.path.join(fullpath,"%s.csv" % (stream_name)), 'w') as f:
                    csvfile = csv.DictWriter(f, delimiter=",", fieldnames=stream_data[0].keys())
                    csvfile.writeheader()
                    for s in stream_data:
                        csvfile.writerow(s)

def dumpflat(cursor):
    """Return a dictionary of stream entries for the **whole** dataset. Each entry has the t, valid, path, and session fields filled in,
    along with the columns stored in the JSON entries"""
    c = cursor
    rows = c.execute("SELECT log.stream,log.time,log.json,log.valid,stream.name,log.session,session.path,session.valid FROM log JOIN stream ON stream.id=log.stream JOIN session on log.session=session.id").fetchall()
    frame = defaultdict(list)
    for stream, time, js, valid, stream_name,session,path,svalid in rows:
        d = json.loads(js)
        d['t'] = time
        d['valid'] = valid                
        d['session_valid'] = svalid
        d['path'] = path
        d['session'] = session
        frame[stream_name].append(d)
    return frame
    
    
def dump_flat_dataframe(cursor):    
    frame = dumpflat(cursor)
    dfs = {}
    for key, df in frame.iteritems():
        columns = json_columns(df)
        dfs[key] = pd.DataFrame(df, columns=columns.keys())
    return dfs
    

def to_csv_flat(cursor, csvdir):
    """Write each stream type to an individual CSV file in the given directory, in the same format as dumpflat() does"""
    streams = dumpflat(cursor)    
    # write out each stream independently
    for stream_name, stream_data in streams.iteritems():                
        with open(os.path.join(csvdir,"%s.csv" % (stream_name)), 'w') as f:            
            csvfile = csv.DictWriter(f, delimiter=",", fieldnames=stream_data[0].keys())
            csvfile.writeheader()
            for s in stream_data:
                csvfile.writerow(s)

    
def dump_sessions(cursor):    
    c = cursor
    sessions = {}
    ss = c.execute("SELECT id, start_time, end_time, test_run, random_seed, valid, complete, description, json, subcount, parent, path, name FROM session").fetchall()        
    for session in ss:
        last_time, count = c.execute("SELECT max(log.time), count(log.id) FROM log JOIN session ON session.id=log.session WHERE session.id=?", (session[0],)).fetchone()
        sessions[session[0]] = dict(id=session[0], start_time=session[1], end_time=session[2],
        test_run=session[3], random_seed=session[4], valid=session[5],
        complete=session[6], description=session[7], json=json.loads(session[8] or 'null'),
        subcount=session[9], parent=session[10], path=session[11], name=session[12], log_count=count, last_time=last_time)
    return sessions
    
def dump_sessions_dataframe(cursor):    
    return pd.DataFrame.from_records(dump_sessions(cursor).values(), index='id')
    
def map_children_sessions(cursor):    
    """Map sessions to all their children, grandchildren etc.
    Returns:
        tree: dictionary mapping session IDs to all children
        path_tree: dictionary mapping each path to every session which begins with that path prefix
    """
    full_tree = defaultdict(list)
    path_tree = defaultdict(list)
    sessions = cursor.execute("SELECT id FROM session").fetchall()
    for session in sessions:
        s = session[0]
        orig = s
        parent,path = cursor.execute("SELECT parent,path FROM session WHERE id=?", (s,)).fetchone()
        while parent!=None:
            full_tree[parent].append(orig)
            path_tree[path].append(orig)
            s = parent
            parent,path = cursor.execute("SELECT parent,path FROM session WHERE id=?", (s,)).fetchone()
    return full_tree, path_tree    
    
def session_tree(cursor):
    tree = defaultdict(list)
    parents = cursor.execute("SELECT id,parent FROM session").fetchall()
    for child, parent in parents:
        tree[parent].append(child)
    return tree    

def paths(cursor):
    paths = cursor.execute("SELECT DISTINCT(path) FROM session ORDER BY path").fetchall() 
    return [p[0] for p in paths]
    
def dump_dataframe(cursor):    
    c = cursor
    all = defaultdict(list) 
    paths = c.execute("SELECT DISTINCT(path) FROM session").fetchall()    
    for path in paths:
        sessions = c.execute("SELECT id FROM session WHERE path=?", path).fetchall()            
        for session in sessions:                
            rows = c.execute("SELECT log.stream,log.time,log.json,log.valid,stream.name FROM log JOIN stream ON stream.id=log.stream WHERE session=? ", session).fetchall()
            frame = defaultdict(list)
            for stream, time, js, valid, stream_name in rows:
                d = json.loads(js)
                d['t'] = time
                d['valid'] = valid                
                frame[stream_name].append(d)            
            # convert to pandas
            dfs = {}
            for k,v in frame.iteritems():
                columns = json_columns(v)
                dfs[k] = pd.DataFrame(v, columns=columns.keys())                                
            all[path].append(dfs)
    return all    
        
def meta(cursor):    
    """Return a pair of dictionaries, representing all of the meta data entries, and their bindings to sessions.
    Returns:
        meta: dictionary of meta data entries, one for each meta type `mtype` (e.g. "PATH", "USER", "DATASET", etc.)
            format:
            {
                name: string,
                description: string,
                data: any JSON object,
                bound: list of session numbers this metadata is bound to
                type: string, [optional]
            }
            
        bound_ix: mapping from session number to meta data dictionary
    """
    
    c = cursor    
    meta = c.execute("SELECT id,name,description,type,mtype,json FROM meta").fetchall()    
    metas = defaultdict(list)
    bound_ix = defaultdict(list)
    for id,name,description,stype,mtype,js in meta:        
        session = c.execute("SELECT session FROM meta_session WHERE meta_session.meta=?", (id,)).fetchall()    
        if session is not None:            
            bound = [s[0] for s in session]
        else:
            bound = []
        if js is None:
            js = 'null'         
        meta_dict = {'name':name, 'description':description, 'type':stype, 'data':json.loads(js), 'bound':bound}
        metas[mtype].append(meta_dict)
        for ix in bound:
            bound_ix[ix].append(meta_dict)
    return metas, bound_ix


def meta_dataframe(cursor):    
    c = cursor    
    metas, _ = meta(c)
    frames = {}    
    for name, value in metas.iteritems():
        frames[name] = pd.DataFrame(value)
        
    return frames
    
if __name__=="__main__":
    import sqlite3
    import sys
    conn = sqlite3.connect("my.db")
    cursor = conn.cursor()
    with open("test.json", "w") as f:
        dump_json(cursor, f)    
    with open("test.json", "r") as f:
        print json.load(f)
        
