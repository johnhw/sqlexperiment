import json
import logging
import os
from collections import defaultdict
import pandas as pd


class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

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
    