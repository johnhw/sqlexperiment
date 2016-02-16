import sqlite3
import json
import pseudo
import logging
import time
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
        sessions = c.execute("SELECT id FROM session WHERE path=?", path).fetchall()            
        for session in sessions:                
            rows = c.execute("SELECT log.stream,log.time,log.json,log.valid,stream.name FROM log JOIN stream ON stream.id=log.stream WHERE session=? ", session).fetchall()
            frame = defaultdict(list)
            for stream, time, js, valid, stream_name in rows:
                d = json.loads(js)
                d['t'] = time
                d['valid'] = valid                
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
    rows = c.execute("SELECT log.stream,log.time,log.json,log.valid,stream.name,log.session,session.path FROM log JOIN stream ON stream.id=log.stream JOIN session on log.session=session.id").fetchall()
    frame = defaultdict(list)
    for stream, time, js, valid, stream_name,session,path in rows:
        d = json.loads(js)
        d['t'] = time
        d['valid'] = valid                
        d['path'] = path
        d['session'] = session
        frame[stream_name].append(d)
    return frame
    

def to_csv_flat(cursor, csvdir):
    """Write each stream type to a single CSV file in the given directory, in the same format as dumpflat() does"""
    streams = dumpflat(cursor)    
    # write out each stream independently
    for stream_name, stream_data in streams.iteritems():                
        with open(os.path.join(csvdir,"%s.csv" % (stream_name)), 'w') as f:
            
            csvfile = csv.DictWriter(f, delimiter=",", fieldnames=stream_data[0].keys())
            csvfile.writeheader()
            for s in stream_data:
                csvfile.writerow(s)
    
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
                dfs[k] = pd.DataFrame(v)                                
            all[path].append(dfs)
    return all    
    
    

def meta(cursor):    
    c = cursor
    
    meta = c.execute("SELECT id,name,description,type,mtype,json FROM meta").fetchall()    
    metas = defaultdict(list)
    for id,name,description,stype,mtype,js in meta:        
        session = c.execute("SELECT session FROM meta_session WHERE meta_session.meta=?", (id,)).fetchall()    
        if session is not None:            
            bound = [s[0] for s in session]
        else:
            bound = []
        if js is None:
            js = 'null'
            
        metas[mtype].append({'name':name, 'description':description, 'type':stype, 'data':json.loads(js), 'bound':bound})
    return metas
    