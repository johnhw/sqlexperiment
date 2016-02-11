import logging
import time
import os
import sqlite3
import json
import cStringIO


def pretty_json(x):
    return ("\n"+json.dumps(x, sort_keys=True, indent=4, separators=(',', ': '))+"\n").replace("\n", "\n        ")
    
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')

def string_report(cursor):
    c = cStringIO.StringIO()
    make_report(cursor, c)
    return c.getvalue()


def string_readme(cursor):
    c = cStringIO.StringIO()
    make_readme(cursor, c)
    return c.getvalue()    

def date_format(t):
    timestruct = time.localtime(t)
    return time.strftime("%d %B %Y", timestruct)
    
    
    
def make_readme(cursor, f, fname="none"):
           
    def sqlresult(query):
        result = cursor.execute(query).fetchone()
        if result is None:
            return ""
        else:
            return result[0]
            
    def allsqlresult(query):
        result = cursor.execute(query)
        if result is None:
            return ""
        else:            
            return [row[0] for row in result.fetchall()]
    
    
    meta = json.loads(sqlresult("SELECT json FROM dataset WHERE id=(SELECT MAX(id) FROM dataset)"))
    
    f.write("# %s\n" % meta.get("title", "---"))    
    f.write("#### %s\n" % meta.get("short_description", ""))
    f.write("#### DOI: %s\n" % meta.get("doi", ""))
    f.write("\n----------------------------------------\n")
    f.write("#### Report date: %s\n" % time.asctime())
    f.write("\n-----------------------------------------\n")
    f.write("#### Authors: %s\n" % meta.get("authors", "unknown"))
    f.write("#### Institution: %s\n" % meta.get("institution", ""))
    f.write("#### License: %s\n" % meta.get("license", "unknown"))
    f.write("----------------------------\n")
    f.write("## Confidential: %s\n" % meta.get("confidential", "no"))
    f.write("----------------------------\n")
    if "funder" in meta:
        f.write("#### Funded by: %s\n" % meta["funder"])
    if "paper" in meta:
        f.write("#### Associated paper:  %s\n" % meta["paper"])
    if "ethics" in meta:
        f.write("#### Ethics board approval number: %s\n" % meta["ethics"])
    
    f.write("----------------------------\n")
    f.write("\n\n **Description**  %s \n\n" % meta.get("description", ""))
    
    f.write("---------\n")
    start = sqlresult("SELECT min(start_time) FROM runs")
    end = sqlresult("SELECT max(end_time) FROM runs")
        
    f.write(" * Data acquired from %s to %s\n" % (date_format(start), date_format(end)))
    
    f.write("\n----------------------------------------\n")
    
    f.write("* Number of experimental runs: %s\n" % sqlresult("SELECT count(id) FROM runs"))
    f.write("* Total duration recorded: %.1f seconds\n" % sqlresult("SELECT sum(end_time-start_time) FROM runs"))
    f.write("* Number of users: %s\n" % sqlresult("SELECT count(id) FROM users"))
   
    f.write("* Total logged entries: %d\n" % sqlresult("SELECT count(id) FROM log"))        
    f.write("\n----------------------------------------\n")
                            
def make_report(cursor, f, fname="none"):
           
    def sqlresult(query):
        result = cursor.execute(query).fetchone()
        if result is None:
            return ""
        else:
            return result[0]
            
    def allsqlresult(query):
        result = cursor.execute(query)
        if result is None:
            return ""
        else:            
            return [row[0] for row in result.fetchall()]
    
    
    f.write("# Report generated for %s\n" % fname)
    f.write("\n----------------------------------------\n")
    f.write("#### Report date: %s\n" % time.asctime())
    
    
    f.write("\n----------------------------------------\n")
    f.write("\n## Runs\n")        
    f.write("* Number of runs: %s\n" % sqlresult("SELECT count(id) FROM runs"))
    f.write("* Total duration recorded: %.1f seconds\n" % sqlresult("SELECT sum(end_time-start_time) FROM runs"))
    f.write("* Dirty exits: %s\n" % sqlresult("SELECT count(clean_exit) FROM runs WHERE clean_exit=0"))
    
    
    f.write("\n----------------------------------------\n")

    f.write("\n## Sessions\n")        
    session_types = cursor.execute("SELECT id, name FROM paths").fetchall()
    for id, name in session_types:                   
        f.write("\n#### %s\n" % name)            
        f.write("##### %s\n" % description)       
        f.write("* Runs: %d\n" % sqlresult("SELECT count(id) FROM session WHERE path=%d"%id))        
        f.write("* Duration recorded: %s seconds\n" % sqlresult("SELECT sum(session.end_time-session.start_time) FROM session WHERE session.path=%d"%id ))
        
    
    f.write("\n----------------------------------------\n")
    f.write("\n## Users\n")
    f.write("* Unique users: %s\n" % sqlresult("SELECT count(id) FROM users"))
    
    for id, name, jsons in cursor.execute("SELECT id,name,json FROM users").fetchall():
        f.write("\n\n#### %s\n" % name)
        f.write("**JSON** \n %s\n" % pretty_json(json.loads(jsons))) 
        f.write("Duration recorded: %s seconds\n" % sqlresult("SELECT sum(session.end_time-session.start_time) FROM session JOIN user_session ON user_session.id=session.id JOIN users ON user_session.user=users.id WHERE users.id=%d"%id))
        f.write("Paths recorded:\n\t%s" % "\n\t".join(allsqlresult("SELECT paths.name FROM paths JOIN session ON paths.id==session.path JOIN user_session ON user_session.id=session.id JOIN users ON user_session.user=users.id WHERE users.id=%d"%id)))
        
    f.write("\n----------------------------------------\n")
    f.write("\n## Log\n")
    f.write("* Log streams recorded: %s\n" % ",".join(allsqlresult("SELECT name FROM log_stream")))
    session_types = cursor.execute("SELECT id, name, description, json FROM log_stream").fetchall()
    for id, name, description, jsons in session_types:
        f.write("\n#### %s\n" % name)
        f.write("##### %s\n" % description)                   
        f.write("**JSON** \n %s\n" % pretty_json(json.loads(jsons)))                   
        f.write("* Total entries: %d\n" % sqlresult("SELECT count(id) FROM log WHERE stream=%d"%id))        
    f.write("\n----------------------------------------\n")
        

