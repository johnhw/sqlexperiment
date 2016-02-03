import logging
import time
import os
import sqlite3

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
                    
def make_report(fname, report=None):
    logging.debug("Opening database '%s'" % (fname))         
    
    # open the database
    conn = sqlite3.connect(fname)                
    cursor = conn.cursor()
    
    basename = os.path.splitext(fname)[0]
    if not report:
        report = "%s_report.txt" % (basename)
        
    def sqlresult(query):
        result = cursor.execute(query)
        if result is None:
            return None
        else:
            return result.fetchone()[0]
            
    def allsqlresult(query):
        result = cursor.execute(query)
        if result is None:
            return None
        else:            
            return [row[0] for row in result.fetchall()]
    
    with open(report, "w") as f:
        f.write("# Report generated for %s\n" % fname)
        f.write("#### Report date: %s\n" % time.asctime())
    
        f.write("\n## Sessions\n")        
        session_types = cursor.execute("SELECT id, name FROM paths").fetchall()
        for id, name in session_types:                   
            f.write("\n#### %s\n" % name)            
            #f.write("##### %s\n" % description)       
            f.write("* Runs: %d\n" % sqlresult("SELECT count(id) FROM session WHERE path=%d"%id))        
            f.write("* Duration recorded: %s seconds\n" % sqlresult("SELECT sum(session.end_time-session.start_time) FROM session WHERE session.path=%d"%id ))
            
        
        f.write("\n## Runs\n")        
        f.write("* Number of runs: %s\n" % sqlresult("SELECT count(id) FROM runs"))
        f.write("* Total duration recorded: %.1f seconds\n" % sqlresult("SELECT sum(end_time-start_time) FROM runs"))
        f.write("* Dirty exits: %s\n" % sqlresult("SELECT count(clean_exit) FROM runs WHERE clean_exit=0"))
        
        f.write("\n## Users\n")
        f.write("* Unique users: %s\n" % sqlresult("SELECT count(id) FROM users"))
        
        f.write("\n## Log\n")
        f.write("* Log streams recorded: %s\n" % ",".join(allsqlresult("SELECT name FROM log_stream")))
        session_types = cursor.execute("SELECT id, name, description FROM log_stream").fetchall()
        for id, name, description in session_types:
            f.write("\n#### %s\n" % name)
            f.write("##### %s\n" % description)       
            f.write("* Total entries: %d\n" % sqlresult("SELECT count(id) FROM log WHERE stream=%d"%id))        
            
        
    conn.close()

make_report("my.db")
os.system("cat my_report.txt")    