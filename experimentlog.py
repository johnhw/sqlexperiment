import sqlite3
import json
import pseudo
import logging
import time
import os
import math
import cStringIO
import numpy as np
import platform
import traceback


def np_to_str(d):
    c = cStringIO.StringIO()
    np.savez(c,**d)
    return c.getvalue()
    
def str_to_np(s):    
    c = cStringIO.StringIO(s)
    n = np.load(c)
    return n

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename="experiment.log")
                    
stream_logger = logging.StreamHandler() 
stream_logger.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logging.getLogger().addHandler(stream_logger)

class ExperimentException(Exception):
    pass

# local NTP server
#default_ntp_servers = ["ntp0.dcs.gla.ac.uk", "ntp1.dcs.gla.ac.uk", "ntp2.dcs.gla.ac.uk"]
default_ntp_servers = ["1.pool.ntp.org","2.pool.ntp.org","3.pool.ntp.org"]
  
def check_time_sync(n_queries=3, servers=None):
    """Use NTP to find the offset from the real time, by querying NTP servers. 
    Queries each server n_queries times"""    
    if servers is None:
        servers = default_ntp_servers
    try: 
        import ntplib
    except ImportError:
        logging.warn("No NTPLib installed; proceeding *without* real synchronisation.\n 'pip install ntplib' will install NTPLib")
        return 0
    c = ntplib.NTPClient()
    for server in servers:        
        # synchronise to each server
        logging.debug("Synchronising to NTP server %s" % server)
        offsets = []
        try:
            # make a bunch of requests from this server, and record the offsets we got back
            for j in range(n_queries):
                response = c.request(server, version=3)                
                offsets.append(response.offset)        
                time.sleep(0.05)
        except ntplib.NTPException, e:
            logging.debug("Request to %s failed with %s" % (server, e))  
            
    # if we got some times, compute the median time and return it (and record some status logs)
    if len(offsets)>0:
        mean = sum(offsets) / float(len(offsets))
        std = math.sqrt(sum(((o-mean)**2 for o in offsets)) / float(len(offsets)))
        median = sorted(offsets)[len(offsets)//2]
        logging.debug("Time offset %.4f (median: %.4f) seconds (%.4f seconds std. dev.)" % (mean, median, std))  
        return median
    return 0
               
            


def pretty_json(x):
    return json.dumps(x, sort_keys=True, indent=4, separators=(',', ': '))
            

            
class ExperimentLog(object):

    def __init__(self, fname, autocommit=None, ntp_sync=True, ntp_servers=None):
        """
        experimenter: Name of experimenter running this trial.          
        autocommit: If None, never autocommits. If an integer, autocommits every n seconds. If True,
                    autocommits on *every* write (not recommended)
                    """
        logging.debug("Opening database '%s'. Autocommit: '%s'" % (fname, autocommit))                 
        
        self.conn = sqlite3.connect(fname)                        
        self.cursor = self.conn.cursor()

        self.time_offset = 0
        if ntp_sync:
            # synchronise the (global) time
            self.time_offset = check_time_sync(n_queries=10, servers=ntp_servers)
            
        
        # extend the cache size and disable synchronous writing
        self.execute("PRAGMA cache_size=2000000;")
        self.execute("PRAGMA synchronous=OFF;")
              
        # create the tables
        table_exists = self.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='setup'").fetchone()[0]
        if not table_exists:
            self.create_tables()
        else:
            logging.debug("Tables already created.")
                            
        self.autocommit = autocommit                
        self.last_commit_time = time.time()
        self.session_stack = []
        self.session_name_stack = []        
        self.session_id = None
        self.in_run = False
        self.active_users = {}
        self.stream_cache = {}
        
    def __enter__(self):
        """Start when using a context-manager"""
        return self
    
    def __exit__(self, type, value, tb):
        """End when using a context-manager"""        
        logging.debug("Exiting: %s", (type, value, tb))
        traceback.print_tb(tb)
        self.close()
    
    
    def run(self, *args, **kwargs):
        """Context manager for runs"""
        class ExperimentRun(object):
            def __init__(self, exp, *args, **kwargs):
                self.exp = exp
                self.args = args
                self.kwargs = kwargs
            def __enter__(self):
                self.exp.start_run(*self.args, **self.kwargs)
            def __exit__(self, type, value, tb):
                logging.debug("Run ending: %s", (type, value, tb))
                traceback.print_tb(tb)
                self.exp.end_run()
        return ExperimentRun(self, *args, **kwargs)
    
    def t(self):
        return self.real_time()
    
    def real_time(self):
        """Return offseted time"""
        return time.time() + self.time_offset
    
    def create_tables(self):
        """Create the SQLite tables for the experiment, if they do not already exist"""
        # set of users
        # stack of sessions, each with a state. Total state is dictionary merge of the session states

        logging.debug("Creating tables.")
        c = self.cursor
                
            
        c.execute('''CREATE TABLE IF NOT EXISTS meta
                     (id INTEGER PRIMARY KEY, mtype TEXT, name TEXT, type TEXT, description TEXT, json TEXT, meta INTEGER)''')
          

        # the state of creation
        c.execute('''CREATE TABLE IF NOT EXISTS setup (id INTEGER PRIMARY KEY, stage TEXT, time REAL)''')
                
        
        # record variables of a particular experiment or trial
        # It is a real data capture session and so has a definite start_time and end_time
        # The random seeds used should be stored so that all data is reproducible
        # If this is a test run, this should be flagged so that the dataset does not get polluted
        # The config used for the run should be stored in config as a JSON entry
        # Valid determines if this trial represents valid data
        # Complete indicates if the trial was finished successfully
        # notes indicates any special notes for this trial
        # parent indicates the hierarchy of this session
        c.execute('''CREATE TABLE IF NOT EXISTS session
                    (id INTEGER PRIMARY KEY, start_time REAL, end_time REAL, last_time REAL,
                    test_run INT, random_seed INT,
                    valid INT, complete INT, description TEXT,
                    json TEXT, 
                    parent INT, path INT, meta INT, 
                    FOREIGN KEY (parent) REFERENCES session(id),
                    FOREIGN KEY (meta) REFERENCES meta(id)
                    FOREIGN KEY (path) REFERENCES meta(id)
                    )''')

                            
        # text tags which are recorded throughout the trial stream
        c.execute('''CREATE TABLE IF NOT EXISTS log
                    (id INTEGER PRIMARY KEY,
                    session INT,
                    valid INT,
                    time REAL,
                    stream INT,                    
                    tag TEXT,
                    json TEXT,                         
                    FOREIGN KEY(stream) REFERENCES meta(id),
                    FOREIGN KEY(session) REFERENCES session(id))
                    ''')
                    
        c.execute('''CREATE TABLE IF NOT EXISTS blobs
                    (id INTEGER PRIMARY KEY,
                    log INTEGER,
                    meta INTEGER,
                    blob BLOB,
                    FOREIGN KEY(log) REFERENCES log(id),
                    FOREIGN KEY(meta) REFERENCES meta(id))
                    ''')
                    
        c.execute('''CREATE TABLE IF NOT EXISTS sync_points
                    (id INTEGER PRIMARY KEY,
                    fname TEXT,        
                    description TEXT,
                    json TEXT,
                    start_time REAL,
                    media_start_time REAL,
                    duration REAL,
                    time_rate REAL)''')
                    
        # runs of the code
        # Experimenter holds the ID of the experimenter for this session
        # clean_exit is True if the run exited normally (clean shutdown)
        # start_time and end_time are the times when the software was started and stopped
        # uname records the details of the machine this was run was executed on
        # ntp_clock_offset records the clock offset that was in effect for this run (all timestamps already incorporate this value)
        # json records any per-run configuration
        c.execute('''CREATE TABLE IF NOT EXISTS runs
                    (id INTEGER PRIMARY KEY,
                    start_time REAL,
                    end_time REAL,
                    experimenter TEXT,
                    clean_exit INT,
                    uname TEXT,
                    ntp_clock_offset REAL,
                    json TEXT)
                    ''')

        # maps software runs to experimental sessions
        c.execute('''CREATE TABLE IF NOT EXISTS run_session
                    (id INTEGER PRIMARY KEY,
                    session INT,
                    run INT,
                    FOREIGN KEY(session) REFERENCES session(id),
                    FOREIGN KEY(run) REFERENCES runs(id))
                    ''')
                    
        # maps parent sessions to all children, grandchildren, etc.
        c.execute('''CREATE TABLE IF NOT EXISTS children
                    (id INTEGER PRIMARY KEY,
                    parent INT,
                    child INT,
                    FOREIGN KEY(parent) REFERENCES session(id),
                    FOREIGN KEY(child) REFERENCES session(id))
                    ''')
        
        # map (many) users/equipment/configs to (many) sessions
        c.execute('''CREATE TABLE IF NOT EXISTS user_session
                    (id INTEGER PRIMARY KEY, user INT, session INT, role TEXT, json TEXT,
                    FOREIGN KEY(user) REFERENCES meta(id)
                    FOREIGN KEY(session) REFERENCES session(id)
                    )''')
                    
        c.execute('''CREATE TABLE IF NOT EXISTS paths (id INTEGER PRIMARY KEY, name TEXT, subcount INT, description TEXT, json TEXT)''')
                    
        c.execute('''CREATE VIEW IF NOT EXISTS users AS SELECT * FROM meta WHERE mtype="USER"''')
        c.execute('''CREATE VIEW IF NOT EXISTS session_meta AS SELECT * FROM meta WHERE mtype="SESSION"''')
        c.execute('''CREATE VIEW IF NOT EXISTS blob_meta AS SELECT * FROM meta WHERE mtype="BLOB"''')
        c.execute('''CREATE VIEW IF NOT EXISTS log_stream AS SELECT * FROM meta WHERE mtype="LOG"''')        
        c.execute('''CREATE VIEW IF NOT EXISTS equipment AS SELECT * FROM meta WHERE mtype="EQUIPMENT"''')        
        c.execute('''CREATE VIEW IF NOT EXISTS dataset AS SELECT * FROM meta WHERE mtype="DATASET"''')
        
        # create the root path        
        c.execute('''INSERT INTO paths(name, subcount) VALUES ('/', 0)''')
        self.set_stage("init")
        
        
    def set_meta(self, **kwargs):
        """Update the global metadata for this entire dataset"""        
        current = self.get_meta()
        for arg,value in kwargs.iteritems():
            current[arg] = value        
        self.execute('INSERT INTO meta(json,mtype) VALUES (?, "DATASET")', (json.dumps(current),))
            
        
    def get_meta(self):
        """Return the metadata for the entire dataset as a dictionary"""
        meta = {}
        row = self.execute("SELECT json FROM dataset WHERE id=(SELECT MAX(id) FROM dataset)").fetchone()
        if row is not None:
            return json.loads(row[0])
        return {}               
        
        
    def start_run(self, experimenter="", run_config={}):
        """Create a new run entry in the runs table."""
        self.execute("INSERT INTO runs(start_time, experimenter, clean_exit, ntp_clock_offset, uname, json) VALUES (?, ?, ?, ?, ?, ?)",
                           (self.real_time(),
                           experimenter,
                           0,                           
                           self.time_offset,
                           json.dumps(platform.uname()),
                           json.dumps(run_config)))
        self.run_id = self.cursor.lastrowid
        logging.debug("Run ID: [%08d]" % self.run_id)
        logging.debug("Experimenter logged as '%s'" % experimenter)
        logging.debug("Run config logged as '%s'" % pretty_json(run_config))        
        self.commit()
        self.in_run = True
        
    def end_run(self):
        """Update the run entry to mark this as a clean exit and reflect the end time."""
        logging.debug("Marking end of run [%08d]." % self.run_id)                
        self.execute("UPDATE runs SET end_time=?, clean_exit=? WHERE id=?",
                           (self.real_time(),
                           1,
                           self.run_id))
        self.in_run = False
        
        
    def sync(self, fname, start_time, duration=None, media_start_time=0, time_rate=1.0, description=None, data={}):
        """Synchronise an external file (e.g. a video or audio recording) with the main log file.
        Must specify the start_time (in seconds since the epoch, same format as all other times). time_rate can be used to adjust
        for files that have some time slippage"""
        logging.debug("Syncing %s to %f:%s (%s) " % (fname, start_time, end_time, description))
        self.execute("INSERT INTO sync_points(fname, start_time, duration, media_start_time, time_rate, description, json) VALUES  (?,?,?,?,?,?)", 
            (fname, start_time, duration,  media_start_time, time_rate, description, json.dumps(data)))
        
        
    
    def add_indices(self):
        """Add indices to the log"""
        self.execute("CREATE INDEX log_session_ix ON log(session)")
        self.execute("CREATE INDEX log_tag_ix ON log(tag)")
        self.execute("CREATE INDEX log_stream_ix ON log(stream)")
        self.execute("CREATE INDEX log_valid_ix ON log(valid)")
            
    def close(self):
        # auto end the run
        if self.in_run:
            self.end_run()
        self.commit()        
        logging.debug("Database closed.")
        
    def commit(self):
        """Force all changes to be stored to the database.         
        """
        logging.debug("<Commit>")
        self.conn.commit()
        
    def get_path_id(self, path):
        """Return the ID of a path, creating a new one if this path has not been seen before"""
        result = self.execute("SELECT id FROM paths WHERE name=?", (path,))        
        row = result.fetchone()
        if row is None:
            self.execute("INSERT INTO paths(name, subcount) VALUES (?,0)", (path,))
            id = self.cursor.lastrowid
        else:
            id = row[0]        
        return id
        
    # clean this up and unify
        
    def register_session(self, name, stype="", description="", data=None, force_update=False):
        """Register a new session type."""
        result = self.execute("SELECT id FROM session_meta WHERE name=?", (name,))        
        id = result.fetchone()
        if id is None:        
            logging.debug("Registering session '%s' of type '%s', with data [%s]" % (name, stype, json.dumps(data)))
            self.execute("INSERT INTO meta(name,type,description,json,mtype) VALUES (?,?,?,?,?)", (name, stype, description, json.dumps(data), "SESSION"))   
        else:
            if not force_update:                
                raise ExperimentException("Session %s already exists; not updating" % name)
            else:
                logging.warn("Session %s exists; force updating" % name)
                self.execute("UPDATE meta SET name=?,type=?,description=?,json=? where meta.id=%d"%id[0], (name, stype, description, json.dumps(data), ))    
                
    def register_blob(self, name, stype="", description="", data=None, force_update=False):
        """Register a new blob type."""
        result = self.execute("SELECT id FROM blob_meta WHERE name=?", (name,))        
        id = result.fetchone()
        if id is None:        
            logging.debug("Registering blob '%s' of type '%s', with data [%s]" % (name, stype, json.dumps(data)))
            self.execute("INSERT INTO meta(name,type,description,json,mtype) VALUES (?,?,?,?,?)", (name, stype, description, json.dumps(data), "BLOB"))   
        else:
            if not force_update:                
                raise ExperimentException("Blob %s already exists; not updating" % name)
            else:
                logging.warn("Blob %s exists; force updating" % name)
                self.execute("UPDATE meta SET name=?,type=?,description=?,json=? where meta.id=%d"%id[0], (name, stype, description, json.dumps(data), ))         
                
    def register_stream(self, name, stype="", description="", data=None, force_update=False):
        """Register a new log type."""
        stream_id = self.get_log_stream(name)
        if stream_id is None:
            logging.debug("Registering log '%s' of type '%s', with data [%s]" % (name, stype, json.dumps(data)))
            self.execute("INSERT INTO meta(name,type,description,json,mtype) VALUES (?,?,?,?,?)", (name, stype, description, json.dumps(data), "LOG"))   
            # cache the ID of this stream, and create a convenient view
            stream_id = self.cursor.lastrowid
            self.stream_cache[name] = stream_id
            self.execute("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM log WHERE stream=%d" % (name, stream_id))
        else:
            if not force_update:                
                raise ExperimentException("Stream %s already exists; not updating" % name)
            else:
                logging.warn("Stream %s exists; force updating" % name)
                self.execute("UPDATE meta SET name=?,type=?,description=?,json=? where meta.id=%d"%stream_id, (name, stype, description, json.dumps(data), ))    
            
    def register_user(self, name=None, stype=None, user_vars={}, force_update=False):
        """Enroll a user in the experiment.         
        Parameters:
            name: If None, a random name will be generated and returned.
            user_vars: specifies any per-user variables (e.g demographics, anthropometrics). 
        Returns:
            The user name added.
        """        
        # create new name is none specified -- caller should be sure to
        # log the return value in this case!
        if name is None:
            name = pseudo.get_pseudo()
            logging.debug("Creating new pseudonym '%s'" % name)
                    
        user_id = self.check_user_exists(name)
        if user_id is not None:
            if not self.force_update:
                raise ExperimentException("User pseudonym %s already exists in the database" % name)
            else:
                self.execute("UPDATE meta SET name=?, type=?, json=? WHERE meta.id=%d" % id,
                               (name, stype,
                                json.dumps(user_vars)))
            logging.debug("User '%s' force updated to state %s" % (name, user_vars))
        else:        
            # insert into the DB
            self.execute("INSERT INTO meta(name, type, json, mtype) VALUES (?, ?, ?, ?)",
                               (name,        
                                stype,
                                json.dumps(user_vars), 
                                "USER"))
            logging.debug("User '%s' enrolled with state %s" % (name, user_vars))
        return name
        
    def set_stage(self, stage):
        """Set the new stage of the database (e.g. init'ed, setup, etc.)"""
        logging.debug("Database moving to stage %s" % stage)
        self.execute("INSERT INTO setup(stage, time) VALUES (?,?)", (stage, self.real_time()))
               
    def get_stage(self):
         result = self.execute("SELECT stage FROM setup WHERE id = (SELECT MAX(id) FROM setup)").fetchone()
         return result[0]
    
    @property
    def session_path(self):
        return "/"+("/".join(self.session_names()))
        
    def cd(self, path=None):
        # path name parsing
        # 
        if path is None:
            # enter a new directory
            self.enter_session()
            return
        else:
            # find largest common subcomponent and then execute the leaves/enters needed
            components = path.split("/") # may contain '..' and '.'
            current_path = self.session_path.split("/")
            
            
            
        
    def enter_session(self, prototype_name=None, extra_config=None, test_run=False, notes=""):
        """Start a new session with the given prototype"""
        
        if not self.in_run:
            raise ExperimentException("No run started; cannot start session")
                       
        # find the ID of the current session path
        current_path_id = self.get_path_id(self.session_path)        
        
        # find the prototype ID
        if prototype_name is None:
            proto = None
            # this is a counted repetition; increment the counter
            result = self.execute("SELECT subcount FROM paths WHERE id=?", (current_path_id,))
            sub_id = result.fetchone()[0]
            self.execute("UPDATE paths SET subcount=? WHERE id=?", (sub_id + 1,current_path_id))                        
            prototype_name = str(sub_id)
        else:
            result = self.execute("SELECT id FROM session_meta WHERE name=?", (prototype_name,))
            proto = result.fetchone()
            if proto is not None:
                proto = proto[0]
                
        path = "/"+("/".join(self.session_names()+[str(prototype_name)]))               
        logging.debug("Entering session '%s'" % path )
         
        logging.debug("Prototype ID '%s'" % proto )        
        
        # get the id of this new path
        path_id = self.get_path_id(path)        
        
        # force a commit        
        t = self.real_time()
        self.execute("INSERT INTO session(start_time, last_time, test_run, json, description, parent, path, meta, complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (t,
                           t,
                           test_run,
                           json.dumps(extra_config),                           
                           notes,
                           self.session_id, path_id, proto, False))
       
        
        # map the session<->run table
        self.execute("INSERT INTO run_session(session, run) VALUES (?,?)", (self.session_id, self.run_id))
        self.session_id = self.cursor.lastrowid
        
        # set the users for this session
        for user_name, user_data in self.active_users.iteritems():            
            self.execute("INSERT INTO user_session(session, user, role, json) VALUES (?, ?, ?, ?)",
                               (self.session_id,
                               user_data["id"],
                               user_data["role"],
                               json.dumps(user_data["data"])
                               ))        

        # record the complete parent/child status
        for parent_id in self.session_stack:
            self.execute("INSERT INTO children(parent, child) VALUES (?, ?)", (parent_id, self.session_id))
        
        logging.debug("Active users: %s" % list(self.active_users))
        
        self.session_stack.append(self.session_id)
        self.session_name_stack.append(str(prototype_name))
        
        logging.debug("New session ID [%08d]" % self.session_id)
        self.commit()
        self.user_changed = False
    
    def leave_session(self, complete=True, valid=True):
        """Stop the current session, marking according to the flags."""
        s = self.session_names()
        path = "/"+("/".join(s))
        logging.debug("Leaving session '%s'" % path)        
        t = self.real_time()
        self.execute("UPDATE session SET end_time=?, last_time=?, valid=?, complete=? WHERE id=?",
                           (t,
                           t,
                           valid,
                           complete,
                           self.session_id))        
        # jump to previous session
        self.session_stack.pop()
        self.session_name_stack.pop()        
        
        if len(self.session_stack)>0:
            self.session_id = self.session_stack[-1]        
            logging.debug("Back to session ID [%08d]" % self.session_id)
        else:
            self.session_id = None  
            logging.debug("Back to root session")
        # force a commit
        self.commit()
        
    def root_session(self):
        while len(self.session_stack)>0:
            self.leave_session()
                           
    def session_states(self):
        """Return the session hierarchy"""
        return tuple(self.session_stack)
    
    def session_names(self):
        """Return the session hierarchy as a list of prototype names, or IDs if there are no prototypes"""    
        return self.session_name_stack
    
    def get_stable_random_seed(self):
        """Return a random seed which is hash of the current session hierarchy"""        
        return self.session_id
    
        
    def check_user_exists(self, name):
        """Return True if the given pseudonym is registered in the users table."""
        results = self.execute("SELECT id FROM users WHERE users.name=?", (name,))
        return results.fetchone() 
        
    def add_active_user(self, name, role=None, data=None):
        """Add the specified user to the active user set for the next session."""
        id = self.check_user_exists(name)
        if id is None:
            logging.warn("User %s not pre-registered; creating new blank entry" % name)
            self.register_user(name, stype="AUTO")
            id = self.check_user_exists(name)            
        id = id[0]
        self.active_users[name] = {"role":role, "data":data, "id":id}
        logging.debug("Added user '%s' to active user list" % name)
        self.user_changed = True
               
    def set_user(self, name, role=None, data=None):
        """Set a single user to be active"""
        self.clear_active_users()
        self.add_active_user(name, role, data)
                
    def remove_active_user(self, name):
        """Remove the specified user from the active user set for the next session."""        
        assert(self.check_user_exists(name))
        if name not in self.active_users:
            raise ExperimentException("Tried to remove user %s from the active list when they were not on it." % name)
            
        logging.debug("Removed user '%s' from active list" % name)
        self.user_changed = True
        
    def clear_active_users(self):
        """Clear all users."""
        logging.debug("All users cleared from active list")
        self.active_users = {}
        self.user_changed = True

    @property
    def users(self):
        """Return the set of active users"""
        return dict(self.active_users)
    
    def get_log_stream(self, name):
        results = self.execute("SELECT id FROM log_stream WHERE log_stream.name=?", (name,))
        return results.fetchone()
        
    def execute(self, query, parameters=()):
    
        
        return self.cursor.execute(query, parameters)
        
    def attach_blob(self, log, blob, meta=None):       
        self.execute("INSERT INTO blobs(log, blob, meta) VALUES (?,?,?)", (log, blob, meta))
       
    
    def log(self, stream, t=None, valid=True, data=None, tag=""):
        """Log the given data in the currently active session        
        Parameters:
            stream: stream id to write to
            tag: Tag to use for the stream (optional)
            t: Timestamp of the data. If None, uses the timestamp when the data is written in
            valid: True if this datapoint should be marked as valid, False otherwise
            data: Dictionary of data entries to be written to the log.        
            
        Returns:
            id: The id of this log entry (e.g. if you want to store additional table in another table)
            """    
        
        if not self.in_run:
            raise ExperimentException("No run started; cannot log data")
        
        
        # check if we already know what kind of stream this is; otherwise,
        # look it up from the DB and cache it for later use
        if stream in self.stream_cache:
            stream_id = self.stream_cache[stream]
        else:
            stream_id = self.get_log_stream(stream)
            # if there is no such stream ID, create a new one and use that
            if stream_id is None:
                logging.warn("No stream %s registered; creating a new blank entry" % stream)
                self.register_stream(stream, stype="AUTO")
                stream_id = self.get_log_stream(stream)
                
            stream_id = stream_id[0]
            self.stream_cache[stream] = stream_id
                    
        t = t or self.real_time()
        self.execute("INSERT INTO log(session, valid, time, stream, tag, json) VALUES (?, ?, ?, ?, ?, ?)",
                           (self.session_id,
                           valid,
                           t,
                           stream_id,
                           tag,
                           json.dumps(data),
                           ))
        
        id = self.cursor.lastrowid        
        
        
        # update last time in the session table
        self.execute("UPDATE session SET last_time=? WHERE id=?",
                           (time.time(),                           
                           self.session_id)) 
                           
        # deal with autocommits to the log
        now = self.real_time()
        if self.autocommit is not None and now - self.last_commit_time > self.autocommit:
            logging.debug("Time-based autocommit")
            self.last_commit_time = now
            self.commit()
            
        return id
            
                       
    
    
if __name__=="__main__":
    with ExperimentLog(":memory:", ntp_sync=False) as e:   
        
        if e.get_stage()=="init":
            e.register_stream("sensor_1")
            e.register_session("Experiment1", "EXP", description="Main experiment")
            e.register_session("Condition A", "COND", description="Condition A")
            e.register_session("Condition B", "COND", description="Condition B")
            e.register_session("Condition C", "COND", description="Condition C")    
            e.set_stage("setup")
       
        p = pseudo.get_pseudo()
        e.register_user(p, user_vars={"age":35})
                
        with e.run(experimenter="JHW") as run:
            e.set_user(p)
            e.enter_session("Experiment1")
            e.enter_session("Condition B")
            e.enter_session()
            e.log("sensor_2", data={"Stuff":1})
            e.leave_session()
            e.enter_session()
            e.log("sensor_2", data={"Stuff":1})
            e.leave_session()
            e.root_session()
        
    from dejson import DeJSON
    DeJSON("my.db", "my_nojson.db")
        
    
        
    
    