import sqlite3
import json
import pseudo
import logging
import time

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename="experiment.log")
                    
stream_logger = logging.StreamHandler() 
stream_logger.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
logging.getLogger().addHandler(stream_logger)

class ExperimentException(Exception):
    pass

def real_time():
    return time.time()

def pretty_json(x):
    return json.dumps(x, sort_keys=True, indent=4, separators=(',', ': '))

class ExperimentLog(object):
    def __init__(self, fname, experimenter, run_config=None, autocommit=None, in_memory=False):
        """
        experimenter: Name of experimenter running this trial.          
        autocommit: If None, never autocommits. If an integer, autocommits every n seconds. If True,
                    autocommits on *every* write (not recommended)
                    """
        logging.debug("Opening database '%s'. Autocommit: '%s'" % (fname, autocommit)) 
        logging.debug("Experimenter logged as '%s'" % experimenter)
        logging.debug("Run config logged as '%s'" % pretty_json(run_config))
        
        # open the database
        self.conn = sqlite3.connect(fname)                
        self.cursor = self.conn.cursor()
        
        # extend the cache size and disable synchronous writing
        self.cursor.execute("PRAGMA cache_size=2000000;")
        self.cursor.execute("PRAGMA synchronous=OFF;")
              
        # create the tables
        self.create_tables()
            
        self.start_run(experimenter=experimenter, run_config=run_config)
                
        self.autocommit = autocommit                
        self.last_commit_time = time.time()
        self.session_stack = []
        self.session_name_stack = []
        self.session_id = None
        self.active_users = {}
        
               
    
    def create_tables(self):
        """Create the SQLite tables for the experiment, if they do not already exist"""
        # set of users
        # stack of sessions, each with a state. Total state is dictionary merge of the session states

        logging.debug("Creating tables.")
        c = self.cursor
        # create a users table
        # each user has a table entry with whichever attributes need recorded
        c.execute('''CREATE TABLE IF NOT EXISTS users
                    (id TEXT PRIMARY KEY, json TEXT)''')

        # the state of a session; should be updated during the trial to allow rollback/recovery of session state
        c.execute('''CREATE TABLE IF NOT EXISTS session_meta
                     (id INTEGER PRIMARY KEY, name TEXT, type TEXT, description TEXT, json TEXT)''')
                                         
        # record variables of a particular experiment or trial
        # It is a real data capture session and so has a definite start_time and end_time
        # The random seeds used should be stored so that all data is reproducible
        # If this is a test run, this should be flagged so that the dataset does not get polluted
        # The config used for the run should be stored in config as a JSON entry
        # Valid determines if this trial represents valid data
        # Complete indicates if the trial was finished succesfully
        # notes indicates any special notes for this trial
        # parent indicates the hierarchy of this session
        c.execute('''CREATE TABLE IF NOT EXISTS session
                    (id INTEGER PRIMARY KEY, start_time REAL, end_time REAL, last_time REAL,
                    test_run INT, random_seed INT,
                    valid INT, complete INT, notes TEXT,
                    extra_config_json TEXT,
                    parent INT, path TEXT, meta INT,
                    FOREIGN KEY (parent) REFERENCES session(id),
                    FOREIGN KEY (meta) REFERENCES session_meta(id)
                    )''')

        # text tags which are recorded throughout the trial stream
        c.execute('''CREATE TABLE IF NOT EXISTS log
                    (id INTEGER PRIMARY KEY,
                    session INT,
                    valid INT,
                    time REAL,
                    path TEXT,                    
                    tag TEXT,
                    json TEXT,            
                    FOREIGN KEY(session) REFERENCES session(id))
                    ''')

        # runs of the code
        # Experimenter holds the ID of the experimenter for this session
        # clean_exit is True if the run exited normally (clean shutdown)
        # start_time and end_time are the times when the software was started and stopped
        # json records any per-run configuration
        c.execute('''CREATE TABLE IF NOT EXISTS runs
                    (id INTEGER PRIMARY KEY,
                    start_time REAL,
                    end_time REAL,
                    experimenter TEXT,
                    clean_exit INT,
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
        
        # map (many) users to (many) sessions
        c.execute('''CREATE TABLE IF NOT EXISTS user_session
                    (id INTEGER PRIMARY KEY, user TEXT, session INT, role TEXT, json TEXT,
                    FOREIGN KEY(user) REFERENCES users(id)
                    FOREIGN KEY(session) REFERENCES session(id)
                    )''')
    
        
    
    def start_run(self, experimenter, run_config):
        """Create a new run entry in the runs table."""
        self.cursor.execute("INSERT INTO runs(start_time, experimenter, clean_exit, json) VALUES (?, ?, ?, ?)",
                           (real_time(),
                           experimenter,
                           0,                           
                           json.dumps(run_config)))
        self.run_id = self.cursor.lastrowid
        logging.debug("Run ID: [%08d]" % self.run_id)
        self.commit()
        
    def end_run(self):
        """Update the run entry to mark this as a clean exit and reflect the end time."""
        logging.debug("Marking end of run.")
        self.cursor.execute("UPDATE runs SET end_time=?, clean_exit=? WHERE id=?",
                           (real_time(),
                           1,
                           self.run_id))
        
            
    def close(self):
        self.end_run()
        self.commit()        
        logging.debug("Database closed.")
        
    def commit(self):
        """Force all changes to be stored to the database.         
        """
        logging.debug("<Commit>")
        self.conn.commit()
        
    def register_session(self, name, stype, description="", data=None):
        """Register a new session type."""
        logging.debug("Registering session '%s' of type '%s', with data [%s]" % (name, stype, json.dumps(data)))
        self.cursor.execute("INSERT INTO session_meta(name,type,description,json) VALUES (?,?,?,?)", (name, stype, description, json.dumps(data)))   
    
    def enter_session(self, prototype_name, extra_config=None, test_run=False, notes=""):
        """Start a new session with the given prototype"""
        path = "/"+("/".join(self.session_names()+[prototype_name]))
        logging.debug("Entering session '%s'" % path )
        
        # find the prototype ID
        result = self.cursor.execute("SELECT id FROM session_meta WHERE name=?", (prototype_name,))
        proto = result.fetchone()[0]        
        
        logging.debug("Prototype ID [%08d]" % proto )        
        
        # force a commit        
        t = real_time()
        self.cursor.execute("INSERT INTO session(start_time, last_time, test_run, extra_config_json, notes, parent, path, meta) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                           (t,
                           t,
                           test_run,
                           json.dumps(extra_config),                           
                           notes,
                           self.session_id, path, proto))
       
        
        # map the session<->run table
        self.cursor.execute("INSERT INTO run_session(session, run) VALUES (?,?)", (self.session_id, self.run_id))
        self.session_id = self.cursor.lastrowid
        
        # set the users for this session
        for user_id, user_data in self.active_users.iteritems():
            self.cursor.execute("INSERT INTO user_session(session, user, role, json) VALUES (?, ?, ?, ?)",
                               (self.session_id,
                               user_id,
                               user_data["role"],
                               user_data["data"]
                               ))
           
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
        t = real_time()
        self.cursor.execute("UPDATE session SET end_time=?, last_time=?, valid=?, complete=? WHERE id=?",
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
            logging.debug("New session ID [%08d]" % self.session_id)
        else:
            self.session_id = None  
            logging.debug("New session ID [NO SESSION]")
        # force a commit
        self.commit()
                           
    def session_states(self):
        """Return the session hierarchy"""
        return tuple(self.session_stack)
    
    def session_names(self):
        """Return the session hierarchy as a list of prototype names, or IDs if there are no prototypes"""    
        return self.session_name_stack
    
    def get_stable_random_seed(self):
        """Return a random seed which is hash of the current session hierarchy"""        
        return self.session_id
    
    def enroll_user(self, name=None, user_vars={}):
        """Enroll a user in the experiment.         
        Parameters:
            name: must be a generated pseudonym from the pseudo module. If None, a random name will be generated.
            user_vars: specifies any per-user variables (e.g demographics, anthropometrics). 
        Returns:
            The user name added.
        """        
        # create new name is none specified -- caller should be sure to
        # log the return value in this case!
        if name is None:
            name = pseudo.get_pseudo()
            logging.debug("Creating new pseudonym '%s'" % name)
            
        # check the name is a valid pseudonym
        name = name.upper()        
        if not pseudo.verify_pseudo(name):
            raise ExperimentException("User pseudonym %s does not verify -- it may be mistyped." % name)                
        
        if self.check_user_exists(name):
            raise ExperimentException("User pseudonym %s already exists in the database" % name)
        
        # insert into the DB
        self.cursor.execute("INSERT INTO users(id, json) VALUES (?, ?)",
                           (name,        
                            json.dumps(user_vars)))
        logging.debug("User '%s' enrolled with state %s" % (name, user_vars))
        return name
        
    def check_user_exists(self, name):
        """Return True if the given pseudonym is registered in the users table."""
        results = self.cursor.execute("SELECT id FROM users WHERE users.id=?", (name,))
        return results.fetchone() 
        
    def add_active_user(self, name, role=None, data=None):
        """Add the specified user to the active user set for the next session."""
        id = self.check_user_exists(name)
        self.active_users[name] = {"role":role, "data":data}
        logging.debug("Added user '%s' to active user list" % name)
        self.user_changed = True
               
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
    
    
    def log(self, stream, t=None, valid=True, data=None, tag=""):
        """Log the given data in the currently active session        
        Parameters:
            stream: stream name to write to
            tag: Tag to use for the stream (optional)
            t: Timestamp of the data. If None, uses the timestamp when the data is written in
            valid: True if this datapoint should be marked as valid, False otherwise
            data: Dictionary of data entries to be written to the log.        
            """    
        t = t or real_time()
        self.cursor.execute("INSERT INTO log(session, valid, time, path, tag, json) VALUES (?, ?, ?, ?, ?, ?)",
                           (self.session_id,
                           valid,
                           t,
                           stream,
                           tag,
                           json.dumps(data)))
                                   
        # update last time in the session table
        self.cursor.execute("UPDATE session SET last_time=? WHERE id=?",
                           (time.time(),                           
                           self.session_id)) 
                           
        # deal with autocommits to the log
        now = real_time()
        if self.autocommit is not None and now - self.last_commit_time > self.autocommit:
            logging.debug("Time-based autocommit")
            self.last_commit_time = now
            self.commit()
            
                       
    
    
if __name__=="__main__":
    e = ExperimentLog(":memory:", experimenter="JHW")    
    p = pseudo.get_pseudo()
    e.enroll_user(p, user_vars={"age":35})
    e.register_session("Experiment1", "EXP", description="Main experiment")
    e.register_session("Condition A", "COND", description="Condition A")
    e.register_session("Condition B", "COND", description="Condition B")
    e.register_session("Condition C", "COND", description="Condition C")
    e.add_active_user(p)
    e.enter_session("Experiment1")
    e.enter_session("Condition B")
    e.log("/sensors/thing1", data={"Stuff":1})
    e.leave_session()
    e.leave_session()
    e.close()