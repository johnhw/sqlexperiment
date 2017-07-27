# add cd
import sqlite3
import json
import logging
import time
import cStringIO
import numpy as np
import platform
import traceback
import collections
from ntpsync import check_time_sync
from multiprocessing import RLock
import six

# save/load dictionaries of Numpy arrays from strings
def np_to_str(d):
    c = cStringIO.StringIO()
    np.savez(c,**d)
    return c.getvalue()

# def np_to_str_(d):
#     c = io.BytesIO()
#     np.savez(c,**d)
#     return c.getvalue()

def str_to_np(s):
    c = cStringIO.StringIO(s)
    n = np.load(c)
    return n

# enable logging
logFormatter = logging.Formatter(fmt="%(asctime)s [%(levelname)-5.5s]  %(message)s",
                                 datefmt='%m-%d %H:%M')

rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler("experiment.log")
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)


class ExperimentException(Exception):
    pass

def pretty_json(x):
    return json.dumps(x, sort_keys=True, indent=4, separators=(',', ': '))

class MetaProxy(object):
        """Proxy for accessing whole-dataset metadata"""

        def __init__(self, explog):
            self.__dict__['_explog'] = explog

        def __dir__(self):
            meta = self._explog.get_meta()
            return sorted(list(meta.keys()))

        def __getattr__(self, attr):
            meta = self._explog.get_meta()
            return meta[attr]

        def __setattr__(self, attr, value):
            self._explog.set_meta(**{attr:value})

MetaTuple = collections.namedtuple('MetaTuple', ['mtype', 'name', 'type', 'description', 'json'])


class SQLLogger(logging.Handler):
    """Simple class to direct logging output to the database"""
    def emit(self, record):
        log_entry = self.format(record)
        self.db._insert_log(log_entry, record.levelno)

class ExperimentLog(object):

    def __init__(self, fname, autocommit=None, ntp_sync=True, ntp_servers=None, run_config={}, test_run=False):
        """
        autocommit: If None, never autocommits. If an integer, autocommits every n seconds. If True,
                    autocommits on *every* write (not recommended)
                    """
        logging.debug("Opening database '%s'. Autocommit: '%s'" % (fname, autocommit))

        self.db_lock = RLock()

        with self.db_lock:
            self.conn = sqlite3.connect(fname)
            self.cursor = self.conn.cursor()

            self.time_offset = 0
            if ntp_sync:
                # synchronise the (global) time
                self.time_offset = check_time_sync(n_queries=10, servers=ntp_servers)

            # extend the cache size and disable synchronous writing
            self.execute("PRAGMA cache_size=2000000;")
            self.execute("PRAGMA synchronous=OFF;")

            # allow simple access to the metadata
            self.meta = MetaProxy(self)

            # create the tables
            table_exists = self.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='runs'").fetchone()[0]

            if not table_exists:
                self.create_tables()
            else:
                logging.debug("Tables already created.")

            self.autocommit = autocommit
            self.last_commit_time = self.real_time()
            self.in_run = False
            self.stream_cache = {}
            self.opened = True

            # start in the root session
            root_id = self.execute("SELECT id FROM session where name='[ROOT]'").fetchone()[0]
            self.resume_session(root_id)

            # start the run
            self._start(run_config=run_config, test_run=test_run)




    def resume_session(self, id):
        """Jump into a new session given by the ID"""
        with self.db_lock:
            self.session_id = id
            logging.debug("Resuming from session %d '%s'" % (id,self.session_path))

    @property
    def session_path(self):
        with self.db_lock:
            path = self.execute("SELECT path FROM session where id=?", (self.session_id,)).fetchone()[0]
            return path

    def __enter__(self):
        """Start when using a context-manager"""
        return self

    def __exit__(self, type, value, tb):
        """End when using a context-manager"""
        logging.debug("Exiting: %s", (type, value, tb))
        traceback.print_tb(tb)
        self.close()

    @property
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

        self.execute('''CREATE TABLE IF NOT EXISTS meta
                     (id INTEGER PRIMARY KEY, mtype TEXT, name TEXT, type TEXT, description TEXT, json TEXT, meta INTEGER)''')

        # record variables of a particular experiment or trial
        # It is a real data capture session and so has a definite start_time and end_time
        # The random seeds used should be stored so that all data is reproducible
        # If this is a test run, this should be flagged so that the dataset does not get polluted
        # The config used for the run should be stored in config as a JSON entry
        # Valid determines if this trial represents valid data
        # Complete indicates if the trial was finished successfully
        # notes indicates any special notes for this trial
        # parent indicates the hierarchy of this session
        self.execute('''CREATE TABLE IF NOT EXISTS session
                    (id INTEGER PRIMARY KEY, start_time REAL, end_time REAL,
                    test_run INT, random_seed INT,
                    valid INT, complete INT, description TEXT,
                    json TEXT,  subcount INT,
                    parent INT, path INT, name TEXT,
                    FOREIGN KEY (parent) REFERENCES session(id),
                    FOREIGN KEY (path) REFERENCES meta(id)
                    )''')

        # text tags which are recorded throughout the trial stream
        self.execute('''CREATE TABLE IF NOT EXISTS log
                    (id INTEGER PRIMARY KEY, session INT, valid INT, time REAL, stream INT, tag TEXT, json TEXT, binary INT,
                    FOREIGN KEY(stream) REFERENCES meta(id),
                    FOREIGN KEY(session) REFERENCES session(id),
                    FOREIGN KEY(binary) REFERENCES binary(id))
                    ''')

        self.execute('''CREATE TABLE IF NOT EXISTS binary (id INTEGER PRIMARY KEY, binary BLOB)''')

        self.execute('''CREATE TABLE IF NOT EXISTS sync_ext
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
        # test_run: True if this is a test run
        # start_time and end_time are the times when the software was started and stopped
        # uname records the details of the machine this was run was executed on
        # ntp_clock_offset records the clock offset that was in effect for this run (all timestamps already incorporate this value)
        # json records any per-run configuration
        self.execute('''CREATE TABLE IF NOT EXISTS runs
                    (id INTEGER PRIMARY KEY,
                    start_time REAL,
                    end_time REAL,
                    test_run INT,
                    clean_exit INT,
                    json TEXT,
                    uname TEXT,
                    ntp_clock_offset REAL)
                    ''')

        # maps software runs to experimental sessions
        self.execute('''CREATE TABLE IF NOT EXISTS run_session (id INTEGER PRIMARY KEY, session INT, run INT, FOREIGN KEY(session) REFERENCES session(id), FOREIGN KEY(run) REFERENCES runs(id))''')


        # stores the logging data from the custom handler
        self.execute('''CREATE TABLE IF NOT EXISTS debug_logging (id INTEGER PRIMARY KEY, time REAL, record TEXT, run INT,  level int, FOREIGN KEY(run) REFERENCES runs(id))''')


        # map (many) users/equipment/configs to (many) sessions
        self.execute('''CREATE TABLE IF NOT EXISTS meta_session
                    (id INTEGER PRIMARY KEY, meta INT, session INT,  json TEXT, time REAL, unbound_session INT,
                    FOREIGN KEY(meta) REFERENCES meta(id)
                    FOREIGN KEY(session) REFERENCES session(id)
                    )''')

        # insert the root session
        self.execute('''INSERT INTO session(name, start_time, path, subcount) VALUES (?,?,?,?)''', ("[ROOT]", self.real_time(),'/',0))

        # create the convenience views
        self.execute('''CREATE VIEW IF NOT EXISTS users AS SELECT * FROM meta WHERE mtype="USER"''')
        self.execute('''CREATE VIEW IF NOT EXISTS session_meta AS SELECT * FROM meta WHERE mtype="SESSION"''')
        self.execute('''CREATE VIEW IF NOT EXISTS stream AS SELECT * FROM meta WHERE mtype="STREAM"''')
        self.execute('''CREATE VIEW IF NOT EXISTS path AS SELECT * FROM meta WHERE mtype="PATH"''')
        self.execute('''CREATE VIEW IF NOT EXISTS equipment AS SELECT * FROM meta WHERE mtype="EQUIPMENT"''')
        self.execute('''CREATE VIEW IF NOT EXISTS dataset AS SELECT * FROM meta WHERE mtype="DATASET"''')
        self.meta.stage = "init"

    @property
    def random_seed(self):
        """Return the current random seed"""
        with self.db_lock:
            return self.execute("SELECT random_seed FROM session WHERE id=?", (self.session_id,)).fetchone()[0]

    def set_meta(self, **kwargs):
        """Update the global metadata for this entire dataset"""
        with self.db_lock:
            current = self.get_meta()
            for arg,value in six.iteritems(kwargs):
                current[arg] = value
            self.execute('INSERT INTO meta(json,mtype) VALUES (?, "DATASET")', (json.dumps(current),))


    def get_meta(self):
        """Return the metadata for the entire dataset as a dictionary"""
        with self.db_lock:
            row = self.execute("SELECT json FROM dataset WHERE id=(SELECT MAX(id) FROM dataset)").fetchone()
            if row is not None:
                return json.loads(row[0])
            return {}


    def get_logger(self):
        return self.logger


    def _insert_log(self, record, levelno):
        """Used by the custom logging module to store records into the database"""
        self.execute("INSERT INTO debug_logging(time,record,run, level) VALUES (?, ?, ?, ?)",
                           (self.real_time(),
                           record,
                           self.run_id,
                           levelno
                           ))

    def _start(self, run_config={}, test_run=False):
        """Create a new run entry in the runs table."""
        self.execute("INSERT INTO runs(start_time,clean_exit, ntp_clock_offset, uname, json, test_run) VALUES (?, ?, ?, ?, ?, ?)",
                           (self.real_time(),
                           0,
                           self.time_offset,
                           json.dumps(platform.uname()),
                           json.dumps(run_config),
                           test_run)
                           )
        self.run_id = self.cursor.lastrowid
        logging.debug("Run ID: [%08d]" % self.run_id)
        logging.debug("Run config logged as '%s'" % pretty_json(run_config))
        self.commit()
        self.in_run = True
        self.logger = SQLLogger()
        self.logger.db = self

    def end(self):
        """Update the run entry to mark this as a clean exit and reflect the end time."""
        with self.db_lock:
            logging.debug("Marking end of run [%08d]." % self.run_id)
            self.execute("UPDATE runs SET end_time=?, clean_exit=? WHERE id=?",
                               (self.real_time(),
                               1,
                               self.run_id))
            self.in_run = False

    def sync_ext(self, fname, start_time=None, duration=None, media_start_time=0, time_rate=1.0, description=None, data={}):
        """Synchronise an external file (e.g. a video or audio recording) with the main log file.
        Specify the start_time (in seconds since the epoch, same format as all other times);
        If omitted, uses the current time. time_rate can be used to adjust
        for files that have some time slippage"""
        if start_time==None:
            start_time = self.real_time()
        with self.db_lock:
            logging.debug("Syncing %s to %f:%s (%s) " % (fname, start_time, description))
            self.execute("INSERT INTO sync_ext(fname, start_time, duration, media_start_time, time_rate, description, json) VALUES  (?,?,?,?,?,?)",
                (fname, start_time, duration,  media_start_time, time_rate, description, json.dumps(data)))

    def add_indices(self):
        """Add indices to the log"""
        with self.db_lock:
            self.execute("CREATE INDEX log_session_ix ON log(session)")
            self.execute("CREATE INDEX log_tag_ix ON log(tag)")
            self.execute("CREATE INDEX log_stream_ix ON log(stream)")
            self.execute("CREATE INDEX log_valid_ix ON log(valid)")

    def close(self):
        # auto end the run
        with self.db_lock:
            self.end()
            self.commit()
            logging.debug("Database closed.")
            self.opened = False

    def commit(self):
        """Force all changes to be stored to the database."""
        with self.db_lock:
            logging.debug("<Commit>")
            self.conn.commit()


    def create(self, mtype, name, stype="", description="", data=None, force_update=False):
        """Register a new metadata object."""
        with self.db_lock:
            id = self.find_metatable(mtype, name)
            if id is None:
                logging.debug("Registering '%s' of type '%s', with data [%s]" % (name, mtype, json.dumps(data)))
                self.execute("INSERT INTO meta(name,type,description,json,mtype) VALUES (?,?,?,?,?)", (name, stype, description, json.dumps(data), mtype))
                id = self.cursor.lastrowid
            else:
                if not force_update:
                    raise ExperimentException("%s:%s already exists; not updating" % (mtype,name))
                else:
                    logging.warn("%s:%s exists; force updating" % (mtype,name))
                    self.execute("UPDATE meta SET name=?,type=?,description=?,json=? where meta.id=%d"%id[0], (name, stype, description, json.dumps(data), ))

            if mtype=="STREAM":
                self.stream_cache[name] = id
                self.execute("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM log WHERE stream=%d" % (name, id))

    @property
    def bindings(self):
        with self.db_lock:
            bound = self.execute("SELECT mtype, name, type, description, meta.json FROM meta JOIN meta_session on meta_session.meta=meta.id WHERE meta_session.session=?", (self.session_id,)).fetchall()
            return set([MetaTuple(*meta) for meta in bound])


    def cd(self, path):
        """Jump to the given path, using standard UNIX path name rules (., .., and /).
        Example:
            cd('/Experiment1') # Absolute path
            cd('Experiment1')  # relative path
            cd('..')           # up one session
        """
        with self.db_lock:
            path = path.rstrip('/') # removing trailing slash

            components = path.split('/')
            current_components = self.session_path.rstrip('/').split('/') # skip the trailing slash
            is_absolute = path.startswith('/')
            logging.debug("CD'ing to %s/" % path)
            # jump to the root if we start with a slash
            if is_absolute:
                i = 0
                while i<len(components) and i<len(current_components) and components[i]==current_components[i]:
                    i += 1
                ups = len(current_components) - i
                for k in range(ups):
                    self.leave()
                components = components[i:]


            for component in components:
                if component=='.':
                    # do nothing, this is the current directory
                    pass
                elif component=='..':
                    # up one directory
                    self.leave()
                else:
                    self.enter(component)

            logging.debug("CD'd to %s" % self.session_path)


    def enter(self, name=None, data=None, test_run=False, description="", session=None):
        """Start a new session with the given prototype"""
        t = self.real_time()
        with self.db_lock:
            if not self.in_run:
                raise ExperimentException("No run started; cannot start session")

            # find the parent details
            parent_id = self.session_id
            path = self.session_path
            logging.debug("Parent session %s" % path)

            # find the prototype ID
            if name is None:
                # this is a counted repetition; increment the counter
                result = self.execute("SELECT subcount FROM session WHERE id=?", (self.session_id,))
                sub_id = result.fetchone()[0]
                self.execute("UPDATE session SET subcount=? WHERE id=?", (sub_id + 1,self.session_id))
                name = str(sub_id)

            new_path = path+str(name)+"/"
            logging.debug("Entering session '%s'" % new_path )

            # log this path
            path_id = self.execute("SELECT id FROM path WHERE name=?", (new_path,)).fetchone()
            if path_id is None:
                self.execute("INSERT INTO meta(name,mtype) VALUES (?, 'PATH')", (new_path,))

            # 64 bit random seed
            seed = int(self.real_time() * 1000 * self.session_id) & ((1<<64)-1)

            self.execute("INSERT INTO session(name, start_time,  test_run, json, description, parent, path, complete, subcount, random_seed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (name,
                               t,
                               test_run,
                               json.dumps(data),
                               description,
                               self.session_id, new_path, False, 0, seed))

            self.session_id = self.cursor.lastrowid

            # map the session<->run table
            self.execute("INSERT INTO run_session(session, run) VALUES (?,?)", (self.session_id, self.run_id))

            # bind a passed session, if we got one
            if session is not None:
                id = self.find_metatable("SESSION", session)
                if id is not None:
                    self.execute("INSERT INTO meta_session(meta,session,time) VALUES (?,?,?)", (id[0], self.session_id, self.real_time()))
                else:
                    logging.warn("Tried to bind to a session prototype (%s) that doesn't exist")

            # apply all parent bindings to this new session
            bindings = self.execute("SELECT meta, json FROM meta_session WHERE session=?", (parent_id,)).fetchall()
            if bindings is not None:
                for meta, js in bindings:
                    self.execute("INSERT INTO meta_session(meta,json,session,time) VALUES (?,?,?,?)", (meta, js, self.session_id, self.real_time()))


            logging.debug("New session ID [%08d]" % self.session_id)
            self.commit()

    def last_session(self, include_completed=False):
        """Return the ID of the last incomplete session in the database. If include_completed is True, returns
        the ID of the last session in the database.

        Returns None if there is no matching session.
        """
        with self.db_lock:
            if include_completed:
                result = self.execute("SELECT id FROM session WHERE start_time=(SELECT max(start_time) FROM session)").fetchone()
            else:
                result = self.execute("SELECT id FROM session WHERE start_time=(SELECT max(start_time) FROM session) AND complete=0").fetchone()
            if result==None:
                return None
            else:
                return result[0]


    def find_metatable(self, mtype, name):
        with self.db_lock:
            return self.execute("SELECT id FROM meta WHERE name=? AND mtype=?", (name,mtype)).fetchone()

    def bind(self, mtype, name, data={}):
        with self.db_lock:
            id = self.find_metatable(mtype, name)
            if id is not None:
                self.execute("INSERT INTO meta_session(meta, session, time, json) VALUES (?,?,?,?)", (id[0], self.session_id, self.real_time(), json.dumps(data)))
                logging.debug("Binding meta table %s:%s to %s" % (mtype, name, self.session_path))
            else:
                logging.warn("Tried to bind non-existent meta table %s:%s" % (mtype, name))

    def unbind(self, mtype, name):
        with self.db_lock:
            id = self.find_metatable(mtype, name)
            if id is not None:
                self.execute("UPDATE meta_session SET  unbound_session=session session=NULL WHERE (meta=? AND session=?)", (id[0],self.session_id))
                logging.debug("Unbinding meta table %s:%s from %s" % (mtype, name, self.session_path))
            else:
                logging.warn("Tried to unbind non-existent meta table %s:%s" % (mtype, name))


    def leave(self, complete=True, valid=True):
        """Stop the current session, marking according to the flags."""
        with self.db_lock:
            logging.debug("Leaving session '%s'" % self.session_path)
            t = self.real_time()
            parent_id = self.execute("SELECT parent FROM session WHERE id=?", (self.session_id,)).fetchone()[0]
            if parent_id is None:
                logging.warn("Tried to leave the root session.")
            else:
                self.execute("UPDATE session SET end_time=?,  valid=?, complete=? WHERE id=?",
                               (t,
                               valid,
                               complete,
                               self.session_id))
                self.session_id = parent_id
            # force a commit
            self.commit()


    def execute(self, query, parameters=()):
        with self.db_lock:
            return self.cursor.execute(query, parameters)

    def log(self, stream, t=None, valid=True, data=None, tag="", binary=None):
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
        t = t or self.real_time()
        with self.db_lock:
            if not self.in_run:
                raise ExperimentException("No run active; cannot log data")


            # check if we already know what kind of stream this is; otherwise,
            # look it up from the DB and cache it for later use
            if stream in self.stream_cache:
                stream_id = self.stream_cache[stream]
            else:
                stream_id = self.execute("SELECT id FROM stream WHERE stream.name=?", (stream,)).fetchone()
                # if there is no such stream ID, create a new one and use that
                if stream_id is None:
                    logging.warn("No stream %s registered; creating a new blank entry" % stream)
                    self.create("STREAM", stream, stype="AUTO")
                    stream_id = self.execute("SELECT id FROM stream WHERE stream.name=?", (stream,)).fetchone()
                stream_id = stream_id[0]
                self.stream_cache[stream] = stream_id

            # attach binaries if needed
            if binary is not None:
                self.execute("INSERT INTO binary(binary) VALUES (?)", (binary,))
                binary_id = self.cursor.lastrowid
            else:
                binary_id = None

            self.execute("INSERT INTO log(session, valid, time, stream, tag, json, binary) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (self.session_id,
                               valid, t, stream_id, tag,
                               json.dumps(data), binary_id))

            id = self.cursor.lastrowid

             # deal with autocommits to the log
            now = self.real_time()
            if self.autocommit is not None and now - self.last_commit_time > self.autocommit:
                logging.debug("Time-based autocommit")
                self.last_commit_time = now
                self.commit()

            return id


if __name__=="__main__":
    from sqlexperiment import pseudo
    with ExperimentLog("my.db", ntp_sync=False) as e:

        print(e.meta.stage)
        if e.meta.stage=="init":
            e.create("STREAM", "sensor_1")
            e.create("SESSION", "Experiment1", description="Main experiment")
            e.meta.stage="setup"

        p = pseudo.get_pseudo()
        e.create("USER", p, data={"age":35})


        e.enter("Experiment1", session="Experiment1")
        e.bind("USER", p)
        e.enter("Condition B")
        print(e.bindings)
        e.cd("/Experiment1")
        e.enter()
        e.log("sensor_2", data={"Stuff":1})
        e.leave()
        e.enter()
        e.log("sensor_2", data={"Stuff":1})
        e.leave()
        e.cd('/')



