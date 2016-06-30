## Experiment logger
The logger stores experimental data in a single SQLite database. It is intended to be fast and lightweight, but record all necessary meta data and timestamps for experimental trials.

As a consequence, [SQLiteBrowser](http://sqlitebrowser.org/) can be used to browse the log results without having to do any coding.

Most of the entries are stored as JSON strings in the database tables; any object that can be serialised by Python's `json` module can be added directly.

Timestamps (including synchronising to NTP) are handled automatically. All times are stored as 64-bit floating point seconds since the epoch. The logger can be used across multiple processes using a simple 0MQ proxy which queues messages and passes them to the log database. Tools to extract the data as Pandas DataFrames and to auto-generate basic reports are provided.


### Structure
* **Log** ExperimentLog has a single **master log** which records all logged data as JSON (with a timestamp) in a single series. The log is annotated with different **streams** that represent distinct sensors or inputs.

* **Session** The log is indexed by **sessions**, where a **session** is a logical part of an experiment (a whole experiment, a condition, a repetition, etc.). 

* **Metadata** JSON Metadata about any log *stream, session, run, user* and the whole *dataset* can be recorded in the database, so there is a single, *consistent* record of everything to do with the experimental trials.

### Binding
ExperimentLog uses the idea of **binding** metadata to sessions. So if you have a user who is doing an experiment, you can create a metadata entry for that user, and then *bind* it to the **sessions** that involve that user.

Session structures are hierarchical, and bindings apply to sessions and all of their children; so if a user is bound to an experiment, they are also bound to all the conditions, sub-conditions, repetitions, etc.


### Runs
* **Run** ExperimentLog also tracks **runs** of the experimental software. A run exists from the start of the experimental software until the process exits. Each session can be part of a single run, or a session can be spread over many runs (e.g. if only part of the data is collected at one time).

### Debug logging
The logger also provides a custom logging handler for the standard Python `logging` module (via the `get_logger()` method), so that any debug messages can be stored in the DB and cross-referenced against experimental runs.

### Simplest possible example
ExperimentLog **allows** the addition of metadata and structure, but doesn't mandate it. The simplest example would be something like:

    from experimentlog import ExperimentLog
    # log some JSON data
    e = ExperimentLog(":memory:", ntp_sync=False)
    e.log("mouse", data={"x":0, "y":0})
    e.log("mouse", data={"x":0, "y":1})
    e.log("mouse", data={"x":0, "y":2})
    e.close()

    
### Using paths
A filesystem-like structure is provided to make it easy to separate data:    

    e = ExperimentLog(":memory:", ntp_sync=False)
    e.cd("/Experiment/Condition1")
    e.log("mouse", data={"x":0, "y":0})
    e.log("mouse", data={"x":0, "y":1})
    e.log("mouse", data={"x":0, "y":2})
    e.cd("/Experiment/Condition2")
    e.log("mouse", data={"x":0, "y":0})
    e.log("mouse", data={"x":0, "y":1})
    e.log("mouse", data={"x":0, "y":2})
    e.close()

### A more complex example    

    from experimentlog import ExperimentLog, np_to_str, str_to_np
    import numpy as np
    ## open a connection to a database; will be created if it does not exist.
    # here we use a memory database so the results are not stored to disk
    e = ExperimentLog(":memory:", ntp_sync=False)
    
### Setting up the database
When a log is set up for the first time, the database needs to be configured for the experimental sessions. 

Each sensor/information **stream** can be registered with the database. This could be individual sensors like a mouse (x,y) time series, or questionnaire results.    

        # check if we've already set everything up
        # note we use the special .meta field to access persistent metadata
        if e.meta.stage=="init":
            e.create("STREAM", name="mouse", description="A time series of x,y cursor positions",
                           # the data is optional, and can contain anything you want 
                          data={
                            "sample_rate": 60,
                            "dpi": 3000,
                            "mouse_device":"Logitech MX600"})
            
            # and a post-condition questionnaire
            e.create("STREAM", name="satisfaction", 
                           description="A simple satisfaction score",
                           # here, we store the questions used for future reference
                          data={
                            "questions":["How satisfied were you with your performance?",
                                        "How satisfied were you with the interface?"]}
                            )

## Sessions
**ExperimentLog** uses the concept of *sessions* to manage experimental data. Sessions are much like folders in a filesystem and usually form a hierarchy, for example:
    
    /
        Experiment1/
            ConditionA/
                0/
                1/
                2/
            ConditionB/
                0/
                1/
                2/
                
        Experiment 2
            ConditionA/
                0/
                1/
                2/
                3/
            ConditionC/
                0/
                1/
                2/
                3/
    

Each *session* can have **metadata** attached to it; for example giving the parameters for a given condition. 

When an experiment is run, **instances** of sessions are created, like files inside the filesystem.    

    if e.meta.stage=="init":
        # We'll register an experiment, with three different conditions
        e.create("SESSION", "Experiment", description="The main experiment", 
                               data={"target_size":40.0, "cursor_size":5.0})
        e.create("SESSION","ConditionA",description="Condition A:circular targets", 
                               data={"targets":["circle"]})
        e.create("SESSION","ConditionB", description="Condition B:square targets", 
                               data={"targets":["square"]})
        e.create("SESSION","ConditionC", description="Condition C:mixed targets", 
                               data={"targets":["circle","square"]})

We'd usually only want to do this metadata creation once-ever; this setup procedure can be recorded by changing the database **stage**:                  

                # mark the database as ready to log data
                # meta is a special field that looks like an object, but is actually backed
                # onto the database. Any field can be read or written to, as long as the value
                # can be dumped to JSON
                e.meta.stage="setup"

## Users
Each instance of a session (usually) involves experimental subjects. Each user should be registered, and then attached to a recording session. Multiple users can be attached to one session (e.g. for experiments with groups) but normally there will just be one user.

The `pseudo` module can generate pronounceable, random, verifiable pseudonyms for subjects.

    import pseudo
    user = pseudo.get_pseudo()
    print user
    >>> FAMOV-OGOPU

    # now register the user with the database
    e.create("USER", name=user, data={"age":30, "leftright":"right"})

    
    # note that passing the session="" parameter automatically
    # binds to that session prototype at the start of the session
    e.enter("Experiment", session="Experiment")
    # attach the user to this experiment, and thus to all conditions, etc.
    e.bind("USER", user)
    e.enter("ConditionA", session="ConditionA")

    # calling enter() without any argument creates a numbered repetition (in this case, 0)
    e.enter()
    
    print e.session_path
    print e.bindings

    # log some data
    e.log("mouse", data={"x":0, "y":10})
    e.log("mouse", data={"x":0, "y":20})
    # leave this repetition
    e.leave() 

    # move out of condition A
    e.leave()
    
    # leave Experiment 1
    e.leave()
    
    # end the recording
    e.end()
    
The data is accessible as SQL tables

    # print some results with raw SQL queries
    mouse_log = e.cursor.execute("SELECT time, json FROM mouse", ())
    print "\n".join([str(m) for m in mouse_log.fetchone()])
                