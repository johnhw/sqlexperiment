import time
import sqlite3
import json


# import cStringIO
import io

def pretty_json(x):
    return (u"\n"+json.dumps(x, sort_keys=True, indent=4, separators=(',', ': '))+"\n").replace(u"\n", "\n        ")



def string_report(cursor):
    # c = cStringIO.StringIO()
    # c = io.BytesIO()
    c = io.StringIO()
    make_report(cursor, c)
    return c.getvalue()


def string_readme(cursor):
    # c = cStringIO.StringIO()
    # c = io.BytesIO()
    c = io.StringIO()
    make_readme(cursor, c)
    return c.getvalue()


def date_format(t):
    timestruct = time.localtime(t)
    return time.strftime(u"%d %B %Y", timestruct)



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


    meta = json.loads(sqlresult(u"SELECT json FROM dataset WHERE id=(SELECT MAX(id) FROM dataset)"))

    f.write(u"# %s\n" % meta.get(u"title", "---"))
    f.write(u"#### %s\n" % meta.get(u"short_description", ""))
    f.write(u"#### DOI: %s\n" % meta.get(u"doi", ""))
    f.write(u"\n----------------------------------------\n")
    f.write(u"#### Report date: %s\n" % time.asctime())
    f.write(u"\n-----------------------------------------\n")
    f.write(u"#### Authors: %s\n" % meta.get(u"authors", "unknown"))
    f.write(u"#### Institution: %s\n" % meta.get(u"institution", ""))
    f.write(u"#### License: %s\n" % meta.get(u"license", "unknown"))
    f.write(u"----------------------------\n")
    f.write(u"## Confidential: %s\n" % meta.get(u"confidential", "no"))
    f.write(u"----------------------------\n")
    if "funder" in meta:
        f.write(u"#### Funded by: %s\n" % meta["funder"])
    if "paper" in meta:
        f.write(u"#### Associated paper:  %s\n" % meta["paper"])
    if "ethics" in meta:
        f.write(u"#### Ethics board approval number: %s\n" % meta["ethics"])

    f.write(u"----------------------------\n")
    f.write(u"\n\n **Description**  %s \n\n" % meta.get(u"description", ""))

    f.write(u"---------\n")
    start = sqlresult(u"SELECT min(start_time) FROM runs")
    end = sqlresult(u"SELECT max(end_time) FROM runs")

    f.write(u" * Data acquired from %s to %s\n" % (date_format(start), date_format(end)))

    f.write(u"\n----------------------------------------\n")

    f.write(u"* Number of experimental runs: %s\n" % sqlresult(u"SELECT count(id) FROM runs"))
    f.write(u"* Total duration recorded: %.1f seconds\n" % sqlresult(u"SELECT sum(end_time-start_time) FROM runs"))
    f.write(u"* Number of users: %s\n" % sqlresult(u"SELECT count(id) FROM users"))

    f.write(u"* Total logged entries: %d\n" % sqlresult(u"SELECT count(id) FROM log"))
    f.write(u"\n----------------------------------------\n")

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


    f.write(u"# Report generated for %s\n" % fname)
    f.write(u"\n----------------------------------------\n")
    f.write(u"#### Report date: %s\n" % time.asctime())


    f.write(u"\n----------------------------------------\n")
    f.write(u"\n## Runs\n")
    f.write(u"* Number of runs: %s\n" % sqlresult(u"SELECT count(id) FROM runs"))
    f.write(u"* Total duration recorded: %.1f seconds\n" % sqlresult(u"SELECT sum(end_time-start_time) FROM runs"))
    f.write(u"* Dirty exits: %s\n" % sqlresult(u"SELECT count(clean_exit) FROM runs WHERE clean_exit=0"))

    f.write(u"\n----------------------------------------\n")



    f.write(u"\n----------------------------------------\n")
    f.write(u"\n## Users\n")
    f.write(u"* Unique users: %s\n" % sqlresult(u"SELECT count(id) FROM users"))

    for id, name, jsons in cursor.execute(u"SELECT id,name,json FROM users").fetchall():
        f.write(u"\n\n#### %s\n" % name)
        f.write(u"**JSON** \n %s\n" % pretty_json(json.loads(jsons)))
        f.write(u"Duration recorded: %s seconds\n" % sqlresult(u"SELECT sum(session.end_time-session.start_time) FROM session JOIN meta_session ON meta_session.session=session.id JOIN users ON meta_session.meta=users.id WHERE users.id=%d"%id))

    f.write(u"\n----------------------------------------\n")
    f.write(u"\n## Log\n")
    f.write(u"* Log streams recorded: %s\n" % (u",".join(allsqlresult(u"SELECT name FROM stream"))))
    session_types = cursor.execute(u"SELECT id, name, description, json FROM stream").fetchall()
    for id, name, description, jsons in session_types:
        f.write(u"\n#### %s\n" % name)
        f.write(u"##### %s\n" % description)
        f.write(u"**JSON** \n %s\n" % pretty_json(json.loads(jsons)))
        f.write(u"* Total entries: %d\n" % sqlresult(u"SELECT count(id) FROM log WHERE stream=%d"%id))
    f.write(u"\n----------------------------------------\n")


