import sqlite3
import time
import os, sys

if len(sys.argv)==3:
    in_db = sys.argv[1]
    conn = sqlite3.connect(in_db)
    print("Dumping %s to %s" % (sys.argv[1], sys.argv[2]))
    with open(sys.argv[2], "w") as f:
        for l in conn.iterdump():
            f.write(l)
else:
    print "Usage: dump.py <in_db> <out_text>"

