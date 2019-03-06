#!/usr/bin/env python
"""
Utility script to automatically create the lists of visits corresponding to
a given filter (or to all of them) in a format suitable for the DM stack
The list of visits is extracted from the registry database.
"""

import sys
import os
import sqlite3
from sqlite3 import Error
from optparse import OptionParser

def createConnection(dbFile):
    try:
        conn = sqlite3.connect(dbFile)
        return conn
    except Error as e:
        sys.exit(e)
    return None

def getFilterList(dbConn):
    cur = dbConn.cursor()
    cur.execute("SELECT DISTINCT filter from raw_visit")
    rows = cur.fetchall()
    list = [i[0] for i in rows]
    return list

def createList(dbConn, filt):
    cur = dbConn.cursor()
    cur.execute("SELECT visit from raw_visit WHERE filter like '"+filt+"%'")
    rows = cur.fetchall()
    list = [i[0] for i in rows]
    return list

def createIdFile(visitList, fileName):
    fd = open(fileName,"w+")
    for vis in visitList:
        st = "--id visit=" + str(vis) + "\n"
        fd.write(st)
    fd.close()
    return None

def main():
    parser = OptionParser(usage="usage: %prog [options] input",
                          version="%prog 1.0")
    parser.add_option("-F", "--filter",
                        type="string",
                        default="all",
                        help="Filter [%default]")
    (opts, args) = parser.parse_args()
    if len(args) != 1:
        parse.error("Wrong number of arguments")

    db = os.path.join(args[0],'registry.sqlite3')
    filt = opts.filter

    conn = createConnection(db)

    # First get the list of available filters in the registry
    filterList = getFilterList(conn)
    print("Fthe following filters are available in the registry: ",filterList)

    if filt != "all":
        if filt not in filterList:
            print("filter %s not in registry"%filt)
            exit(99)
        visitList = createList(conn, filt)
        createIdFile(visitList, filt + ".list")
    else:
        for f in filterList:
            visitList = createList(conn, f)
            createIdFile(visitList, f + ".list")

if __name__ == '__main__':
    main()
