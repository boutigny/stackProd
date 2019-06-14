#!/usr/bin/env python

import os
import time
import sys
import getpass
from optparse import OptionParser
import sqlite3
from sqlite3 import Error

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

def createSubmit(visitList, toWrite, configFile, filt, clobber, rerun, launch) :
    filename = "scripts/" + filt + "/" + visitList + ".list"
    if not os.path.isdir("scripts/" + filt) :
        os.makedirs("scripts/" + filt)

    theFile = open(filename,"w+")
    for line in toWrite:
        print(line, file=theFile)

    cwd = os.getcwd()
    cmd = "jointcal.py " + cwd + "/../../input --rerun "+ rerun +" @" + filename + " --configfile " + configFile
    if clobber:
        cmd = cmd + " --clobber-config --clobber-versions"
    print(cmd)

    dirLog = cwd + "/log/" + filt
    if not os.path.isdir(dirLog) :
        os.makedirs(dirLog)
    log = dirLog + "/" + visitList + ".log"
    if os.path.isfile(log):
        os.remove(log)
    jobName = 'joincal_' + filt
    qsub = "qsub -N " + jobName + " -P P_lsst -q long -l sps=1 -j y -o "+ log + " <<EOF"
    scriptName = "scripts/" + filt + "/" + visitList + ".sh"
    script = open(scriptName,"w")
    script.write(qsub + "\n")
    script.write("#!/usr/local/bin/bash\n")
    script.write(" cd " + cwd + "\n")
    script.write(" source " + cwd + "/../../setup.sh\n")
    script.write(" " + cmd + "\n")
    script.write("EOF" + "\n")
    script.close()
    os.system("chmod +x " + scriptName)
    if launch:
        os.system("./"+scriptName)
        time.sleep(1)

if __name__ == "__main__":

    parser = OptionParser(usage="usage: %prog [options] input - print jobs info before launching them",
                          version="%prog 1.0")
    parser.add_option("-r", "--rerun", type="string", default=getpass.getuser(), help="rerun directory [%default]")
    parser.add_option("-F", "--filter", type="string", default="all", help="Filter [%default]")
    parser.add_option("-c", "--config", type="string", default="jointcalConfig.py", help="Configuration file [%default]")
    parser.add_option("-L", "--launch", action="store_true", help="Launch the jobs if set")
    parser.add_option("--clobber", action="store_true", help="Clobber everything if set")
    (opts, args) = parser.parse_args()

    if not os.path.islink("_parent"):
        sys.exit('_parent link is missing')

    configFile = opts.config
    launch = opts.launch
    clobber = opts.clobber

    # Get list of filters
    db = os.path.join('_parent','input','registry.sqlite3')
    conn = createConnection(db)
    # First get the list of available filters in the registry
    filterList = getFilterList(conn)
    print("The following filters are available in the registry: ",filterList)

    filt = opts.filter

    for myfilt in filterList:
        if filt == "all" or filt == myfilt :
            filename = myfilt + ".list"
            file = open(filename,"r")
            imod = 0
            visitList = ""
            toWrite = []
            flag = 0

            for cnt, line in enumerate(file) :
                line = line[:-1]
                words = line.split()
                visit = words[1].split("=")[1]
                if len(words) == 2 :
                    line = line + " ccd=0..35"
                visitList = visitList + visit + "_"
                toWrite.append(line)
            visitList = visitList[:-1]
            print("Jobs for filter =",myfilt, " & visits =", visitList)
            createSubmit(visitList, toWrite, configFile, myfilt, clobber,
                   opts.rerun,launch)

    if not launch :
        print("----------------------------------------------------------")
        print(" Add option  --launch (or -L)  to launch the jobs, if it looks OK\n")
