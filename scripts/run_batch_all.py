#!/usr/bin/env python

import os
import time
import sys
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

def submit(visitList, toWrite, configFile, filt) :
    filename = "lists/" + filt + "/" + visitList + ".list"
    if not os.path.isdir("lists/" + filt) :
        os.makedirs("lists/" + filt)
        
    theFile = open(filename,"w+")
    for line in toWrite:
        print(line, file=theFile)
    cmd = "processCcd.py input --output "+ outputDir +" @" + filename + " --configfile " + configFile + " --clobber-config -j 8 --clobber-versions --timeout 999999999"
    print(cmd)
    
    cwd = os.getcwd()
    dirLog = cwd + "/log/" + filt
    if not os.path.isdir(dirLog) :
        os.makedirs(dirLog)
    log = dirLog + "/" + visitList + ".log"
    print(log)
    qsub = "qsub -P P_lsst -q mc_long -pe multicores 8 -l sps=1 -j y -o "+ log + " <<EOF"
    scriptName = "lists/" + filt + "/" + visitList + ".sh"
    script = open(scriptName,"w")
    script.write(qsub + "\n")
    script.write("#!/usr/local/bin/bash\n")
    script.write(" cd " + cwd + "\n")
    script.write(" source setup.sh\n")
    script.write(" " + cmd + "\n")
    script.write("EOF" + "\n")
    script.close()
    os.system("chmod +x " + scriptName)
    os.system("./"+scriptName)
    time.sleep(1)

if __name__ == "__main__":

    parser = OptionParser(usage="usage: %prog [options] input - print jobs info before launching them",
                          version="%prog 1.0")
    parser.add_option("-O", "--output", type="string", default="output", help="Output [%default]")
    parser.add_option("-F", "--filter", type="string", default="all", help="Filter [%default]")
    parser.add_option("-m", "--mod", type="int", default=10, help="Nbr. of visits per job [%default]")
    parser.add_option("-M", "--max", type="int", default=999, help="Max nbr of jobs to be submitted [%default]")
    parser.add_option("-c", "--config", type="string", default="myProcessCcd.py", help="Configuration file [%default]")
    parser.add_option("-L", "--launch", type="int", default="0", help="Launch the jobs [%default]")
    (opts, args) = parser.parse_args()

    #take the list from makeVisitList.py
    db = os.path.join(args[0],'registry.sqlite3')
    conn = createConnection(db)
    # First get the list of available filters in the registry
    filterList = getFilterList(conn)

    filt = opts.filter

    if filt != "all":
        if filt not in filterList :
            print("filter %s not in registry"%filt)
            exit(99)
 
    modularity = opts.mod
    maxVisit = opts.max
    configFile = opts.config
    outputDir = opts.output
    launch_go = opts.launch

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
                if cnt > maxVisit-1 :
                    break
                if imod < modularity :
                    visitList = visitList + visit + "_"
                    toWrite.append(line)
                    imod += 1
                else :
                    visitList = visitList[:-1]
                    print("Job for filter =",myfilt, " & visits =", visitList)
                    if launch_go :
                        submit(visitList, toWrite, configFile, myfilt)
                    toWrite = []
                    toWrite.append(line)
                    visitList = visit + "_"
                    imod = 1
                        
            visitList = visitList[:-1]
            print("Job for filter =",myfilt, " & visits =", visitList)
            if launch_go :
                submit(visitList, toWrite, configFile, myfilt)
            
    if launch_go == 0 :
        print("----------------------------------------------------------")
        print(" Add option  --launch 1  to launch the jobs, if it looks OK")
                        
	
	
	
