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

def getTractPatchList(dbConn):
	cur = dbConn.cursor()
	cur.execute("SELECT distinct tract, patch from overlaps order by tract")
	rows = cur.fetchall()
	list = [(i[0], i[1]) for i in rows]
	return list

def createSubmit(visitList, toWrite, configFile, clobber, rerun, launch) :
	filename = "scripts" + "/" + visitList + ".list"
	if not os.path.isdir("scripts") :
		os.makedirs("scripts")

	theFile = open(filename,"w+")
	for line in toWrite:
		print(line, file=theFile)

	cwd = os.getcwd()
	cmd = "multiBandDriver.py " + cwd + "/../../input --rerun "+ rerun +" @" + filename + \
						  " --configfile " + configFile + " --cores 8"
	if clobber:
		cmd = cmd + " --clobber-config --clobber-versions"
	print(cmd)

	dirLog = cwd + "/log"
	if not os.path.isdir(dirLog) :
		os.makedirs(dirLog)
	log = dirLog + "/" + visitList + ".log"
	if os.path.isfile(log):
		os.remove(log)
	jobName = 'multiB'
	qsub = "qsub -N " + jobName + " -P P_lsst -q mc_long -pe multicores 8 -l sps=1 -j y -o "+ log + " <<EOF"
	scriptName = "scripts" + "/" + visitList + ".sh"
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
	parser.add_option("-m", "--mod", type="int", default=10, help="Nbr. of tract/patch per job [%default]")
	parser.add_option("-c", "--config", type="string", default="jointcalConfig.py", help="Configuration file [%default]")
	parser.add_option("-L", "--launch", action="store_true", help="Launch the jobs if set")
	parser.add_option("--clobber", action="store_true", help="Clobber everything if set")
	(opts, args) = parser.parse_args()

	configFile = opts.config
	launch = opts.launch
	clobber = opts.clobber

	# Get list of filters
	db = os.path.join('../..','input','registry.sqlite3')
	conn = createConnection(db)
	# Get the list of available filters in the registry
	filterList = getFilterList(conn)
	print("The following filters are available in the registry: ",filterList)
	idFlt = ""
	for flt in filterList:
		idFlt += flt + "^"
	idFlt = idFlt[:-1]

	overlapDb =  '../1-tract2visit_mapper/overlaps.sqlite3'
	connOverlap = createConnection(overlapDb)

	rows = getTractPatchList(connOverlap)
	toWrite = []
	name = ""
	oldTract = -99
	for cnt, cpl in enumerate(rows):
		tract = cpl[0]
		patch = cpl[1].replace("(","").replace(")","").replace(" ","")
		line = "--id tract=%s patch=%s filter=%s"%(str(tract), patch, idFlt)
		if tract != oldTract:
			name = name + str(tract) + "_" + patch.replace(",", "-") + "_"
			oldTract = tract
		else:
			name = name + patch.replace(",", "-") + "_"
		toWrite.append(line)
		if (cnt+1)%opts.mod == 0:
			name = name[:-1]
			print("multiBand job ", name, "ready to launch")
			createSubmit(name, toWrite, configFile, clobber,
						opts.rerun,launch)
			toWrite = []
			name = ""
			oldTract = -99
	if toWrite:
		name = name[:-1]
		print("multiBand job ", name, "ready to launch")
		createSubmit(name, toWrite, configFile, clobber,
					opts.rerun,launch) 

	if not launch :
		print("----------------------------------------------------------")
		print(" Add option  --launch (or -L)  to launch the jobs, if it looks OK\n")
