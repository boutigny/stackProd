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

def getPatchList(dbConn, tract):
	cur = dbConn.cursor()
	cur.execute("SELECT distinct patch from overlaps where tract='%s'"%tract)
	rows = cur.fetchall()
	list = [(i[0]).replace("(","").replace(")","").replace(" ","") for i in rows]
	return list

def makeChunks(l, n):
    # Slit a list in nchunks 
    # https://chrisalbon.com/python/data_wrangling/break_list_into_chunks_of_equal_size
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def createSubmit(tract, patches, rerun, launch) :
	if not os.path.isdir("scripts/" + str(tract)) :
		os.makedirs(os.path.join("scripts", str(tract)))

	script = '../../../my_packages/DC2-production/scripts/make_object_catalog.py'
	# create file name form patch list
	patchFileName = patches.replace(",", "-").replace("^", "_")

	cwd = os.getcwd()
	cmd = "python "+ script + " "  + cwd + "/../../input/rerun/" + rerun + " " + str(tract) + \
						  " --patch " + patches + \
						  " --output-dir " + cwd + "/../../input/rerun/" + rerun + "/dpdd" + \
						  " --overwrite"

	dirLog = os.path.join(cwd, "log", str(tract))
	if not os.path.isdir(dirLog) :
		os.makedirs(dirLog)
	log = dirLog + "/" + patchFileName + ".log"
	if os.path.isfile(log):
		os.remove(log)
	jobName = 'objCat'
	qsub = "qsub -N " + jobName + " -P P_lsst -q long -l sps=1 -j y -o "+ log + " <<EOF"
	scriptName = "scripts/" + str(tract) + "/" + patchFileName + ".sh"
	script = open(scriptName,"w")
	script.write(qsub + "\n")
	script.write("#!/usr/local/bin/bash\n")
	script.write(" cd " + cwd + "\n")
	script.write(" source " + cwd + "/../../setup.sh\n")
	script.write(" source /pbs/throng/lsst/software/desc/setup.sh\n")
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
	parser.add_option("-m", "--mod", type="int", default=5, help="Nbr. of tract/patch per job [%default]")
	parser.add_option("-t", "--tract", type="int", help="Tract number")
	parser.add_option("-L", "--launch", action="store_true", help="Launch the jobs if set")
	(opts, args) = parser.parse_args()

	launch = opts.launch
	tract = opts.tract

	overlapDb =  '../1-tract2visit_mapper/overlaps.sqlite3'
	connOverlap = createConnection(overlapDb)

	patchList = getPatchList(connOverlap, tract)
	chunks = list(makeChunks(patchList,opts.mod))

	for chunk in chunks:
		patchArg = ''
		for patch in chunk:
			patchArg = patchArg + '^' + patch
		patchArg = patchArg[1:]

		createSubmit(tract, patchArg, opts.rerun, launch)

	if not launch :
		print("----------------------------------------------------------")
		print(" Add option  --launch (or -L)  to launch the jobs, if it looks OK\n")
