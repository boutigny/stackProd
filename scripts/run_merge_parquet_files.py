#!/usr/bin/env python

import os
import time
import sys
import getpass
from optparse import OptionParser

def createSubmit(tract, rerun, launch) :
	if not os.path.isdir("scripts") :
		os.makedirs(os.path.join("scripts"))

	script = '../../../my_packages/DC2-production/scripts/merge_parquet_files.py'

	cwd = os.getcwd()
	cmd = "python "+ script + " "  + cwd + "/../../input/rerun/" + rerun + "/dpdd/object_" + str(tract) + "_*.parquet"\
						  " -o " + cwd + "/../../input/rerun/" + rerun + "/dpdd/object_tract_" + str(tract) + ".parquet" \
						  " --sort-input-files"

	dirLog = os.path.join(cwd, "log")
	if not os.path.isdir(dirLog) :
		os.makedirs(dirLog)
	log = dirLog + "/" + str(tract) + ".log"
	if os.path.isfile(log):
		os.remove(log)
	jobName = 'MrgPqt'
	qsub = "qsub -N " + jobName + " -P P_lsst -q long -l sps=1 -j y -o "+ log + " <<EOF"
	scriptName = "scripts/" + str(tract) + ".sh"
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
	parser.add_option("-t", "--tract", type="int", help="Tract number")
	parser.add_option("-L", "--launch", action="store_true", help="Launch the jobs if set")
	(opts, args) = parser.parse_args()

	launch = opts.launch
	tract = opts.tract

	createSubmit(tract, opts.rerun, launch)

	if not launch :
		print("----------------------------------------------------------")
		print(" Add option  --launch (or -L)  to launch the jobs, if it looks OK\n")
