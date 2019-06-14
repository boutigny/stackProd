#!/bin/bash

# Wrapup script to populate a sqlite3 DB with tract,patch - visit mapping
# First argument should be the input rerun directory

if [ "$1" = "" ]; then
	echo "Input rerun directory is missing"
	exit 99
fi

if [ -f filters.list ]; then
   rm filters.list
fi
touch filters.list
for filename in ../0-singleFrameDriver/*.list; do
	cat $filename >> filters.list
	sed -e "s/--id visit=//" filters.list > tmp.tmp ; mv tmp.tmp filters.list
done

./tract2visit_mapper.py --visits filters.list --indir ../../"$1"