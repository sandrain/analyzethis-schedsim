#!/bin/sh

if [ $# -ne 2 ]; then
	echo "Usage: $0 <Application type> <Node count>"
	echo
	echo "Supported application types:"
	echo " - CYBERSHAKE, MONTAGE, SIPHT, LIGO, GENOME"
	echo
	exit
fi

apptype="$1"
count="$2"

outfile="${apptype}_${count}.xml"

gen/bin/AppGenerator -a $apptype -- -n $count > $outfile

