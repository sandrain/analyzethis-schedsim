#!/bin/sh

if [ -z "$1" ]; then
	echo "Usage: $0 <workflow name>"
	exit 0
fi

workflow="$1"

function report {
	scheduler=$1
	workflow=$2
	echo $workflow : $scheduler

	for n in `seq 2 16`; do
		nosd=$((n*2))
		echo
		#for file in `ls | grep montage | grep $scheduler | grep ^$nosd`; do
		for ntask in `seq 30 10 100`; do
			file="${nosd}_${scheduler}_${workflow}_${ntask}.txt"
			runtime="`tail -n1 $file | awk '{print $4}'`"
			echo -e "$nosd\t$ntask\t$runtime"
		done
	done
}

echo ==============================================================
report rr $workflow
echo ==============================================================
report input-enhanced $workflow

