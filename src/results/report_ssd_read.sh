#!/bin/sh

if [ -z "$1" ]; then
	echo "Usage: $0 <workflow name>"
	exit 0
fi

workflow="$1"

function report_mean {
	scheduler=$1
	workflow=$2
	echo "mean read, $workflow : $scheduler"

	for n in `seq 1 6`; do
		nosd=$((2**n))
		#for file in `ls | grep montage | grep $scheduler | grep ^$nosd`; do
		for ntask in `seq 30 10 100`; do
			file="${nosd}_${scheduler}_${workflow}_${ntask}.txt"
			runtime="`cat $file | grep '^SSD mean read' | awk {'print $5'}`"
			echo -e "$nosd\t$ntask\t$runtime"
		done
	done
}

function report_std {
	scheduler=$1
	workflow=$2
	echo "std read, $workflow : $scheduler"

	for n in `seq 1 6`; do
		nosd=$((2**n))
		#for file in `ls | grep montage | grep $scheduler | grep ^$nosd`; do
		for ntask in `seq 30 10 100`; do
			file="${nosd}_${scheduler}_${workflow}_${ntask}.txt"
			runtime="`cat $file | grep '^SSD std read' | awk {'print $5'}`"
			echo -e "$nosd\t$ntask\t$runtime"
		done
	done
}

echo ==============================================================
report_mean rr $workflow
echo ==============================================================
report_mean input-enhanced $workflow
echo ==============================================================
report_std rr $workflow
echo ==============================================================
report_std input-enhanced $workflow


