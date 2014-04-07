#!/bin/sh

pattern="workflows/$1*.xml"
netbw="$((1*104857600))"
runtime="1"

for n in `seq 1 6`; do
    for workflow in $pattern; do
        nosd=$((2**n))
        scheduler='rr'
        f=`basename $workflow .xml`
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
		$workflow > $outfile
        scheduler='input'
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
		$workflow > $outfile
    done
done

