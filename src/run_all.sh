#!/bin/sh

pattern="workflows/$1*.xml"

for n in `seq 2 4`; do
    for workflow in $pattern; do
        nosd=$((n*2))
        scheduler='rr'
        f=`basename $workflow .xml`
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -n $nosd -s $scheduler $workflow > $outfile
        scheduler='input-enhanced'
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -n $nosd -s $scheduler $workflow > $outfile
    done
done

