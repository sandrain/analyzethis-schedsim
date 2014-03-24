#!/bin/sh

pattern="workflows/$1*.xml"

for n in `seq 1 6`; do
    for workflow in $pattern; do
        nosd=$((2**n))
        scheduler='rr'
        f=`basename $workflow .xml`
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -n $nosd -s $scheduler $workflow > $outfile
        scheduler='input-enhanced'
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -n $nosd -s $scheduler $workflow > $outfile
    done
done

