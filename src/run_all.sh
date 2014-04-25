#!/bin/sh

pattern="workflows/$1*.xml"
netbw="$((250*(1<<20)))"    # the default is 250MB/s
runtime="1"

ls $pattern 2>/dev/null
if [ $? -ne 0 ]; then
	echo "workflow not found"
	exit 1
fi

for n in `seq 1 6`; do
    for workflow in $pattern; do
        nosd=$((2**n))
        f=`basename $workflow .xml`

        for scheduler in rr locality minwait hostonly hostreduce; do
            outfile="results/${nosd}_${scheduler}_${f}.txt"
            ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
                $workflow > $outfile
        done
    done
done

