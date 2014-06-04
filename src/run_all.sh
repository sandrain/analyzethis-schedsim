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

        # minwait
        scheduler="minwait"
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
            $workflow > $outfile

        # hostonly (10x faster)
        scheduler="hostonly"
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
            -x 10 $workflow > $outfile

        # hostreduce
        scheduler="hostreduce"
        outfile="results/${nosd}_${scheduler}_${f}.txt"
        ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
            -x 1.25 $workflow > $outfile


#        for scheduler in minwait hostonly hostreduce; do
#            outfile="results/${nosd}_${scheduler}_1core_${f}.txt"
#            ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
#                -x 6.25  $workflow > $outfile
#        done
#        for scheduler in minwait hostonly hostreduce; do
#            outfile="results/${nosd}_${scheduler}_8core_${f}.txt"
#            ./sim.py -e -r $runtime -b $netbw -n $nosd -s $scheduler \
#                -x 50  $workflow > $outfile
#        done
    done
done

