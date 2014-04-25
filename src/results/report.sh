#!/bin/sh

if [ -z "$1" ]; then
	echo "Usage: $0 <workflow name>"
	exit 0
fi

w=$1

## report runtime
echo "runtime"
echo "nosd,rr,locality,minwait,hostonly,hostreduce"
for i in `seq 2 6`; do
	frr="$((2**i))_rr_${w}_60.txt"
	finput="$((2**i))_input_${w}_60.txt"
	fminwait="$((2**i))_minwait_${w}_60.txt"
	fhostonly="$((2**i))_hostonly_${w}_60.txt"
	fhostreduce="$((2**i))_hostreduce_${w}_60.txt"
	rr=`tail -n1 $frr | awk '{print $4}'`
	input=`tail -n1 $finput | awk '{print $4}'`
	minwait=`tail -n1 $fminwait | awk '{print $4}'`
	hostonly=`tail -n1 $fhostonly | awk '{print $4}'`
	hostreduce=`tail -n1 $fhostreduce | awk '{print $4}'`
	echo $((2**i)),$rr,$input,$minwait,$hostonly,$hostreduce
done

## report utilization
echo "osd utilization"
echo "nosd,rr,locality,minwait,hostonly,hostreduce"
for i in `seq 2 6`; do
	frr="$((2**i))_rr_${w}_60.txt"
	finput="$((2**i))_input_${w}_60.txt"
	fminwait="$((2**i))_minwait_${w}_60.txt"
	fhostonly="$((2**i))_hostonly_${w}_60.txt"
	fhostreduce="$((2**i))_hostreduce_${w}_60.txt"
	rr=`egrep '^OSD mean utilization' $frr | awk '{print $5}'`
	input=`egrep '^OSD mean utilization' $finput | awk '{print $5}'`
	minwait=`egrep '^OSD mean utilization' $fminwait | awk '{print $5}'`
	hostonly=`egrep '^OSD mean utilization' $fhostonly | awk '{print $5}'`
	hostreduce=`egrep '^OSD mean utilization' $fhostreduce | awk '{print $5}'`
	echo $((2**i)),$rr,$input,$minwait
done

## report total data transfer
echo "data transfer"
echo "nosd,rr,locality,minwait,hostonly,hostreduce"
for i in `seq 2 6`; do
	frr="$((2**i))_rr_${w}_60.txt"
	finput="$((2**i))_input_${w}_60.txt"
	fminwait="$((2**i))_minwait_${w}_60.txt"
	fhostonly="$((2**i))_hostonly_${w}_60.txt"
	fhostreduce="$((2**i))_hostreduce_${w}_60.txt"
	rr=`egrep '^Total data' $frr | awk '{print $5}'`
	input=`egrep '^Total data' $finput | awk '{print $5}'`
	minwait=`egrep '^Total data' $fminwait | awk '{print $5}'`
	hostonly=`egrep '^Total data' $fhostonly | awk '{print $5}'`
	hostreduce=`egrep '^Total data' $fhostreduce | awk '{print $5}'`
	echo $((2**i)),$rr,$input,$minwait
done

## report write skewness
echo "write skewness"
echo "nosd,rr,locality,minwait,hostonly,hostreduce"
for i in `seq 2 6`; do
	frr="$((2**i))_rr_${w}_60.txt"
	rrmean=`egrep '^SSD mean write' $frr | awk '{print $5}'`
	rrstd=`egrep '^SSD std write' $frr | awk '{print $5}'`
	rr=`python -c "print float($rrstd)/float($rrmean)*100"`
	finput="$((2**i))_input_${w}_60.txt"
	inputmean=`egrep '^SSD mean write' $finput | awk '{print $5}'`
	inputstd=`egrep '^SSD std write' $finput | awk '{print $5}'`
	input=`python -c "print float($inputstd)/float($inputmean)*100"`
	fminwait="$((2**i))_minwait_${w}_60.txt"
	minwaitmean=`egrep '^SSD mean write' $fminwait | awk '{print $5}'`
	minwaitstd=`egrep '^SSD std write' $fminwait | awk '{print $5}'`
	minwait=`python -c "print float($minwaitstd)/float($minwaitmean)*100"`
	fhostonly="$((2**i))_hostonly_${w}_60.txt"
	hostonlymean=`egrep '^SSD mean write' $fhostonly | awk '{print $5}'`
	hostonlystd=`egrep '^SSD std write' $fhostonly | awk '{print $5}'`
	hostonly=`python -c "print float($hostonlystd)/float($hostonlymean)*100"`
	fhostreduce="$((2**i))_hostreduce_${w}_60.txt"
	hostreducemean=`egrep '^SSD mean write' $fhostreduce | awk '{print $5}'`
	hostreducestd=`egrep '^SSD std write' $fhostreduce | awk '{print $5}'`
	hostreduce=`python -c "print float($hostreducestd)/float($hostreducemean)*100"`
	echo $((2**i)),$rr,$input,$minwait,$hostonly,$hostreduce
done


