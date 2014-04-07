#!/bin/sh

for app in cybershake montage sipht ligo genome; do
	./generate.sh $app 50
#	for i in `seq 3 10`; do
#		./generate.sh $app $((i*10))
#	done
done

