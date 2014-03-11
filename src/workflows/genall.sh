#!/bin/sh

for app in cybershake montage sipht ligo genome; do
	for i in `seq 3 10`; do
		./generate.sh $app $((i*10))
	done
done

