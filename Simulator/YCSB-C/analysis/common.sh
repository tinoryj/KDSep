#!/bin/bash

concatFunc() {
    for t in $*; do
	if [[ `echo $t | grep "[a-zA-Z]" | wc -l` -ne 0 ]]; then
	    if [[ `echo $t | sed 's/e//g' | grep "[a-zA-Z]" | wc -l` -eq 0 ]]; then
		num=`echo $t | awk '{if ($1>0 && $1<0.00001) { t=sprintf("%.1e", $1); print t;} else printf "%f", $1;}' | cut -c1-7 | sed 's/.00000//g'`
	    else
		num=$t
	    fi
	else 
	    num=`echo $t | awk '{if ($1>0 && $1<0.00001) sprintf("%.1e", $1); else printf "%f", $1;}' | cut -c1-7 | sed 's/.00000//g'`
	fi
	printf "%s\t" $num
    done
    printf "\n"
}
