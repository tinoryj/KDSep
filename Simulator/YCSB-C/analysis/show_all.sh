#!/bin/bash

DR=`dirname $0`
${DR}/show_reads.sh $*
${DR}/show_writes.sh $*
${DR}/show_cache.sh $*
