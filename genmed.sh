#!/usr/bin/env bash

mkdir -p /mnt/efs/pipetest2
./gendata.py /mnt/efs/pipetest2/medpipe -f 10 -c 10 -r 10000000 -z -t medpipe
