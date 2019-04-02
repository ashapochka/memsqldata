#!/usr/bin/env bash

mkdir -p /mnt/efs/pipetest2
./gendata.py /mnt/efs/pipetest2/medpipe -f 4 -c 10 -r 100000000 -z -t medpipe
