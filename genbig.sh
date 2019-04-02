#!/usr/bin/env bash

mkdir -p /mnt/efs/pipetest3
./gendata.py /mnt/efs/pipetest3/bigpipe -f 12 -c 10 -r 500000000 -z -t bigpipe
