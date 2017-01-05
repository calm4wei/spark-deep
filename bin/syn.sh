#!/usr/bin/env bash

HOSTNAME=`hostname`
USER=`whoami`
SRC=$1
DETH=$2

for i in `cat /etc/hosts | grep -E 'datacube2' | grep -v "$HOSTNAME" | awk -F " " '{print $1}'`
do
    scp -r "$1" "$USER"@"$i":"$2"
done
