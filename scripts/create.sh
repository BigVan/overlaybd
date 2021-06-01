#!/bin/bash

for i in $( seq 1 7 )
do
   mkdir -p /opt/overlaybd/test/$i/fs;
done

mkdir -p /sys/kernel/config/target/core/user_123/vol1
echo -n dev_config=overlaybd/$(pwd)/config.json > /sys/kernel/config/target/core/user_123/vol1/control
echo -n 1 > /sys/kernel/config/target/core/user_123/vol1/enable



