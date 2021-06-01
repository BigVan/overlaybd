rmdir /sys/kernel/config/target/core/user_123/vol1

for i in $( seq 1 7 )
do
   rm -rf /opt/overlaybd/test/$i;
done