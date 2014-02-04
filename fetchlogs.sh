#!/bin/bash

# Fetch cc logs
#
SYSNAME=$1
DATE=`date +%F_%H%M`
LOGPATH=/ion-dev/r2/fetch-logs
DAYS_TO_KEEP=5

if [ -z $SYSNAME ]; then
  echo "Usage: $0 [sysname]"
  exit -1
fi


case $SYSNAME in
  r2_full)
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --status' > /tmp/proclist
    ;;
  r2_test_system1)
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/ionalpha.properties --status' > /tmp/proclist
    ;;
  r2_dev)
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/iondev.properties --status' > /tmp/proclist
    ;;
  *)
    echo "sysname invalid"
    exit -1
esac
# get list of hosts, insert ceictl process list command?
cat /tmp/proclist | grep Workspace | awk '{print $5}' | sort -u > /tmp/hostlist

# get one host to perform cleanup
CLEAN_HOST=`head -1 /tmp/hostlist`
ssh root@$CLEAN_HOST -o StrictHostKeyChecking=no "find $LOGPATH/$SYSNAME -maxdepth 1 -type d -mtime +$DAYS_TO_KEEP | xargs rm -rf"

# loop through host list to cp logs
for i in `cat /tmp/hostlist`; do
  ssh root@$i -o StrictHostKeyChecking=no "mkdir -p $LOGPATH/$SYSNAME/$DATE/$i; rsync -rltm --include='*.log' -f 'hide,! */' /home/cc/ $LOGPATH/$SYSNAME/$DATE/$i/"
done