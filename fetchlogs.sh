#!/bin/bash

# Fetch cc logs
#
# Requirements:  Your public ssh key needs to exist in the dt-data sshkeys 
# 		 list

SYSNAME=$1
DATE=`date +%F_%H%M`
DAYS_TO_KEEP=5

if [ -z $SYSNAME ]; then
  echo "Usage: $0 [sysname], where sysname in r2_full,r2_test_system1, \
      r3_test_system1,r2_dev,R2_STAGE_SYSTEM,R2_BETA_SYSTEM,R3_BETA_SYSTEM."
  exit -1
fi


case $SYSNAME in
  r2_full)
    LOGPATH=/ion-dev/r2/fetch-logs
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --status' > /tmp/proclist
    ;;
  r2_test_system1)
    LOGPATH=/ion-alpha/r2/fetch-logs
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/ionalpha.properties --status' > /tmp/proclist
    ;;
  r3_test_system1)
    LOGPATH=/ion-alpha/r3/fetch-logs
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/ionalpha_r3.properties --status' > /tmp/proclist
    ;;
  r2_dev)
    LOGPATH=/ion-dev/r2/fetch-logs
    ssh buildbot-runner@buildbot -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/iondev.properties --status' > /tmp/proclist
    ;;
  R2_STAGE_SYSTEM)
    LOGPATH=/ion-stage/r2/fetch-logs
    ssh jenkins@jenkins-pl -o StrictHostKeyChecking=no '~/nimbus-cloud-client-020/bin/cloud-client.sh --conf ~/nimbus-cloud-client-020/conf/ionstage.properties --status' > /tmp/proclist
    ;;
  R2_BETA_SYSTEM)
    LOGPATH=/ion-beta/r2/fetch-logs
    ssh jenkins@pub-4-100.dev -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/ionbeta.properties --status' > /tmp/proclist
    ;;
  R3_BETA_SYSTEM)
    LOGPATH=/ion-beta/r3/fetch-logs
    ssh jenkins@r2-dev1 -o StrictHostKeyChecking=no '~/nimbus-cloud-client-018/bin/cloud-client.sh --conf ~/nimbus-cloud-client-018/conf/r3-beta.properties --status' > /tmp/proclist
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
  ssh root@$i -o StrictHostKeyChecking=no "mkdir -p $LOGPATH/$SYSNAME/$DATE/$i; rsync -rltm --include='*.log*' -f 'hide,! */' /home/cc/ $LOGPATH/$SYSNAME/$DATE/$i/"
done

# find any logs with 600 perms and change to 644
ssh root@$CLEAN_HOST -o StrictHostKeyChecking=no "find $LOGPATH/$SYSNAME/$DATE -perm 600 | xargs chmod 644"

echo "Logs for $SYSNAME written to $LOGPATH/$SYSNAME/$DATE"
