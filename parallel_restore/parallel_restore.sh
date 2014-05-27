#!/bin/bash
SERVER=${1:-"localhost:27017"}
DUMP_DIR=${2}
MAXJOBS=${3:-4}

# set to have job control
set -o monitor
LOGFILE=restore.log
PIPE=.parallel_restore.pipe.tmp
log() {
  echo "[`date`] $*" >> $LOGFILE
}

> $LOGFILE

print_progress() {
  db=${1##*/}
  time=$2

  if [[ "${db}" == "oplog.bson" ]]; then
    echo "Progress: ${db} replayed in $time"
  else
    sleep `expr $RANDOM % 5`
    count=`cat $PIPE`
    count=$((count + 1))
    echo $count > $PIPE
    pct=$((count * 100 / db_total))
    echo "Progress: $count / $db_total (${pct}%) - done ${db} in $time"
  fi
}

restore_db() {
  db=$1
  log " ==> Starting the database restore: ${db}"
  CMD="mongorestore --host ${SERVER} ${db}"
  log "   ${CMD}" >> $LOGFILE
  time_s=$(date +"%s")
  eval ${CMD} >> $LOGFILE 2>&1
  log " restore return code: $?"
  time_e=$(date +"%s")
  eval ${CMD} >> $LOGFILE 2>&1
  time_d=$((time_e-time_s))
  time_t=`echo $((time_d/60))m $((time_d%60))secs`
  log " <== End of database restore: ${db}" 
  print_progress $db "$time_t"
}

restore_oplog() {
  db=$1
  log " ==> Starting the oplog restore: ${db}"
  CMD="mongorestore --oplogReplay --host ${SERVER} ${db}"
  log "   ${CMD}" >> $LOGFILE
  time_s=$(date +"%s")
  eval ${CMD} >> $LOGFILE 2>&1
  log " restore return code: $?"
  time_e=$(date +"%s")
  log " <== End of oplog database restore: ${db}" 
  time_d=$((time_e-time_s))
  time_t=`echo $((time_d/60))m $((time_d%60))secs`
  print_progress "oplog.bson" "$time_t"
}

if [ -z ${SERVER} ] || [ -z ${DUMP_DIR} ]; then
  echo ""
  echo "Error, please specify server, dump directory and max threads"
  echo "-----------------------------------------------------------------------"
  echo "Usage: $0 server:port dump_directory max_threads"
  echo "Example: $0 localhost:27017 ~/dump 2"
  echo "-----------------------------------------------------------------------"
  echo ""
  exit 0
fi

db_total=`find ${DUMP_DIR} -mindepth 1 -maxdepth 1 -type d | wc -l | sed "s/^[ \t]*//g"`

log "Running a mongorestore from ${DUMP_DIR} with ${MAXJOBS} threads into server: ${SERVER} (total dbs: ${db_toal})"

echo "Restoring ${db_total} databases using ${MAXJOBS} threads"

rm -rf $PIPE
echo 0 > $PIPE

time_begin=$(date +"%s")
for db in `find ${DUMP_DIR} -mindepth 1 -maxdepth 1 -type d`
do
  log "Database: ${db}" 
  while [ $(jobs -p | wc -l) -ge $MAXJOBS ]; 
  do
    sleep 5
  done
  restore_db ${db} &
done

log "Waiting for all dumps to finish.."
wait
log "All dumps finished, now restore the oplog at the end..."

oplog=${DUMP_DIR}/oplog.bson

if [ -f ${oplog} ]; then
    log "oplog exists, restoring it by copying to temp oplog directory"
    oplogdir="${DUMP_DIR}/.oplog.tmp"
    mkdir -p ${oplogdir}
    rm -rf ${oplogdir}/*
    cp -r ${oplog} ${oplogdir}/
    restore_oplog ${oplogdir}
    rm -rf ${oplogdir}
else
    log "oplog is missing, skipping it"
fi
time_end=$(date +"%s")
time_diff=$((time_end-time_begin))
time_taken=`echo $((time_diff/60))m $((time_diff%60))secs`

log "Restore completed (total databases restored: `cat $PIPE`) in $time_taken" 
echo "total time taken: ${time_taken}"
rm -rf $PIPE
exit 0
