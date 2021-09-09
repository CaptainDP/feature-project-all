#!/usr/bin/env bash

set -e

dataDt=$(date -d "1 day ago" +"%Y%m%d")

THIS_PATH="$(
  cd $(dirname $0)
  pwd -P
)"
CONF_PATH="conf"

JSON="{}"
conf="cmp_tmp_user_ctr.json"
if [[ $# -ge 1 ]]; then
  pre_days="$1"

  if [[ "$1" =~ ^[1-9]+$ ]] && [[ $1 -lt 3650 ]]; then
    dataDt=$(date -d "${pre_days} day ago" +"%Y%m%d")
  else
    dataDt=${pre_days}
  fi
fi

if [[ $# -ge 2 ]]; then
  JSON="$2"
fi
if [[ $# -ge 2 ]]; then
  conf="$3"
fi

echo "cmp_tmp_user_ctr.sh:$0 ${dataDt} ${CONF_PATH}/${conf} ${JSON}"

sh ${THIS_PATH}/../run_online.sh ${dataDt} ${CONF_PATH}/${conf} ${JSON}
