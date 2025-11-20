#!/bin/bash

DATADOG_MONITOR_IDS="238383701"

checks-datadog-monitor-status \
  ${DATADOG_MONITOR_IDS} \
  --skip-check="${SKIP_CANARY_CHECKS}"
