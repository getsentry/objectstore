#!/bin/bash

checks-datadog-monitor-status \
  ${DATADOG_MONITOR_IDS} \
  --skip-check="${SKIP_CANARY_CHECKS}"
