#!/bin/bash
set -e

/opt/flink/bin/jobmanager.sh start
exec /opt/flink/bin/taskmanager.sh start-foreground
