#!/usr/bin/env bash
# ===========================================================================
# spark-env.sh — إعدادات بيئة Spark Standalone Cluster
# ===========================================================================
# يتم نسخ هذا الملف إلى $SPARK_HOME/conf/spark-env.sh على كل عقدة.
# يحدد المتغيرات الأساسية لتشغيل Master و Workers.
# ===========================================================================

# --- عقدة Master ---
export SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master}"
export SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"

# --- عقد Workers ---
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"

# --- Java ---
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"

# --- Python ---
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"

# --- Logging ---
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-/opt/spark/logs}"
