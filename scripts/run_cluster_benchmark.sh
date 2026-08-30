#!/usr/bin/env bash
# ===========================================================================
# سكربت المقارنة: Local Mode vs Cluster Mode
# ===========================================================================
#
# يقارن أداء معالجة 1M+ سجل بين:
#   1. الوضع المحلي (local[*])
#   2. الوضع الموزع (spark://spark-master:7077)
#
# الاستخدام:
#   bash scripts/run_cluster_benchmark.sh
#   bash scripts/run_cluster_benchmark.sh --rows 500000
#
# المتطلبات:
#   - Docker Compose cluster يعمل (bash scripts/cluster_start.sh)
#   - ملف البيانات موجود (أو سيُنشأ تلقائياً)
#
# ===========================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

ROWS="${1:-1000000}"
DATA_FILE="data/orders_1m.csv"
REPORT_FILE="reports/cluster_benchmark.md"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   📊 مقارنة الأداء: Local vs Cluster                        ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# -----------------------------------------------------------------------
# 1. التحقق من ملف البيانات
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[1/5] التحقق من ملف البيانات...${NC}"
if [ ! -f "$DATA_FILE" ]; then
    echo -e "${YELLOW}   ⏳ توليد $ROWS سجل...${NC}"
    python3 scripts/generate_million_records.py --rows "$ROWS" --output "$DATA_FILE"
fi

FILE_SIZE=$(du -h "$DATA_FILE" | cut -f1)
LINE_COUNT=$(wc -l < "$DATA_FILE")
RECORD_COUNT=$((LINE_COUNT - 1))
echo -e "${GREEN}✅ ملف البيانات: $DATA_FILE ($FILE_SIZE, $RECORD_COUNT سجل)${NC}"

# -----------------------------------------------------------------------
# 2. التحقق من الكلاستر
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[2/5] التحقق من حالة الكلاستر...${NC}"
if curl -s http://localhost:8080 &>/dev/null; then
    echo -e "${GREEN}✅ Spark Cluster يعمل${NC}"
    CLUSTER_AVAILABLE=true
else
    echo -e "${RED}⚠️  Spark Cluster غير متاح. سيتم تشغيل الوضع المحلي فقط.${NC}"
    echo -e "${YELLOW}   شغّل الكلاستر أولاً: bash scripts/cluster_start.sh${NC}"
    CLUSTER_AVAILABLE=false
fi

# -----------------------------------------------------------------------
# 3. تشغيل الوضع المحلي (local[*])
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[3/5] تشغيل المعالجة بالوضع المحلي (local[*])...${NC}"
echo -e "   $RECORD_COUNT سجل..."

# تنظيف قاعدة البيانات قبل التشغيل
python3 -c "
from pymongo import MongoClient
c = MongoClient('mongodb://localhost:27017')
for col in ['orders_raw','orders_validated','orders_quarantine','pipeline_idempotency_keys']:
    c.orders_pipeline[col].drop()
print('   ✅ تم تنظيف قاعدة البيانات')
" 2>/dev/null || echo "   ⚠️ تعذر تنظيف MongoDB (قد يكون غير متصل محلياً)"

LOCAL_START=$(date +%s%N)
SPARK_MASTER="local[*]" \
MONGO_URI="mongodb://localhost:27017" \
python3 src/main.py --input "$DATA_FILE" --force-engine pyspark 2>&1 | tee /tmp/benchmark_local.log
LOCAL_END=$(date +%s%N)

LOCAL_SECONDS=$(( (LOCAL_END - LOCAL_START) / 1000000000 ))
LOCAL_MS=$(( (LOCAL_END - LOCAL_START) / 1000000 ))
echo -e "\n${GREEN}✅ الوضع المحلي: ${LOCAL_SECONDS}s (${LOCAL_MS}ms)${NC}"

# -----------------------------------------------------------------------
# 4. تشغيل الوضع الموزع (spark://spark-master:7077)
# -----------------------------------------------------------------------
if [ "$CLUSTER_AVAILABLE" = true ]; then
    echo -e "\n${YELLOW}[4/5] تشغيل المعالجة بالوضع الموزع (Cluster)...${NC}"
    echo -e "   $RECORD_COUNT سجل عبر spark://spark-master:7077..."

    # تنظيف قاعدة البيانات (عبر Docker MongoDB)
    docker exec spark-cluster-mongodb mongosh --eval "
        use orders_pipeline;
        db.orders_raw.drop();
        db.orders_validated.drop();
        db.orders_quarantine.drop();
        db.pipeline_idempotency_keys.drop();
        print('تم التنظيف');
    " 2>/dev/null || true

    CLUSTER_START=$(date +%s%N)

    # تشغيل spark-submit داخل الكلاستر
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --jars /opt/spark/pipeline-jars/mongo-spark-connector_2.13-10.4.0.jar,/opt/spark/pipeline-jars/bson-4.8.2.jar,/opt/spark/pipeline-jars/bson-record-codec-4.8.2.jar,/opt/spark/pipeline-jars/mongodb-driver-core-4.8.2.jar,/opt/spark/pipeline-jars/mongodb-driver-sync-4.8.2.jar \
        --conf "spark.mongodb.read.connection.uri=mongodb://spark-cluster-mongodb:27017/orders_pipeline.orders_raw" \
        --conf "spark.mongodb.write.connection.uri=mongodb://spark-cluster-mongodb:27017/orders_pipeline" \
        --conf "spark.executor.memory=2g" \
        --conf "spark.driver.memory=2g" \
        --py-files /opt/spark/pipeline-src/*.py \
        /opt/spark/pipeline-src/main.py \
        --input /opt/spark/pipeline-data/orders_1m.csv \
        --force-engine pyspark 2>&1 | tee /tmp/benchmark_cluster.log

    CLUSTER_END=$(date +%s%N)
    CLUSTER_SECONDS=$(( (CLUSTER_END - CLUSTER_START) / 1000000000 ))
    CLUSTER_MS=$(( (CLUSTER_END - CLUSTER_START) / 1000000 ))
    echo -e "\n${GREEN}✅ الوضع الموزع: ${CLUSTER_SECONDS}s (${CLUSTER_MS}ms)${NC}"
else
    CLUSTER_SECONDS="N/A"
    CLUSTER_MS="N/A"
fi

# -----------------------------------------------------------------------
# 5. توليد تقرير المقارنة
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[5/5] توليد تقرير المقارنة...${NC}"
mkdir -p reports

cat > "$REPORT_FILE" << EOF
# 📊 تقرير مقارنة الأداء: Local vs Cluster Mode

## معلومات التشغيل
| المعلومة | القيمة |
|---|---|
| ملف البيانات | \`$DATA_FILE\` |
| حجم الملف | $FILE_SIZE |
| عدد السجلات | $RECORD_COUNT |
| تاريخ التشغيل | $(date '+%Y-%m-%d %H:%M:%S') |

## نتائج المقارنة

| الوضع | الزمن (ثانية) | الزمن (مللي ثانية) | معدل المعالجة (سجل/ثانية) |
|---|---|---|---|
| **Local [\*]** | ${LOCAL_SECONDS}s | ${LOCAL_MS}ms | ~$(( RECORD_COUNT / (LOCAL_SECONDS > 0 ? LOCAL_SECONDS : 1) )) rec/s |
| **Cluster (2 Workers)** | ${CLUSTER_SECONDS}s | ${CLUSTER_MS}ms | $([ "$CLUSTER_SECONDS" != "N/A" ] && echo "~$(( RECORD_COUNT / (CLUSTER_SECONDS > 0 ? CLUSTER_SECONDS : 1) )) rec/s" || echo "N/A") |

## تحليل النتائج

### الوضع المحلي (local[*])
- يستخدم جميع أنوية المعالج على جهاز واحد
- لا يوجد عبء شبكة (network overhead)
- مناسب للملفات الصغيرة والمتوسطة

### الوضع الموزع (Spark Standalone Cluster)
- يوزع العمل على Workers متعددة
- يوجد عبء إضافي لتنسيق المهام والشبكة
- يتفوق عند زيادة حجم البيانات وعدد العقد
- **Executors**: 2 عمال × 2 أنوية = 4 أنوية متوازية
- **Memory**: 2 عمال × 2GB = 4GB ذاكرة تنفيذ

## ملاحظات
- المقارنة على نفس الجهاز الفعلي (Docker containers) قد لا تُظهر فرقاً كبيراً
  لأن الموارد الفعلية مشتركة. الفرق الحقيقي يظهر عند التشغيل على أجهزة منفصلة.
- الهدف الأساسي هو إثبات عمل الكلاستر الموزع مع توزيع المهام على Workers.
EOF

echo -e "${GREEN}✅ تم حفظ التقرير في: $REPORT_FILE${NC}"

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}   ✅ المقارنة اكتملت!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "   📄 التقرير: ${GREEN}$REPORT_FILE${NC}"
echo -e "   🌐 Spark UI: ${GREEN}http://localhost:8080${NC} (لمشاهدة Jobs/Stages/Tasks)"
echo ""
