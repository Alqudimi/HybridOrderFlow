#!/usr/bin/env bash
# ===========================================================================
# تشغيل كلاستر Spark Standalone + MongoDB عبر Docker Compose
# ===========================================================================
#
# الاستخدام:
#   bash scripts/cluster_start.sh
#   bash scripts/cluster_start.sh --generate-data    # توليد 1M سجل تلقائياً
#
# ===========================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/cluster/docker-compose.yml"

# ألوان للطباعة
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   🚀 تشغيل Spark Standalone Cluster                         ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# -----------------------------------------------------------------------
# 1. التحقق من المتطلبات
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[1/6] التحقق من المتطلبات...${NC}"

if ! command -v docker &>/dev/null; then
    echo -e "${RED}❌ Docker غير مثبت. قم بتثبيت Docker أولاً.${NC}"
    exit 1
fi

if ! docker compose version &>/dev/null 2>&1; then
    echo -e "${RED}❌ Docker Compose V2 غير متوفر.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker و Docker Compose متوفران${NC}"

# -----------------------------------------------------------------------
# 2. إيقاف أي تشغيل سابق
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[2/6] إيقاف أي تشغيل سابق...${NC}"
docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true
echo -e "${GREEN}✅ تم التنظيف${NC}"

# -----------------------------------------------------------------------
# 3. تشغيل الكلاستر
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[3/6] تشغيل الكلاستر (MongoDB + Spark Master + 2 Workers)...${NC}"
docker compose -f "$COMPOSE_FILE" up -d

echo -e "\n${YELLOW}   ⏳ انتظار تهيئة الخدمات (15 ثانية)...${NC}"
sleep 15

# -----------------------------------------------------------------------
# 4. التحقق من الحالة
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[4/6] التحقق من حالة الخدمات...${NC}"
docker compose -f "$COMPOSE_FILE" ps

# التحقق من MongoDB
if docker exec spark-cluster-mongodb mongosh --eval "db.adminCommand('ping')" &>/dev/null; then
    echo -e "${GREEN}✅ MongoDB يعمل بنجاح${NC}"
else
    echo -e "${RED}❌ MongoDB لا يستجيب${NC}"
fi

# التحقق من Spark Master
if curl -s http://localhost:8080 &>/dev/null; then
    echo -e "${GREEN}✅ Spark Master UI يعمل على http://localhost:8080${NC}"
else
    echo -e "${RED}❌ Spark Master UI لا يستجيب${NC}"
fi

# -----------------------------------------------------------------------
# 5. طباعة معلومات الإصدارات
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[5/6] التحقق من توحيد الإصدارات عبر العقد...${NC}"
echo -e "${BLUE}─────────────────────────────────────────────${NC}"

for NODE in spark-master spark-worker-1 spark-worker-2; do
    echo -e "\n${GREEN}📦 $NODE:${NC}"

    JAVA_VER=$(docker exec "$NODE" java -version 2>&1 | head -1 || echo "N/A")
    PYTHON_VER=$(docker exec "$NODE" python3 --version 2>&1 || echo "N/A")
    SPARK_VER=$(docker exec "$NODE" /opt/bitnami/spark/bin/spark-submit --version 2>&1 | grep "version" | head -1 || echo "N/A")

    echo "   Java:   $JAVA_VER"
    echo "   Python: $PYTHON_VER"
    echo "   Spark:  $SPARK_VER"
done

echo -e "\n${BLUE}─────────────────────────────────────────────${NC}"

# MongoDB version
MONGO_VER=$(docker exec spark-cluster-mongodb mongosh --eval "db.version()" --quiet 2>/dev/null || echo "N/A")
echo -e "${GREEN}📦 MongoDB: $MONGO_VER${NC}"

# Connector JARs
echo -e "\n${GREEN}📦 MongoDB Spark Connector JARs (مشتركة عبر جميع العقد):${NC}"
docker exec spark-master ls /opt/bitnami/spark/pipeline-jars/ 2>/dev/null || echo "  (غير متوفرة)"

# -----------------------------------------------------------------------
# 6. التحقق من البيانات المشتركة
# -----------------------------------------------------------------------
echo -e "\n${YELLOW}[6/6] التحقق من البيانات المشتركة عبر العقد...${NC}"
for NODE in spark-master spark-worker-1 spark-worker-2; do
    FILES=$(docker exec "$NODE" ls /opt/bitnami/spark/pipeline-data/ 2>/dev/null | wc -l)
    echo -e "   $NODE: $FILES ملفات بيانات متاحة"
done

# -----------------------------------------------------------------------
# توليد البيانات الضخمة إذا طُلب
# -----------------------------------------------------------------------
if [[ "${1:-}" == "--generate-data" ]]; then
    echo -e "\n${YELLOW}📊 توليد 1,000,000 سجل...${NC}"
    cd "$PROJECT_ROOT"
    python3 scripts/generate_million_records.py --rows 1000000 --output data/orders_1m.csv
fi

# -----------------------------------------------------------------------
# ملخص
# -----------------------------------------------------------------------
echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}   ✅ الكلاستر جاهز للعمل!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "   🌐 Spark Master UI:    ${GREEN}http://localhost:8080${NC}"
echo -e "   🔌 Spark Master URL:   ${GREEN}spark://spark-master:7077${NC}"
echo -e "   🗄️  MongoDB URI:        ${GREEN}mongodb://localhost:27017${NC}"
echo -e "   👷 Worker 1 UI:        ${GREEN}http://localhost:8081${NC}"
echo -e "   👷 Worker 2 UI:        ${GREEN}http://localhost:8082${NC}"
echo -e "   📊 Application UI:     ${GREEN}http://localhost:4040${NC} (أثناء التنفيذ)"
echo ""
echo -e "   لتشغيل Pipeline عبر الكلاستر:"
echo -e "   ${YELLOW}SPARK_MASTER=spark://spark-master:7077 \\"
echo -e "   MONGO_URI=mongodb://spark-cluster-mongodb:27017 \\"
echo -e "   python3 src/main.py --input data/orders_1m.csv --force-engine pyspark${NC}"
echo ""
