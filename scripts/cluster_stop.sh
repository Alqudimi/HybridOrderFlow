#!/usr/bin/env bash
# ===========================================================================
# إيقاف كلاستر Spark Standalone + MongoDB
# ===========================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/cluster/docker-compose.yml"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   🛑 إيقاف Spark Standalone Cluster                         ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

docker compose -f "$COMPOSE_FILE" down

echo -e "\n${GREEN}✅ تم إيقاف جميع الخدمات بنجاح${NC}"
echo -e "   لإزالة البيانات المحفوظة: docker compose -f $COMPOSE_FILE down -v"
