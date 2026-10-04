# خط البيانات الهجين ومعالجة وتحليل البيانات الضخمة (Big Data Platform — Phase 1 & Phase 2)

مشروع متكامل وشامل لمعالجة وتحليل بيانات متجر إلكتروني ضخمة ومختلطة الجودة، يجمع بين:
1. **المرحلة الأولى (Phase 1 - Ingestion & ELT Pipeline)**:
   - مسار هجين **ELT** (Extract-Load-Transform).
   - توجيه ذكي للملفات **File Router** بين المعالجة الخفيفة السريعة **Python Batch Streaming** والمعالجة المتوازية الضخمة **Apache Spark / PySpark**.
   - إثبات موثوقية كامل لـ **Idempotency** و **Atomic Replace / Upsert** ومنع التكرار نهائياً.
   - تطبيق **المسار المتقدم B** (التحميل التزايدي وإدارة الإصدارات Delta & Versioning).
   - تشغيل الكلاستر الموزع **Spark Standalone Cluster** (Master + 2 Workers).

2. **المرحلة الثانية (Phase 2 - Analytics, Indexing, Materialized Views & Unified FastAPI)**:
   - **فهرسة متقدمة (Advanced MongoDB Indexing)** تتبع نمط **ESR (Equality, Sort, Range)** مع Multikey Index و Single Field Index.
   - **كتالوج استعلامات متقدم (5 Core Queries)** مع دعم المعاملات الديناميكية واستقلال تام عن البيانات (Data Independence).
   - **مقارنة حقيقية للأداء عبر Explain Plan (`executionStats`)** توثق انتقال الاستعلامات من المسح الشامل البطيء `COLLSCAN` إلى المسح الفهرسي فائق السرعة `IXSCAN` مع إلغاء الفرز في الذاكرة `In-Memory Sort`.
   - **محرك تقارير تجميعية متقدم (5 Aggregation Pipelines)** لحساب مؤشرات الأداء الحيوية، وأعلى المدن دخلاً، والمنتجات الأكثر مبيعاً، وتوزيع الحالات والدفع.
   - **جداول مادية ذكية (2 Materialized Views)** تدعم **التحديث التزايدي الذكي (Incremental Refresh)** عبر علامات المزامنة (Watermarks) لمعالجة البيانات الجديدة فقط دون إعادة مسح السجلات التاريخية.
   - **جدولة مهام وخلفيات تشغيلية (Scheduled & Manual Jobs)** مع نظام تدقيق وتسجيل متين في قاعدة البيانات (`job_execution_logs`) يرصد البداية والنهاية والمدة والحالة والأخطاء في حالات النجاح والفشل.
   - **واجهة برمجية موحدة ومتكاملة (Unified FastAPI REST API)** تغطي كافة الوظائف مع توثيق تفاعلي كامل عبر **Swagger UI (`/docs`)**.

---

## 🏗️ المعمارية العامة للنظام (System Architecture)

```
                    ┌────────────────────────────────────────────────────────┐
                    │                      Input Data                        │
                    │               (CSV Files / Delta Data)                 │
                    └───────────────────────────┬────────────────────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │    Smart File Router  │
                                    └─────┬───────────┬─────┘
                     <= 200MB (Batch)     │           │     > 200MB (Distributed)
                 ┌────────────────────────┘           └────────────────────────┐
                 ▼                                                             ▼
     ┌───────────────────────┐                                     ┌───────────────────────┐
     │  Python Batch Engine  │                                     │  PySpark Engine /     │
     │  (Lightweight Stream) │                                     │  Distributed Cluster  │
     └───────────┬───────────┘                                     └───────────┬───────────┘
                 │                                                             │
                 └──────────────────────────────┬──────────────────────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │    ELT Raw Landing    │
                                    │     (orders_raw)      │
                                    └───────────┬───────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │ Quality Cleaning Rules│
                                    │ (8+ Deterministic Rs) │
                                    └─────┬───────────┬─────┘
                     Failed Cleaning      │           │     Clean / Corrected
                 ┌────────────────────────┘           └────────────────────────┐
                 ▼                                                             ▼
     ┌───────────────────────┐                                     ┌───────────────────────┐
     │   orders_quarantine   │                                     │   orders_validated    │
     │ (Audit trail & errors)│                                     │(Atomic Upsert/Version)│
     └───────────────────────┘                                     └───────────┬───────────┘
                                                                               │
        ┌───────────────────────────────────┬──────────────────────────────────┴───────────────────────────────────┐
        ▼                                   ▼                                   ▼                                  ▼
┌───────────────┐                   ┌───────────────┐                   ┌───────────────┐                  ┌───────────────┐
│  ESR Indexes  │                   │    Queries    │                   │ Aggregations  │                  │ Materialized  │
│ (COLL->IXSCAN)│                   │   Catalog     │                   │   Pipelines   │                  │     Views     │
└───────────────┘                   └───────────────┘                   └───────────────┘                  └───────┬───────┘
                                                                                                                   │
                                                                                                       Incremental Refresh
                                                                                                       (Watermark Engine)
                                                                                                                   │
                                                                                                                   ▼
                                                                                                       ┌───────────────────┐
                                                                                                       │  mv_daily_sales   │
                                                                                                       │  mv_top_products  │
                                                                                                       └───────────────────┘
                                                ▲
                                                │ (Triggers & Synchronizes)
                                    ┌───────────┴───────────┐
                                    │  Scheduler / Jobs     │
                                    │ (Logs Execution Audit)│
                                    └───────────▲───────────┘
                                                │
                                    ┌───────────┴───────────┐
                                    │    FastAPI REST API   │
                                    │  (Swagger UI: /docs)  │
                                    └───────────────────────┘
```

---

## 🛠️ المتطلبات وتجهيز البيئة (Environment Setup)

### 1. المتطلبات البرمجية
- **Python**: 3.10+ (تم التحقق على Python 3.12 / 3.13)
- **MongoDB**: 6.0+ / 7.0+ (متوفر محلياً أو عبر حاوية Docker على المنفذ `27017`)
- **Docker & Docker Compose** (اختياري: لتشغيل MongoDB وكلاستر Spark الموزع)

### 2. تثبيت الحزم والمكتبات
```bash
# إنشاء وتفعيل البيئة الافتراضية
python3 -m venv .venv
source .venv/bin/activate

# تثبيت الاعتماديات
pip install -r requirements.txt
# أو باستخدام uv:
uv pip install -r requirements.txt
```

### 3. إعداد متغيرات البيئة
```bash
cp env.example .env
```
ملف `.env.example` يحتوي على الإعدادات الافتراضية الآمنة:
```ini
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=orders_pipeline
INPUT_FILE_PATH=data/orders_sample.csv
ENGINE_SELECTION_THRESHOLD_BYTES=209715200
API_HOST=0.0.0.0
API_PORT=8000
```

### 4. تشغيل قاعدة البيانات MongoDB
إذا كنت تستخدم Docker:
```bash
docker start spark-cluster-mongodb || docker run -d --name spark-cluster-mongodb -p 27017:27017 mongo:7.0
```

---

## ⚡ فحص التحقق الذاتي الشامل (Master Audit & Self-Correction)

يمكنك تشغيل أداة التحقق الشاملة التي تفحص جميع متطلبات المشروع الـ 21 وتختبر كل مكوّن بشكل حي وتولّد مصفوفة المتطلبات فورياً:
```bash
python scripts/audit_all.py
```

---

## 🚀 تشغيل خادم FastAPI وتصفح Swagger UI

### تشغيل الخادم
```bash
uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload
```

### استعراض الواجهات
- **Swagger UI التفاعلي**: افتح المتصفح على [http://localhost:8000/docs](http://localhost:8000/docs)
- **Redoc UI**: افتح المتصفح على [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## 📋 دليل الـ Endpoints والعمليات في Phase 2

### 1. الفحص الصحي (Health Check)
- **Endpoint**: `GET /health`
- **الوصف**: التحقق من اتصال الخادم وقاعدة البيانات وحالة الـ Pipeline.
```bash
curl -X GET http://localhost:8000/health
```

### 2. استيعاب البيانات (Ingest Pipeline)
- **Endpoint**: `POST /ingest`
- **الوصف**: تشغيل مسار الـ ELT الكامل على ملف بيانات.
```bash
curl -X POST http://localhost:8000/ingest \
     -H "Content-Type: application/json" \
     -d '{"file_path": "data/orders_sample.csv"}'
```

### 3. إنشاء الفهارس (Indexes Creation)
- **Endpoint**: `POST /indexes`
- **الوصف**: إنشاء الفهارس المطلوبة (Single Field, Multikey, Compound ESR).
```bash
curl -X POST http://localhost:8000/indexes
```
الفهارس المنشأة تشمل:
1. `idx_customer_id`: فهرس مفرد لتسريع استعلامات العملاء.
2. `idx_items_sku`: فهرس متعدد المفاتيح (Multikey) على المصفوفة المتداخلة `items_json.sku`.
3. `idx_city_status_order_date`: فهرس مركب ثلاثي وفق معيار **ESR** `(city: 1, status: 1, order_date: -1)` يخدم الفلاتر المتعددة ويلغي عملية الفرز المكلفة من الذاكرة (`In-Memory SORT`).

### 4. كتالوج الاستعلامات (5 Core Queries)
- **قائمة الاستعلامات**: `GET /queries`
- **تنفيذ استعلام محدد**: `GET /queries/{name}`

الاستعلامات الخمسة المتاحة:
1. `orders_by_customer`: جلب طلبات عميل محدد مرتبة تنازلياً حسب التاريخ.
   ```bash
   curl -X GET "http://localhost:8000/queries/orders_by_customer?customer_id=عميل-1&limit=20"
   ```
2. `orders_by_city_and_status`: استعلام مركب يطابق المدينة وحالة الطلب ويفرز حسب تاريخ الطلب (يستفيد بالكامل من فهرس ESR المركب).
   ```bash
   curl -X GET "http://localhost:8000/queries/orders_by_city_and_status?city=صنعاء&status=مؤكد&limit=20"
   ```
3. `high_value_orders`: الطلبات ذات القيمة المرتفعة التي تتجاوز حداً مالياً معيناً.
   ```bash
   curl -X GET "http://localhost:8000/queries/high_value_orders?min_amount=500000&limit=20"
   ```
4. `orders_by_payment_status`: الطلبات المفلترة بحالة السداد المالي (مثال: بانتظار الدفع، تم الدفع).
   ```bash
   curl -X GET "http://localhost:8000/queries/orders_by_payment_status?payment_status=تم%20الدفع&limit=20"
   ```
5. `orders_containing_item_sku`: استعلام عن الطلبات التي تحوي منتجاً محدداً عبر الـ SKU (يستفيد من فهرس Multikey).
   ```bash
   curl -X GET "http://localhost:8000/queries/orders_containing_item_sku?sku=SKU-1010&limit=20"
   ```

### 5. محرك التقارير التجميعية (5 Analytical Aggregations)
- **قائمة التقارير**: `GET /aggregations`
- **تنفيذ تقرير محدد**: `GET /aggregations/{name}`

التقارير الخمسة المتاحة:
1. `sales_by_city`: إجمالي المبيعات والإيرادات ومتوسط قيمة الطلب مصنفة حسب المدينة.
   ```bash
   curl -X GET "http://localhost:8000/aggregations/sales_by_city?limit=10"
   ```
2. `top_products`: المنتجات الأكثر مبيعاً وتحقيقاً للإيرادات مع عدد الوحدات المباعة ومتوسط السعر.
   ```bash
   curl -X GET "http://localhost:8000/aggregations/top_products?limit=10"
   ```
3. `top_customers`: كبار العملاء وأكثرهم شراءً وإنفاقاً ومعدل سلة المشتريات.
   ```bash
   curl -X GET "http://localhost:8000/aggregations/top_customers?limit=10"
   ```
4. `sales_by_period`: تحليل المبيعات المجمعة حسب السنة والشهر واليوم لإظهار منحنيات النمو.
   ```bash
   curl -X GET "http://localhost:8000/aggregations/sales_by_period"
   ```
5. `orders_by_status`: التوزيع النسبي للطلبات حسب حالاتها التشغيلية (تم التسليم، ملغي، قيد الانتظار...).
   ```bash
   curl -X GET "http://localhost:8000/aggregations/orders_by_status"
   ```

### 6. الجداول المادية والتحديث التزايدي (Materialized Views & Incremental Refresh)
- **Endpoint**: `POST /refresh-mv`
- **المعاملات**: `{"force_rebuild": false}`

```bash
# تشغيل التحديث التزايدي الذكي
curl -X POST http://localhost:8000/refresh-mv \
     -H "Content-Type: application/json" \
     -d '{"force_rebuild": false}'
```

الجداول المادية المتوفرة في قاعدة البيانات:
1. `mv_daily_sales_summary`: ملخص مبيعات الأيام مع مؤشرات الإيراد وعدد الطلبات المكتملة ومتوسط الطلب.
2. `mv_top_products_summary`: ملخص إحصائيات المنتجات مع عدد الوحدات والمبيعات الإجمالية.

**آلية التحديث التزايدي (Incremental Engine)**:
- يعتمد النظام على علامة مائية (`mv_refresh_watermarks`) تحفظ آخر معرف سجل تم دمجه.
- عند استدعاء التحديث بدون بيانات جديدة، يعود فوراً بحالة `up_to_date` بزمن يقارب 0 ميلي ثانية وبدون أي مسح غير مبرر.
- عند وصول سجلات جديدة، يحدد النظام بدقة التواريخ والـ SKUs المتأثرة ويعيد تجميع وحفظ تلك الصفوف حصراً باستخدام `bulk_write(UpdateOne(upsert=True))`.

### 7. المهام المجدولة والتشغيل اليدوي وسجل التدقيق (Scheduled Jobs & Execution Logs)
- **استعراض المهام المسجلة**: `GET /jobs`
- **التشغيل اليدوي الفوري لأي مهمة**: `POST /jobs/{name}/run`

المهام المسجلة:
1. `refresh_materialized_views_job`: المزامنة الدورية للجداول المادية كل 15 دقيقة (`interval=900s`).
   ```bash
   curl -X POST http://localhost:8000/jobs/refresh_materialized_views_job/run
   ```
2. `daily_sales_report_job`: توليد لقطة تشغيلية إدارية بالساعة وحفظها في `reports_archive` (`interval=3600s`).
   ```bash
   curl -X POST http://localhost:8000/jobs/daily_sales_report_job/run
   ```

**سجل التدقيق المتين (Job Execution Logging)**:
كل عملية تشغيل (مجدولة أو يدوية أو عبر الـ API) تسجل وثيقة كاملة في مجموعة `job_execution_logs`:
- `job_name`: اسم المهمة.
- `trigger_type`: نوع المحفز (`scheduled` | `manual` | `api`).
- `started_at` & `finished_at`: طوابع زمنية دقيقة بصيغة ISO UTC.
- `duration_seconds`: مدة التنفيذ بالثواني.
- `status`: حالة التنفيذ (`SUCCESS` أو `FAILED`).
- `records_processed`: عدد السجلات المعالجة.
- `error`: نص رسالة الخطأ والـ StackTrace في حالة الفشل.

---

## 🔬 فحص الأداء وتقرير Explain Plan الحقيقي

تم تطبيق اختبار `explain("executionStats")` قبل وبعد إنشاء الفهارس على 3 استعلامات جوهرية، وتوثيق النتائج الحقيقية الصادرة من محرك MongoDB:

| الاستعلام | الفهرس المطبق | نوع الفهرس | السجلات المفحوصة (قبل -> بعد) | المفاتيح المفحوصة (قبل -> بعد) | مرحلة التنفيذ (قبل -> بعد) | استخدام IXSCAN | إلغاء الفرز بالذاكرة |
|---|---|---|---|---|---|---|---|
| `orders_by_customer` | `idx_customer_id` | Single | 941 -> **1** | 0 -> **1** | SORT (COLLSCAN) -> **SORT (IXSCAN)** | ✅ نعم | — |
| `orders_by_city_and_status` | `idx_city_status_order_date` | Compound (ESR) | 941 -> **12** | 0 -> **12** | SORT (COLLSCAN) -> **FETCH (IXSCAN)** | ✅ نعم | ✅ تم الإلغاء بالكامل |
| `orders_containing_item_sku` | `idx_items_sku` | Multikey | 941 -> **269** | 0 -> **269** | SORT (COLLSCAN) -> **SORT (IXSCAN)** | ✅ نعم | — |

التقرير التحليلي المكتمل متوفر في:
- [`reports/index_explain_benchmark.md`](reports/index_explain_benchmark.md)
- [`reports/index_explain_benchmark.json`](reports/index_explain_benchmark.json)

---

## 🧪 تشغيل الاختبارات الآلية (Automated Tests)

```bash
# تشغيل كامل حزمة الاختبارات الوحدوية والتكاملية
pytest tests/ -v

# تشغيل اختبار التدقيق الشامل لـ Phase 2
python scripts/audit_all.py
```

---

## 🔒 الأمان واستقلالية البيانات (Security & Data Independence)

1. **حماية الأسرار**: تم حظر ملفات `.env` في `.gitignore`، وتوفير `.env.example` مع قيم نموذجية خالية من أي بيانات اعتماد حقيقية.
2. **استقلالية البيانات (Data Independence)**:
   - كافة الاستعلامات والتقارير والجداول المادية لا تعتمد على أسماء مدن ثابتة أو معرّفات عملاء جامدة أو تواريخ مصطنعة؛ بل تقبل معاملات ديناميكية وتستخلص البيانات مباشرة من قاعدة البيانات.
   - عند تحميل أي ملف CSV جديد، يتكيف النظام تلقائياً ويعالج السجلات ويحدث الجداول المادية والتقارير التجميعية بدقة تامة.
