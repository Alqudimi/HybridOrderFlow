# خط البيانات الهجين لمعالجة الطلبات الضخمة (Hybrid Orders Data Pipeline)

مشروع متكامل ومتقدم لمعالجة بيانات متجر إلكتروني ضخمة ومختلطة الجودة وفق نمط **ELT** (Extract-Load-Transform)، يجمع بين المعالجة التدفّقية الخفيفة عبر **Python Batch Streaming** والمعالجة المتوازية الضخمة عبر **Apache Spark / PySpark**، مع قاعدة بيانات **MongoDB** وإثبات موثوقية كامل لـ **Idempotency** و**Upsert** وتطبيق **المسار المتقدم B (التحميل التزايدي وإدارة الإصدارات)**.

---

## 🌟 أبرز المميزات المعمارية والوظيفية

1. **الموجّه التلقائي للمحرك (File Router)**: نقطة تشغيل موحدة تفحص حجم الملف الفعلي؛ تختار `python_batch` للملفات <= 200MB، و`pyspark` للملفات الضخمة > 200MB.
2. **التحميل الخام الكامل وفق ELT**: جميع السجلات تصل أولاً إلى `orders_raw` دون أي حذف أو تصفية مسبقة، مع حفظ `run_id` واسم الملف ورقم الصف والوقت والمحرك.
3. **أكثر من 8 قواعد جودة وتنظيف حتمية**:
   - تحويل الأرقام العربية والفارسية إلى أرقام لاتينية رقمية.
   - إزالة رموز العملات وتوحيد العملة إلى `YER`.
   - إزالة فواصل الآلاف والنقاط وتحويل القيم إلى أرقام عشرية.
   - تحويل الأسعار المكتوبة بالكلمات المعروفة حصراً.
   - توحيد صيغة أرقام الهواتف وإزالة المسافات ومفتاح الدولة الدولي.
   - إصلاح الرموز المكررة في البريد الإلكتروني (`@@` -> `@`, `..` -> `.`) وعزل غير الصالح.
   - توحيد صيغ التواريخ إلى معيار ISO وعزل التواريخ المستحيلة.
   - إزالة المسافات الزائدة وتوحيد حالات الطلب والدفع إلى قاموس قياسي.
   - إعادة حساب إجمالي الطلب ومقارنته بالإجمالي الأصلي وتصحيحه عند ثبوت الفرق.
4. **أثر التدقيق الكامل (Audit Trail)**: كل سجل مصحح يحتفظ بقائمة `corrections` توضح الحقل المعدل، القيمة الأصلية، القيمة المصححة، ورمز القاعدة `rule_code`.
5. **طبقة العزل الآمنة (Quarantine)**: نقل السجلات غير القابلة للإصلاح بأمان إلى `orders_quarantine` مع أسباب ورموز الأخطاء (`error_codes`) والسجل الخام الأصلي دون حذف.
6. **مفتاح الأعمال الثابت وموثوقية إعادة التشغيل (Stable Business Key & Idempotency)**:
   - اعتماد `order_id` كمفتاح فريد في `orders_validated` عبر Unique Index.
   - تنفيذ الكتابة باستخدام `Atomic Replace/Upsert`، مما يضمن أن إعادة تشغيل نفس المدخل لا تنشئ سجلات مكررة مطلقاً (`inserted_count = 0` و `unchanged_count > 0`).
7. **المسار المتقدم B (التحميل التزايدي وإدارة الإصدارات Delta & Versioning)**:
   - دعم ملفات Delta التي تحتوي على الإضافات والتعديلات فقط.
   - تطبيق `Version Handling` عبر تحديثات مشروطة على مستوى MongoDB لمنع استبدال نسخة أحدث بنسخة أقدم أو غير مرقمة.
8. **المقاييس الشاملة والاتساق الرياضي**:
   - تسجيل الأزمنة ومعدل المعالجة (Throughput) والعدادات في `reports/results.json` و`reports/results.md`.
   - إثبات معادلة الاتساق الرياضية لكل تشغيل:
     $$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$

---

## 🛠️ المتطلبات والبيئة البرمجية

- **Python**: 3.10+ (تم التحقق على Python 3.12)
- **MongoDB**: 6.0+ / 7.0+ / 8.0+ (تم التحقق على MongoDB v8.3.7 محليًا على المنفذ `27017`)
- **Java**: OpenJDK 11 أو 17
- **Apache Spark**: PySpark 3.5+ أو 4.2+
- **MongoDB Spark Connector**: `org.mongodb.spark:mongo-spark-connector_2.13:10.4.0` (متوفر محليًا في مجلد `jars/`)

لتثبيت الاعتماديات:
```bash
pip install -r requirements.txt
```

---

## 🚀 دليل التشغيل والأوامر العملية

### 1. استخراج عينة صغيرة قابلة لإعادة الإنتاج (Streaming Sample Extraction)
```bash
python3 src/create_small_sample.py --input data/orders_sample.csv --output data/orders_sample.csv --rows 1000
```

### 2. التشغيل التلقائي عبر File Router (Python Batch للعينات)
```bash
python3 src/main.py --input data/orders_sample.csv
```

### 3. تشغيل مسار البيانات الكبيرة المتوازي عبر Apache Spark (PySpark)
```bash
python3 src/main.py --input data/orders_sample.csv --force-engine pyspark
```

### 4. تشغيل المسار المتقدم B (التحميل التزايدي Delta Loader)
```bash
python3 src/main.py --input data/orders_sample.csv --incremental --version-field version
```

### 5. تشغيل الاستطلاع الذكي (Smart Polling & Checkpoint Resume)
```bash
python3 src/main.py --input data/orders_sample.csv --smart-poll
```

---

## 🧪 حزمة الاختبارات والتحقق الشامل

### تشغيل الاختبارات الوظيفية (Unit Tests):
```bash
pytest -v
```

### تشغيل حزمة الفحص والتحقق الشامل وتوليد التقارير:
```bash
bash scripts/run_verification.sh
```

---

## 📁 هيكل المشروع المنظم

```text
midterm-data-pipeline/
|-- README.md                      دليل التشغيل والمواصفات الشاملة
|-- TODO.md                        قائمة المهام وسجل حالة التنفيذ
|-- requirements.txt               المتطلبات البرمجية
|-- config/
|   `-- settings.py                إعدادات المشروع ومتغيرات البيئة
|-- data/
|   |-- orders_sample.csv          عينة البيانات غير النظيفة
|   `-- .gitkeep
|-- jars/                          حزم موصل MongoDB Spark Connector JARs
|-- src/
|   |-- main.py                    نقطة التشغيل الرئيسية
|   |-- file_router.py             موجّه الملفات واختيار المحرك
|   |-- create_small_sample.py     استخراج العينات التدفقي
|   |-- batch_loader.py            محرك التحميل الدفعي التدفقي (Python Batch)
|   |-- spark_loader.py            محرك التحميل والمعالجة المتوازية (PySpark)
|   |-- quality_rules.py           قواعد التنظيف والتصنيف وأثر التدقيق
|   |-- elt_pipeline.py            تنسيق خط ELT وعمليات Upsert
|   |-- incremental_loader.py      المسار المتقدم B: التحميل التزايدي
|   |-- mongo_setup.py             تهيئة الاتصال ومستودعات MongoDB
|   |-- repositories.py            مستودعات MongoDB والتحقق من الفهارس والقواعد
|   |-- smart_poller.py            الاستطلاع الذكي وإدارة نقاط الاستئناف
|   |-- mongo_aggregation.py       تجميعات MongoDB على جانب الخادم
|   `-- metrics.py                 حساب المقاييس والتحقق من الاتساق
|-- tests/
|   |-- test_cleaning_rules.py     اختبارات قواعد التنظيف
|   |-- test_classification.py     اختبارات التصنيف والعزل
|   |-- test_mongo_features.py     اختبارات MongoDB وIdempotency وPolling
|   `-- test_router_and_sample.py  اختبارات الموجّه واستخراج العينات
|-- scripts/
|   |-- run_full_verification.py   سكربت التحقق الشامل من جميع المراحل
|   |-- run_verification.sh        أمر التشغيل الموحد
|   |-- generate_report.py         مولد تقرير النتائج التحليلي
|   |-- generate_million_records.py مولد بيانات المليون سجل
|   |-- cluster_start.sh           تشغيل كلاستر Spark الموزع
|   |-- cluster_stop.sh            إيقاف الكلاستر
|   `-- run_cluster_benchmark.sh   مقارنة أداء Local vs Cluster
|-- cluster/
|   |-- docker-compose.yml         تعريف خدمات الكلاستر (Docker)
|   |-- spark-env.sh               إعدادات بيئة Spark
|   |-- spark-defaults.conf        إعدادات Spark الافتراضية
|   `-- workers                    قائمة عناوين العمال
|-- reports/
|   |-- results.json               سجل مقاييس التشغيل
|   |-- results.md                 التقرير التحليلي الشامل المقارن
|   `-- cluster_benchmark.md       تقرير مقارنة أداء الكلاستر
`-- docs/
    |-- architecture.md            التوثيق المعماري ومخططات التدفق
    |-- verification.md            سجل التحقق التشغيلي والأدلة الفعلية
    `-- cluster_setup.md           دليل إعداد الكلاستر الموزع
```

---

## 🖥️ تشغيل الكلاستر الموزع (Spark Standalone Cluster)

### الإعداد السريع (Docker Compose)
```bash
# 1. تشغيل الكلاستر (Master + 2 Workers + MongoDB)
bash scripts/cluster_start.sh

# 2. توليد مليون سجل للمقارنة
python3 scripts/generate_million_records.py --rows 1000000

# 3. تشغيل المعالجة عبر الكلاستر
SPARK_MASTER=spark://spark-master:7077 \
MONGO_URI=mongodb://spark-cluster-mongodb:27017 \
python3 src/main.py --input data/orders_1m.csv --force-engine pyspark

# 4. مقارنة الأداء: Local vs Cluster
bash scripts/run_cluster_benchmark.sh

# 5. إيقاف الكلاستر
bash scripts/cluster_stop.sh
```

### واجهات المراقبة
| الواجهة | العنوان |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Worker 1 UI | http://localhost:8081 |
| Worker 2 UI | http://localhost:8082 |
| Application UI | http://localhost:4040 (أثناء التنفيذ) |

> للتوثيق الكامل: [`docs/cluster_setup.md`](docs/cluster_setup.md)

---

## 📊 سيناريو العرض العملي أمام المحاضر (Presentation Walkthrough)

1. **الخطوة 1 - تشغيل العينة الصغيرة**: تشغيل `python3 src/main.py --input data/orders_sample.csv` وإظهار اختيار Router لمحرك `python_batch` وطباعة زمن ومعدل الإدخال.
2. **الخطوة 2 - إثبات نمط ELT في MongoDB**: فتح MongoDB وإظهار وجود كافة السجلات في `orders_raw` ببياناتها الأصلية قبل التنظيف.
3. **الخطوة 3 - استعراض تصنيف الجودة وأثر التدقيق**:
   - عرض مثال لسجل سليم (`valid`).
   - عرض مثال لسجل مصحح (`corrected`) مع حقل `corrections` وقيم ما قبل وبعد التصحيح.
   - عرض مثال لسجل معزول في `orders_quarantine` مع رمز الخطأ `error_codes` وتفاصيل السبب.
4. **الخطوة 4 - تشغيل مسار Apache Spark**: تشغيل `python3 src/main.py --input data/orders_sample.csv --force-engine pyspark` وإظهار معالجة Spark المتوازية والكتابة عبر `MongoDB Spark Connector`.
5. **الخطوة 5 - إثبات Idempotency وUpsert**:
   - إعادة تشغيل نفس الأمر وملاحظة ثبات عدد السجلات في `orders_validated` دون أي تكرار (`unchanged_count`).
   - تعديل سجل واحد وإظهار تحديثه في مكانه (`updated_count = 1`) دون إنشاء سجل ثانٍ.
6. **الخطوة 6 - إثبات المسار المتقدم B (التحميل التزايدي)**: تشغيل `scripts/run_full_verification.py` وإظهار معالجة ملفات Delta وتطبيق تحديثات الإصدارات الأحدث فقط.
7. **الخطوة 7 - تشغيل الكلاستر الموزع**: تشغيل `bash scripts/cluster_start.sh` وفتح `http://localhost:8080` لإظهار Master و Workers، ثم تشغيل المعالجة عبر الكلاستر ومراقبة توزيع Tasks على Executors.
8. **الخطوة 8 - مقارنة الأداء**: تشغيل `bash scripts/run_cluster_benchmark.sh` وعرض نتائج المقارنة بين Local و Cluster على مليون سجل.
9. **الخطوة 9 - استعراض تقرير النتائج**: فتح ملف `reports/results.md` و `reports/cluster_benchmark.md` واستعراض المقاييس المقارنة وتحقق معادلة الاتساق الرياضية.
