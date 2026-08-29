# Hybrid Orders Data Pipeline

خط بيانات هجين لمعالجة ملف طلبات تجارة إلكترونية غير نظيف وفق نمط ELT:
يصل كل سجل أولًا إلى `orders_raw`، ثم يصنف إلى `orders_validated` أو
`orders_quarantine` مع أثر التصحيح وسبب العزل.

## ما تم تنفيذه

- Router واحد يقرأ حجم الملف ويختار `python_batch` عندما يكون الحجم
  `<= SMALL_FILE_THRESHOLD_MB`، و`pyspark` عندما يتجاوزه.
- Python Batch يستخدم `csv.DictReader` وStreaming و`insert_many` بدفعات قابلة
  للضبط، ولا يستخدم `list(reader)` أو Pandas.
- مسار PySpark يستخدم `SparkSession` وDataFrame API وSchema ثابتة من نوع
  `StringType` للحقول الحساسة، ويكتب عبر MongoDB Spark Connector.
- ELT واضح: التحميل الكامل إلى Raw يسبق أي تنظيف.
- أكثر من ثماني قواعد تصحيح حتمية: الأرقام العربية، فواصل الآلاف، العملات،
  الكلمات الرقمية المعروفة، الهاتف، البريد، التاريخ، Trim والقواميس، وإعادة
  حساب الإجمالي.
- Audit Trail في `corrections` يحفظ الحقل والقيمة الأصلية والجديدة و`rule_code`.
- Quarantine لا يحذف السجلات: يحفظ `error_codes` و`error_details` و`raw_record`.
- `order_id` هو Stable Business Key، مع Unique Index وAtomic Upsert.
- لكل دفعة حفظ `Idempotency Key` ثابت مع بصمة Payload؛ إعادة الطلب نفسه لا
  تضيف Raw/Quarantine ولا تعيد الكتابة التجارية، وإعادة استخدام المفتاح مع
  Payload مختلف تفشل صراحة.
- كل دفعة معالجة تحفظ checkpoint ذرياً في حالة التشغيل. عند انقطاع التشغيل
  يعاد استخدام claim نفسه وتستأنف قواعد الجودة بعد آخر `source_row_number`
  ناجح بدلاً من إعادة معالجة الملف كاملاً.
- الكتابة التزايدية تقارن `version` داخل عملية MongoDB الذرية؛ النسخة الأقدم
  أو المساوية لا تستبدل النسخة الأحدث.
- مقاييس التشغيل في `reports/results.json`، بما فيها الاتساق:
  `raw_loaded = valid_count + corrected_count + quarantine_count`.
- مسار B اختياري للـ Incremental Loading بملفات Delta و`version`.
- اختبارات أساسية للقواعد والتصنيف والـ Router وإعادة التشغيل.

## التشغيل السريع دون MongoDB

تم توفير `--dry-run` كـ Adapter اختباري في الذاكرة. هذا لا يستبدل MongoDB في
العرض النهائي، لكنه يثبت قواعد الجودة وIdempotency محليًا:

```bash
python src/create_small_sample.py \
  --input attached_assets/sample_orders_1787957383234.csv \
  --output data/orders_sample.csv \
  --rows 1000

python src/main.py \
  --input data/orders_sample.csv \
  --dry-run \
  --log-level INFO
```

تظهر في الناتج: حجم الملف، سبب اختيار المحرك، رقم الدفعة، الزمن، معدل
الإدخال، العدادات، ونجاح معادلة الاتساق.

### الاستطلاع الذكي والاستئناف

للتشغيل الحقيقي استخدم `--smart-poll`. يحفظ النظام بصمة المصدر وwatermark
والـclaim في MongoDB. إذا توقف التشغيل بعد حفظ دفعات، يعاد تشغيل نفس الأمر؛
بعد انتهاء lease سيستعيد checkpoint ويبدأ من آخر صف ناجح:

```bash
export SMART_POLL_LEASE_SECONDS=60
python src/main.py --input data/orders_sample.csv --smart-poll
```

لا يعلن الـpoller نجاح المصدر إلا بعد اكتمال ELT. لذلك لا يتم إسقاط claim
المتوقف ولا اعتبار المصدر معالجاً قبل الأوان. التشغيل `--dry-run` يستخدم حالة
داخل الذاكرة للعرض والاختبار فقط؛ التشغيل القابل للاستعادة بين العمليات يحتاج
MongoDB.

## تشغيل MongoDB

يتطلب MongoDB يعملًا محليًا أو URI صالحًا في البيئة. لا تضع الأسرار في
الكود أو Git:

```bash
export MONGO_URI='mongodb://localhost:27017'
export MONGO_DATABASE='orders_pipeline'
export BATCH_SIZE=1000
export SMALL_FILE_THRESHOLD_MB=200

python src/main.py --input data/orders_sample.csv
```

يقوم البرنامج بإنشاء:

- `orders_raw`: تاريخ التحميل الخام، دون Validator أو Unique Business Index.
- `orders_validated`: الحالة التجارية النهائية، Schema Validation وUnique
  Index على `order_id` وUpsert.
- `orders_quarantine`: السجلات غير القابلة للتصحيح مع أسبابها والسجل الخام.
- `pipeline_idempotency_keys`: سجل مفاتيح طلبات الحفظ وبصمات Payload.
- `pipeline_source_state` و`pipeline_processing_claims`: watermark وclaims
  وcheckpoints الخاصة بالـsmart polling.

## إثبات مسار PySpark

لملف أكبر من الحد:

```bash
export SPARK_MASTER='local[*]'
python src/main.py --input data/orders_huge_mixed_quality.csv
```

ولأغراض العرض يمكن إجبار الاختيار دون خداع Router الحجم:

```bash
python src/main.py \
  --input data/orders_sample.csv \
  --force-engine pyspark
```

مسار Spark يحتاج Java 11+، PySpark، وMongoDB Spark Connector متوافقًا مع
نسخة Spark. الكود يستخدم Schema ثابتة ولا يضيف `repartition` عشوائيًا؛ يسجل
عدد Input Partitions. في عنقود مستقل استخدم مثلًا:

```bash
export SPARK_MASTER='spark://MASTER_IP:7077'
python src/main.py --input data/orders_huge_mixed_quality.csv
```

يجب توثيق نسخة Java/Python/Spark/Connector ومشاركة الملف بين العقد عند اختيار
مسار Cluster A، وإرفاق Spark UI أثناء العرض.

## الاختبارات

```bash
pytest -q
```

الاختبارات لا تحتاج MongoDB وتغطي:

1. القواعد التصحيحية وأثرها.
2. أخطاء التاريخ وJSON والعناصر والقيم السالبة.
3. مفاتيح الأعمال المفقودة والتكرار.
4. الاتساق بين Raw والنتائج.
5. إعادة تشغيل نفس المصدر دون زيادة Business Records.
6. إنشاء العينة والـ Router والـ threshold.
7. Idempotency Key للدفعات، حماية النسخ الأقدم، والاستئناف من checkpoint.

## المسار التزايدي B

يمكن تطبيق ملف Delta دون تحميله كاملاً إلى الذاكرة:

```python
from src.incremental_loader import load_delta
from src.mongo_setup import create_repository
from config.settings import Settings

settings = Settings.from_env()
repository = create_repository(settings)
try:
    stats = load_delta(
        "data/orders_delta.csv",
        run_id="delta-run-1",
        repository=repository,
        version_field="version",
        batch_size=settings.batch_size,
    )
    print(stats)
finally:
    repository.close()
```

أو من نقطة التشغيل:

```bash
python src/main.py \
  --input data/orders_delta.csv \
  --incremental \
  --version-field version
```

يجب أن يحتوي Delta على `version`. التشغيل الأول يسجل Insert، والنسخة الأحدث
تسجل Update، وإعادة نفس الملف تسجل Unchanged دون Duplicate أو إعادة تطبيق
نسخة أقدم. ويمكن تغيير اسم عمود watermark عبر `version_field`.

## هيكل المشروع

```text
config/settings.py             الإعدادات والبيئة
src/file_router.py             اختيار المحرك
src/batch_loader.py            Streaming Raw Loader
src/spark_loader.py            PySpark + Connector path
src/quality_rules.py           التنظيف والتصنيف
src/elt_pipeline.py            orchestration وELT
src/repositories.py            MongoDB وIn-memory adapters
src/incremental_loader.py      Path B: Delta + version
src/metrics.py                 المقاييس والتقارير
src/mongo_setup.py             تهيئة collections/indexes/validator
tests/                         الاختبارات
docs/architecture.md           قرارات المعمارية
reports/results.json           مخرجات التشغيل
```

## ملاحظات العرض

شغّل العينة مع `--dry-run` لرؤية Python Batch، ثم شغّل MongoDB لعرض Raw
قبل التنظيف، وسجلًا valid/corrected/quarantined، ثم أعد التشغيل بنفس البيانات.
في `orders_validated` يجب أن يبقى عدد Business Records ثابتًا؛ الإعادة تظهر
في المقاييس كـ `unchanged_count` بدل Duplicate.
