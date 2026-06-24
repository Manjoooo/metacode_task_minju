# Criteo 광고 이벤트 기반 Lakehouse 운영 파이프라인

> Kafka, Spark, Apache Iceberg, MinIO, Airflow/스케줄러를 활용해 광고 이벤트 로그의 수집, 정제, 집계, 운영 모니터링, Iceberg 테이블 관리를 구현한 로컬 Lakehouse 프로젝트입니다.

---

## 0. 프로젝트 요약

본 프로젝트는 Criteo 광고 이벤트 데이터를 기반으로 광고 플랫폼에서 발생하는 `impression`, `click`, `conversion` 이벤트를 수집하고, Bronze/Silver/Gold 계층으로 정제 및 집계한 뒤, Iceberg 메타데이터를 활용해 운영 상태를 점검하는 Lakehouse 파이프라인이다.

실제 광고 서버에서 이벤트를 직접 받을 수 없기 때문에, CSV 데이터를 Kafka Producer가 row 단위로 발행하여 실시간 이벤트 유입을 모사하였다. 이후 Spark Structured Streaming이 Kafka topic을 구독하여 Bronze raw zone에 저장하고, Silver/Gold 계층 및 Iceberg 테이블을 구성하였다.

본 프로젝트의 핵심은 단순한 ETL이 아니라, 광고 데이터에서 실제로 발생할 수 있는 운영 이슈를 고려했다는 점이다. 특히 다음 문제를 중심으로 설계하였다.

- 광고 이벤트의 실시간 수집 및 원본 보존
- 지연 전환(late conversion)으로 인한 과거 KPI 변동
- streaming ingestion으로 발생할 수 있는 small file 문제
- Iceberg snapshot/files metadata 기반 운영 점검
- compaction 자동화 및 alert/report 생성
- AWS 사용이 어려운 상황을 고려한 로컬 대체 아키텍처 구성

---

## 1. 도메인 및 데이터 특성

### 1.1 도메인

도메인은 광고 플랫폼 이벤트 로그 처리이다. 광고 플랫폼에서는 사용자가 광고를 보는 `impression`, 광고를 클릭하는 `click`, 실제 구매/가입 등으로 이어지는 `conversion` 이벤트가 발생한다.

광고 데이터의 특징은 다음과 같다.

- `impression`과 `click`은 비교적 빠르게 발생한다.
- `conversion`은 click 이후 수 시간에서 수 일 늦게 들어올 수 있다.
- 캠페인 단위 성과 지표는 뒤늦게 들어온 전환 이벤트 때문에 과거 집계값이 바뀔 수 있다.
- 원본 로그를 보존하지 않으면 장애 발생 시 재처리 및 원인 추적이 어렵다.
- 스트리밍으로 작은 파일이 계속 생성될 수 있어 Iceberg compaction 관리가 필요하다.

### 1.2 사용 데이터

본 프로젝트에서는 Criteo 광고 이벤트 데이터를 기반으로 실습용 이벤트 로그를 생성하였다.

| 항목 | 내용 |
|---|---|
| 원본 데이터 | Criteo 광고 이벤트 데이터 |
| 생성 이벤트 | impression, click, conversion |
| 이벤트 수 | Bronze 기준 141,738건 |
| 주요 키 | event_id, user_id, campaign_id, event_type, event_time |
| 집계 기준 | event_date, campaign_id |

---

## 2. 데이터 규모 및 트래픽 가정

### 2.1 현재 규모

| 항목 | 가정 |
|---|---:|
| 일 이벤트 수 | 1,000,000건 |
| 시간당 평균 이벤트 수 | 약 40,000건 |
| 초당 평균 이벤트 수 | 약 12건 |
| 평균 이벤트 크기 | 약 1KB |
| 일 raw 데이터 크기 | 약 1GB |
| 월 raw 데이터 크기 | 약 30GB |
| 연 raw 데이터 크기 | 약 365GB |

### 2.2 트래픽 패턴

| 구분 | 패턴 |
|---|---|
| 평일 피크 시간 | 오전 8시 ~ 12시 |
| 피크 시간 트래픽 | 평소 대비 2~3배 |
| 주말 트래픽 | 평일 평균 대비 1.5배 |
| 캠페인 프로모션 | 특정 campaign_id에 이벤트 집중 가능 |
| conversion 지연 | click 이후 수 시간~수 일 후 발생 가능 |

### 2.3 성장 시나리오

| 시점 | 이벤트 규모 | 예상 raw 데이터 |
|---|---:|---:|
| 현재 | 일 100만 이벤트 | 약 1GB/day |
| 6개월 후 | 일 1,000만 이벤트 | 약 10GB/day |
| 1~2년 후 | 일 1억 이벤트 | 약 100GB/day |

본 프로젝트는 로컬 Docker 기반으로 구현했지만, 향후 일 1억 이벤트까지 증가할 수 있는 상황을 고려해 Kafka partition, Spark executor, Iceberg partition/compaction, Gold 집계 테이블 확장 전략을 함께 설계하였다.

---

## 3. 기술 스택

| 영역 | 사용 기술 |
|---|---|
| 이벤트 생성 | Python, CSV replay |
| 이벤트 수집 | Apache Kafka |
| 스트리밍 처리 | Spark Structured Streaming |
| 배치 처리 | Spark SQL, PySpark |
| 테이블 포맷 | Apache Iceberg |
| 로컬 오브젝트 스토리지 | MinIO |
| 카탈로그/쿼리 | Spark Iceberg Catalog, Spark SQL |
| 오케스트레이션 | Airflow, Windows Task Scheduler |
| 모니터링 | health report, alert log, HTML dashboard |
| 제출/협업 | GitHub Repository |

---

## 4. AWS 대체 아키텍처

AWS 과금 및 실습 제약을 고려하여, AWS 기반 구성요소를 로컬 오픈소스 도구로 대체하였다.

| AWS 구성요소 | 로컬 대체 구현 | 설명 |
|---|---|---|
| Amazon S3 | MinIO bucket `ad-lakehouse` | raw/silver/gold 데이터 저장소 |
| AWS Glue Catalog | Spark Iceberg Hadoop Catalog | Iceberg table catalog 역할 |
| Athena | Spark SQL | Iceberg 테이블 및 metadata table 조회 |
| EMR / Spark on EKS | Docker Spark master/worker | Spark streaming/batch 실행 |
| EventBridge / Glue Workflow | Airflow + Windows Task Scheduler | 파이프라인 및 maintenance 자동화 |
| CloudWatch | health_report.md, dashboard, alert log | 운영 상태 점검 및 리포트 |
| SNS / Slack Alert | alerts.log, AlertSender 구조 | 장애/성공 이벤트 기록 |
| QuickSight | HTML dashboard | 비즈니스 KPI 및 운영 지표 시각화 |

따라서 본 프로젝트는 AWS를 직접 사용하지 않았지만, 실제 AWS 환경에서의 Lakehouse 구성요소를 로컬 환경에서 기능적으로 치환하여 구현하였다.

---

## 5. 전체 파이프라인 구조

```text
Criteo / Sample Ad Event CSV
        ↓
Kafka Producer
        ↓
Kafka Topics
  - ad-impressions
  - ad-clicks
  - ad-conversions
        ↓
Spark Structured Streaming
        ↓
Bronze Layer
  raw ad events / append-only
        ↓
Spark Batch Processing
        ↓
Silver Layer
  processed_events
        ↓
Spark Aggregation
        ↓
Gold Layer
  campaign_summary
        ↓
Apache Iceberg Tables
        ↓
Spark SQL / Metadata Health Query / Dashboard
        ↓
Compaction Automation + Monitoring Report
```

---

## 6. Medallion Architecture

### 6.1 Bronze Layer: raw ad events

Bronze 계층은 Kafka에서 수집한 광고 이벤트를 원본에 가깝게 저장하는 append-only 계층이다.

역할은 다음과 같다.

- Kafka에서 수집한 원본 이벤트 보존
- event_type, raw_date, raw_hour 기준 파티셔닝
- 장애 발생 시 재처리 source로 사용
- 중복/오류 이벤트도 즉시 삭제하지 않고 추적 가능하게 보존
- downstream 정제 로직 변경 시 Bronze부터 다시 재처리 가능

현재 Bronze Iceberg 테이블 row count는 다음과 같다.

```text
bronze_ad_events  141,738 rows
```

### 6.2 Silver Layer: processed events

Silver 계층은 Bronze 데이터를 분석 가능한 형태로 정제한 계층이다.

주요 역할은 다음과 같다.

- event_time, event_date 정규화
- event_type 검증
- 잘못된 값 제거
- downstream 집계 기준 스키마 제공
- Gold summary 계산의 기준 테이블 역할

현재 Silver Iceberg 테이블 row count는 다음과 같다.

```text
silver_processed_events  139,866 rows
```

### 6.3 Gold Layer: campaign summary

Gold 계층은 BI 대시보드와 KPI 조회를 위한 집계 계층이다.

Gold 테이블은 `event_date`, `campaign_id` 기준으로 광고 성과 지표를 저장한다.

| 지표 | 설명 |
|---|---|
| impressions | 광고 노출 수 |
| clicks | 광고 클릭 수 |
| conversions | 전환 수 |
| ctr | clicks / impressions |
| cvr | conversions / clicks |

현재 Gold Iceberg 테이블 row count는 다음과 같다.

```text
gold_campaign_summary  2,409 rows
```

---

## 7. Kafka 기반 실시간 수집 구현

### 7.1 Kafka Topic

광고 이벤트 타입별로 topic을 분리하였다.

```text
ad-impressions
ad-clicks
ad-conversions
```

이렇게 분리한 이유는 이벤트 타입별 트래픽 특성과 처리 우선순위가 다르기 때문이다. 예를 들어 impression은 이벤트 수가 많고, conversion은 수는 적지만 KPI에 직접적인 영향을 주므로 별도 모니터링이 필요하다.

### 7.2 Kafka Producer

`code/pipelines/kafka_producer.py`는 CSV 데이터를 읽어 row 단위로 Kafka topic에 발행한다. 이를 통해 정적 CSV 데이터를 실시간 이벤트 스트림처럼 재생한다.

```text
CSV row
→ event_type 확인
→ ad-impressions / ad-clicks / ad-conversions topic으로 전송
```

### 7.3 Spark Structured Streaming

Spark Structured Streaming은 Kafka topic을 구독하여 Bronze raw zone에 저장한다. checkpointLocation을 사용하여 장애 발생 후에도 offset 기반 재시작이 가능하도록 구성하였다.

---

## 8. Iceberg 테이블 구현

Bronze/Silver/Gold 계층을 Iceberg 테이블로 구성하였다.

```text
local.ad_lakehouse.bronze_ad_events
local.ad_lakehouse.silver_processed_events
local.ad_lakehouse.gold_campaign_summary
```

테이블 생성 후 Spark SQL에서 row count를 확인한 결과는 다음과 같다.

| Table | Row Count |
|---|---:|
| bronze_ad_events | 141,738 |
| silver_processed_events | 139,866 |
| gold_campaign_summary | 2,409 |

Iceberg를 사용한 이유는 단순 Parquet 저장만으로는 다음 기능을 제공하기 어렵기 때문이다.

- snapshot 기반 테이블 변경 이력 추적
- metadata table을 통한 파일 수, 파일 크기, snapshot 상태 점검
- compaction을 통한 small file 관리
- time travel / rollback 가능성
- schema evolution 및 partition evolution 가능성
- 대규모 테이블에서 안전한 append/replace/merge 운영

---

## 9. Iceberg Metadata 기반 Health Query

운영자가 테이블 상태를 빠르게 확인할 수 있도록 Iceberg metadata table을 활용한 health query를 구성하였다.

위치:

```text
code/health-queries/
```

생성한 health query는 다음과 같다.

| 파일 | 목적 |
|---|---|
| 01_row_count_check.sql | Bronze/Silver/Gold row count 확인 |
| 02_snapshot_history_check.sql | Iceberg snapshot 이력 확인 |
| 03_file_size_check.sql | 파일 개수, 평균 파일 크기, 총 파일 크기 확인 |
| 04_small_file_check.sql | 16MB 미만 small file 수 확인 |
| 05_latest_partition_check.sql | 최신 파티션 날짜 확인 |
| 06_gold_kpi_check.sql | Gold KPI 정합성 확인 |
| 07_table_list_check.sql | Iceberg catalog table 존재 여부 확인 |
| 08_daily_event_volume_check.sql | 일자별 이벤트 볼륨 확인 |
| all_health_queries.sql | 전체 health query 통합 실행 |

### 9.1 Health Query 실행 결과

```text
01_row_count_check
bronze_ad_events        141738
silver_processed_events 139866
gold_campaign_summary   2409
```

```text
03_file_size_check
bronze_ad_events        33 files   avg 0.3762 MB
silver_processed_events 33 files   avg 0.3997 MB
gold_campaign_summary   31 files   avg 0.0044 MB
```

```text
05_latest_partition_check
bronze_ad_events        2026-07-16
silver_processed_events 2026-07-16
gold_campaign_summary   2026-07-16
```

```text
06_gold_kpi_check
impressions  100000
clicks       35411
conversions  4455
CTR          0.35411
CVR          0.125808
```

Snapshot history에서는 초기 적재 시 `append` snapshot이 생성되고, compaction 이후 `replace` snapshot이 추가되는 것을 확인하였다. 이를 통해 Iceberg가 테이블 변경 이력을 metadata로 관리한다는 점을 검증하였다.

---

## 10. Iceberg Compaction 자동화

### 10.1 문제 정의

스트리밍 기반 ingestion에서는 작은 파일이 지속적으로 생성될 수 있다. 작은 파일이 많아지면 다음 문제가 발생한다.

- 쿼리 planning 비용 증가
- 파일 open 비용 증가
- scan 성능 저하
- 운영자가 테이블 상태를 파악하기 어려움

따라서 Iceberg의 `rewrite_data_files` 기능을 이용해 compaction을 수행하도록 구성하였다.

### 10.2 Compaction SQL

위치:

```text
code/maintenance/iceberg_compaction.sql
```

수행 내용:

```text
CALL local.system.rewrite_data_files(...)
```

대상 테이블:

```text
bronze_ad_events
silver_processed_events
gold_campaign_summary
```

### 10.3 자동화 방식

초기에는 Airflow DAG로 compaction 자동화를 시도하였다. 그러나 로컬 Docker 환경에서 Airflow 컨테이너와 Spark 컨테이너의 Iceberg warehouse 파일 권한이 달라 `Permission denied`가 발생하였다. 이에 따라 실제 compaction은 성공했던 `spark-master` 컨테이너에서 수행하도록 하고, Windows Task Scheduler 기반 cron-style 자동화로 전환하였다.

자동화 스크립트 위치:

```text
orchestration/run_iceberg_compaction.ps1
```

자동화 실행 흐름:

```text
Windows Task Scheduler
        ↓
PowerShell script
        ↓
docker compose exec spark-master spark-sql
        ↓
Iceberg rewrite_data_files
        ↓
monitoring/reports/iceberg_maintenance_report.md
        ↓
monitoring/alerts/iceberg_maintenance_alerts.log
```

### 10.4 자동화 검증 결과

작업 스케줄러 즉시 실행 결과:

```text
LastTaskResult : 0
NextRunTime    : 2026-06-25 오전 2:00:00
```

maintenance report 결과:

```text
executed_by: Windows Task Scheduler / cron-style script
maintenance_type: rewrite_data_files / compaction
return_code: 0
```

compaction 후 파일 상태:

```text
bronze  33  0.3762 MB
silver  33  0.3997 MB
gold    31  0.0044 MB
```

alert log:

```text
[OK] Iceberg compaction completed successfully.
```

---

## 11. MinIO 기반 Object Storage 구성

AWS S3를 직접 사용하지 않고 MinIO를 S3-compatible storage로 사용하였다.

MinIO 설정:

```text
URL: http://localhost:9001
Bucket: ad-lakehouse
ID/PW: minioadmin / minioadmin
```

Bucket 구조:

```text
ad-lakehouse/
  raw/
  silver/
  gold/
```

MinIO health check에서는 bucket 및 layer path 존재 여부를 확인한다.

```text
[OK] minio_storage_check - MinIO bucket and layer paths exist
```

---

## 12. Monitoring / Alert 구현

운영 상태를 확인하기 위해 health check script와 alert log를 구성하였다.

주요 파일:

```text
monitoring/check_pipeline_health.py
monitoring/alert_sender.py
monitoring/reports/health_report.md
monitoring/reports/iceberg_maintenance_report.md
monitoring/alerts/alerts.log
monitoring/alerts/iceberg_maintenance_alerts.log
```

점검 항목:

- Kafka topic 존재 여부
- MinIO bucket/layer path 접근 가능 여부
- Bronze row count
- Silver/Gold path 및 `_SUCCESS` 존재 여부
- Gold row count
- 최신 partition freshness
- Iceberg compaction 성공 여부

정상 예시:

```text
[OK] kafka_topic_check
[OK] minio_storage_check
[OK] bronze_count_check
[OK] gold_row_count_check
[OK] Iceberg compaction completed successfully.
```

---

## 13. Dashboard 구현

### 13.1 운영 모니터링 대시보드

운영 상태를 HTML dashboard로 확인할 수 있도록 구성하였다.

```text
dashboard/monitoring_dashboard.html
```

표시 항목:

- Kafka topic health
- MinIO storage health
- Bronze/Silver/Gold 상태
- Gold row count
- alert log
- report 생성 시각

### 13.2 비즈니스 KPI 대시보드

Gold summary 기반 비즈니스 KPI dashboard를 구성하였다.

```text
dashboard/business_dashboard.html
```

표시 항목:

- impressions
- clicks
- conversions
- CTR
- CVR
- campaign performance
- daily event volume

### 13.3 Live Replay Dashboard

정적 CSV 데이터를 event_time 순서로 replay하여 near-real-time dashboard를 생성한다.

```text
dashboard/run_live_business_dashboard.py
```

실행:

```powershell
python dashboard\run_live_business_dashboard.py
```

출력:

```text
dashboard/live_business_dashboard.html
```

이 dashboard는 이벤트가 들어오는 것처럼 `Processed Events`, `Impressions`, `Clicks`, `Conversions`, `CTR`, `CVR` 값이 주기적으로 증가한다. 실제 Kafka 스트리밍 수집 구조와 별개로, 발표 및 제출 시 실시간 KPI 모니터링 화면을 보여주기 위한 replay dashboard이다.

---

## 14. 장애 시나리오 및 복구 전략

### 14.1 Kafka broker down

증상:

- Kafka topic 조회 실패
- producer/consumer 이벤트 처리 중단

점검:

```text
[CRITICAL] kafka_topic_check
```

복구:

```powershell
docker compose up -d kafka
```

설계 포인트:

- Spark checkpointLocation을 사용해 offset 기반 재시작 가능
- Bronze append-only 구조로 원본 이벤트 손실 추적 가능

### 14.2 MinIO / S3-compatible storage down

증상:

- bucket 접근 실패
- raw/silver/gold path 확인 실패

점검:

```text
[CRITICAL] minio_storage_check
```

복구:

```powershell
docker compose up -d minio
```

설계 포인트:

- storage 장애는 downstream 처리 실패로 이어질 수 있으므로 health check에서 별도 점검
- 복구 후 bucket/layer path 재확인

### 14.3 Gold output missing

증상:

- Gold summary path 또는 `_SUCCESS` 누락
- dashboard KPI 조회 실패

점검:

```text
[ALERT] gold path check
```

복구:

- Silver 또는 Iceberg table에서 Gold summary 재생성
- dashboard 재생성

### 14.4 Small file 증가

증상:

- Iceberg files metadata에서 file_count 증가
- avg_file_size 감소

점검:

```sql
SELECT COUNT(*), AVG(file_size_in_bytes)
FROM local.ad_lakehouse.bronze_ad_events.files;
```

복구:

```sql
CALL local.system.rewrite_data_files(...);
```

자동화:

```text
Windows Task Scheduler / cron-style script
```

---

## 15. Late Conversion 처리 관점

광고 데이터에서는 conversion이 impression/click보다 늦게 들어올 수 있다. 본 프로젝트의 이벤트 데이터에서도 2026-06-16에 impression/click이 집중되어 있고, 이후 날짜에는 conversion만 존재하는 구간이 나타난다.

예시:

```text
2026-06-16  impressions=100000  clicks=35411  conversions=1404
2026-06-17~2026-07-16  delayed conversions only
```

이 때문에 이후 날짜의 CTR/CVR은 `NULL`로 나타날 수 있다. 이는 오류가 아니라, conversion event_time이 실제 전환 발생 시점 또는 지연 반영 시점으로 기록된 결과이다.

운영 설계에서는 다음 방식으로 대응한다.

- Bronze에는 원본 이벤트를 append-only로 보존
- Silver에서 event_type과 event_time을 표준화
- Gold에서는 최근 N일 window를 재집계하거나 Iceberg MERGE/replace 전략으로 summary 갱신
- Iceberg snapshot을 통해 과거 KPI 갱신 이력을 추적

---

## 16. 10x / 100x 확장 전략

### 16.1 10x 성장: 일 1,000만 이벤트

| 영역 | 확장 전략 |
|---|---|
| Kafka | topic partition 수 증가, producer batch 설정 조정 |
| Spark | executor/worker 수 증가, shuffle partition 조정 |
| Storage | raw/silver/gold partition 관리 강화 |
| Iceberg | compaction 주기 단축, file size target 조정 |
| Dashboard | Gold summary 기반 조회 유지 |

### 16.2 100x 성장: 일 1억 이벤트

| 영역 | 확장 전략 |
|---|---|
| Kafka | event_type별 topic 분리 유지, consumer group 병렬성 확대 |
| Spark | 별도 cluster에서 streaming/batch 분리 운영 |
| Iceberg | partition evolution, manifest 관리, compaction 리소스 분리 |
| Gold | campaign/date/hour 단위 사전 집계 테이블 구성 |
| Monitoring | metadata query 자동화, alert rule 세분화 |
| Backfill | 운영 streaming과 분리된 batch cluster에서 재처리 |

100x 규모에서는 모든 raw 데이터를 dashboard에서 직접 조회하지 않고, Gold summary 및 pre-aggregated table 중심으로 조회하도록 구성한다.

---

## 17. 프로젝트 산출물 구조

```text
final-project/
  README.md
  code/
    pipelines/
      kafka_producer.py
      kafka_to_raw_files.py
      prepare_streaming_sample.py
    health-queries/
      01_row_count_check.sql
      02_snapshot_history_check.sql
      03_file_size_check.sql
      04_small_file_check.sql
      05_latest_partition_check.sql
      06_gold_kpi_check.sql
      07_table_list_check.sql
      08_daily_event_volume_check.sql
      all_health_queries.sql
    maintenance/
      iceberg_compaction.sql
  data/
    sample_ad_events_100k.csv
  warehouse/
    raw/
    silver/
    gold/
  iceberg_warehouse/
  monitoring/
    check_pipeline_health.py
    alert_sender.py
    reports/
    alerts/
  dashboard/
    generate_monitoring_dashboard.py
    generate_business_dashboard.py
    run_live_business_dashboard.py
    monitoring_dashboard.html
    business_dashboard.html
    live_business_dashboard.html
  orchestration/
    run_iceberg_compaction.ps1
```

---

## 18. 제출용 주요 캡처 목록

| 번호 | 캡처 내용 |
|---:|---|
| 01 | Docker compose services running |
| 02 | Kafka topics 생성 확인 |
| 03 | Kafka producer 실행 화면 |
| 04 | Bronze raw parquet 생성 확인 |
| 05 | Bronze/Silver/Gold row count |
| 06 | MinIO bucket `ad-lakehouse` 및 raw/silver/gold path |
| 07 | pipeline health check OK |
| 08 | MinIO down alert |
| 09 | MinIO recovery OK |
| 10 | Iceberg table 생성 결과 |
| 11 | Iceberg snapshot metadata query |
| 12 | Iceberg files metadata query |
| 13 | manual compaction result |
| 14 | Windows Task Scheduler `LastTaskResult: 0` |
| 15 | Iceberg maintenance report `return_code: 0` |
| 16 | Iceberg maintenance alert `[OK]` |
| 17 | Iceberg health query row count |
| 18 | Iceberg snapshot history health query |
| 19 | Iceberg file size health query |
| 20 | Gold KPI health query |
| 21 | live dashboard running early |
| 22 | live dashboard running mid |
| 23 | live dashboard final KPI |

---

## 19. 프로젝트에서 강조할 점

본 프로젝트는 단순히 CSV를 읽어 집계하는 작업이 아니라, 광고 이벤트 파이프라인에서 실제로 발생할 수 있는 운영 문제를 고려하여 설계하였다.

핵심 강조점은 다음과 같다.

1. Kafka 기반 이벤트 수집 구조를 구현하였다.
2. Bronze/Silver/Gold medallion architecture를 구성하였다.
3. Apache Iceberg 테이블을 생성하고 snapshot/files metadata를 활용하였다.
4. Iceberg health query 8개를 구성하여 운영 가시성을 확보하였다.
5. streaming ingestion으로 발생할 수 있는 small file 문제를 compaction 자동화로 관리하였다.
6. Airflow/컨테이너 권한 이슈를 확인하고, 안정적인 cron-style scheduler 방식으로 자동화를 완성하였다.
7. AWS를 직접 쓰지 않고도 MinIO, Spark SQL, scheduler, HTML dashboard로 AWS Lakehouse 구조를 로컬에서 대체하였다.
8. live replay dashboard를 통해 near-real-time KPI 모니터링 화면을 제공하였다.

---

## 20. 결론

본 프로젝트는 광고 이벤트 데이터를 대상으로 실시간 수집, Lakehouse 저장, Iceberg 테이블 관리, 운영 모니터링, 자동화까지 포함한 end-to-end 데이터 파이프라인을 구현하였다.

최종 구현 범위는 다음과 같다.

```text
데이터 생성 및 이벤트화 완료
Kafka 이벤트 수집 완료
Spark Structured Streaming 기반 Bronze 적재 완료
Bronze/Silver/Gold 계층 구성 완료
Apache Iceberg 테이블 생성 완료
Iceberg snapshot/files metadata health query 완료
MinIO 기반 S3 대체 저장소 구성 완료
pipeline health check 및 alert log 구현 완료
Iceberg compaction 자동화 완료
비즈니스/운영/live dashboard 구성 완료
```

이를 통해 단순 데이터 처리뿐 아니라, 운영 관점에서 장애를 탐지하고, 테이블 상태를 점검하며, 규모 증가에 대응할 수 있는 Lakehouse 파이프라인 설계 역량을 보여주는 것을 목표로 한다.
