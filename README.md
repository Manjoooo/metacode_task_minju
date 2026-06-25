# Criteo 광고 이벤트 기반 Lakehouse 운영 파이프라인

> 로컬 Docker 환경에서 Kafka, Spark Structured Streaming, Apache Iceberg, MinIO, Scheduler, Dashboard를 활용해 광고 이벤트 로그의 수집·정제·집계·운영 점검·Iceberg 테이블 관리를 구현한 Lakehouse PoC 프로젝트입니다.

---

## 0. 프로젝트 요약

본 프로젝트는 Criteo 기반 광고 이벤트 데이터를 `impression`, `click`, `conversion` 이벤트로 변환하고, Kafka Producer가 row 단위로 발행하여 실시간 이벤트 유입을 모사한 뒤, Spark 기반 Bronze/Silver/Gold 계층으로 처리하는 광고 Lakehouse 파이프라인이다.

핵심 목표는 단순히 CSV를 집계하는 것이 아니라, 광고 데이터 파이프라인에서 실제로 발생할 수 있는 운영 이슈를 고려하는 것이다.

- 광고 이벤트의 실시간 수집 모사
- Bronze/Silver/Gold Medallion Architecture 구성
- 지연 전환(late conversion)으로 인한 과거 KPI 변동 고려
- Iceberg snapshot/files metadata 기반 운영 점검
- streaming ingestion으로 발생할 수 있는 small file 문제와 compaction
- health report, alert log, dashboard를 통한 운영 가시성 확보
- AWS 비용 제약을 고려한 로컬 대체 아키텍처 구성

본 프로젝트는 AWS를 직접 사용한 프로덕션 시스템이 아니라, AWS Lakehouse 구조를 로컬 오픈소스 환경에서 기능적으로 대체 구현한 PoC이다.

---

## 1. 현재 구현 범위와 향후 확장 범위

### 1.1 현재 직접 구현한 범위

| 영역 | 구현 내용 | 검증 방식 |
|---|---|---|
| 이벤트 생성 | Criteo 기반 이벤트 CSV 생성 | sample data 및 producer 실행 |
| Kafka 수집 | `ad-impressions`, `ad-clicks`, `ad-conversions` topic 발행 | Kafka topic 확인 |
| Streaming 적재 | Spark Structured Streaming 기반 Bronze 적재 | Bronze parquet 및 row count 확인 |
| Medallion | Bronze/Silver/Gold 계층 구성 | row count 확인 |
| Iceberg | Bronze/Silver/Gold Iceberg table 생성 | `SHOW TABLES`, row count |
| Metadata 점검 | snapshots/files metadata query | append/replace, file count 확인 |
| Health Check | Kafka/MinIO/Bronze/Silver/Gold 상태 점검 | `health_report.md`, `alerts.log` |
| 장애 검증 | Kafka down, MinIO down, Gold output missing | alert/recovery 캡처 |
| Compaction | Iceberg `rewrite_data_files` 실행 | compaction output 확인 |
| 자동화 | Windows Task Scheduler 기반 cron-style compaction | `LastTaskResult: 0`, `return_code: 0` |
| Dashboard | monitoring/business/live replay dashboard | HTML dashboard 캡처 |

### 1.2 향후 확장으로 분리한 범위

아래 항목은 현재 구현 완료가 아니라, 운영 환경으로 확장할 때의 설계 방향이다.

| 확장 항목 | 현재 상태 |
|---|---|
| OpenRTB request 이벤트 추가 | 향후 확장 |
| Kafka multi-broker / replication | 향후 확장 |
| Kubernetes/EKS 기반 HA | 향후 확장 |
| Karpenter node pool 운영 | 향후 확장 |
| Airflow가 Spark Streaming job을 직접 관리 | 향후 확장 |
| Iceberg `MERGE INTO` 기반 late conversion update | 향후 확장 |
| `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests` 자동화 | 향후 확장 |
| Slack/Discord/Teams webhook 실전 연동 | AlertSender 구조 기반 향후 확장 |

---

## 2. 도메인 및 데이터 특성

광고 플랫폼에서는 `impression`, `click`, `conversion` 이벤트가 발생한다. 광고 데이터의 주요 특징은 `conversion`이 `impression` 또는 `click`보다 늦게 들어올 수 있다는 점이다. 이로 인해 특정 날짜의 campaign KPI가 뒤늦게 바뀔 수 있다.

| 항목 | 내용 |
|---|---|
| 원본 데이터 | Criteo 광고 이벤트 기반 데이터 |
| 생성 이벤트 | impression, click, conversion |
| Bronze 기준 이벤트 수 | 141,738건 |
| 주요 키 | event_id, user_id, campaign_id, event_type, event_time |
| 집계 기준 | event_date, campaign_id |

---

## 3. 데이터 규모 및 트래픽 가정

| 항목 | 가정 |
|---|---:|
| 일 이벤트 수 | 1,000,000건 |
| 시간당 평균 이벤트 수 | 약 40,000건 |
| 초당 평균 이벤트 수 | 약 12건 |
| 평균 이벤트 크기 | 약 1KB |
| 일 raw 데이터 크기 | 약 1GB |
| 월 raw 데이터 크기 | 약 30GB |
| 연 raw 데이터 크기 | 약 365GB |

| 시점 | 이벤트 규모 | 예상 raw 데이터 |
|---|---:|---:|
| 현재 | 일 100만 이벤트 | 약 1GB/day |
| 6개월 후 | 일 1,000만 이벤트 | 약 10GB/day |
| 1~2년 후 | 일 1억 이벤트 | 약 100GB/day |

---

## 4. 기술 스택

| 영역 | 사용 기술 |
|---|---|
| 이벤트 생성 | Python, CSV replay |
| 이벤트 수집 | Apache Kafka |
| 스트리밍 처리 | Spark Structured Streaming |
| 배치 처리 | Spark SQL, PySpark |
| 테이블 포맷 | Apache Iceberg |
| 로컬 오브젝트 스토리지 | MinIO |
| 카탈로그/쿼리 | Spark Iceberg Hadoop Catalog, Spark SQL |
| 오케스트레이션 | Airflow 실험, Windows Task Scheduler |
| 모니터링 | health report, alert log, HTML dashboard |
| 제출/협업 | GitHub Repository |

---

## 5. AWS 대체 아키텍처

AWS 과금 및 실습 제약을 고려하여 AWS 구성요소를 로컬 오픈소스 도구로 대체하였다.

| AWS 구성요소 | 로컬 대체 구현 | 설명 |
|---|---|---|
| Amazon S3 | MinIO bucket `ad-lakehouse` | raw/silver/gold 데이터 저장소 |
| AWS Glue Catalog | Spark Iceberg Hadoop Catalog | Iceberg table catalog 역할 |
| Athena | Spark SQL | Iceberg 테이블 및 metadata table 조회 |
| EMR / Spark on EKS | Docker Spark master/worker | Spark streaming/batch 실행 |
| EventBridge / Glue Workflow | Airflow 실험 + Windows Task Scheduler | maintenance 자동화 |
| CloudWatch | health report, dashboard, alert log | 운영 상태 점검 및 리포트 |
| SNS / Slack Alert | alerts.log, AlertSender 구조 | 장애/성공 이벤트 기록 |
| QuickSight | HTML dashboard | 비즈니스 KPI 및 운영 지표 시각화 |

![MinIO bucket console](docs/images/04_minio_bucket_console.png)

---

## 6. 전체 파이프라인 구조

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

![Architecture](docs/images/01_architecture.png)

---

## 7. Kafka 기반 실시간 이벤트 유입 모사

이벤트 타입별로 topic을 분리하였다.

```text
ad-impressions
ad-clicks
ad-conversions
```

`code/pipelines/kafka_producer.py`는 CSV 데이터를 읽어 row 단위로 Kafka topic에 발행한다. 실제 광고 서버를 연결한 것은 아니며, 정적 CSV 데이터를 실시간 이벤트 스트림처럼 흘려보내기 위한 replay 구조이다.

Spark Structured Streaming은 Kafka topic을 구독하여 Bronze raw zone에 저장한다. checkpointLocation을 사용해 Spark job 재시작 시 처리 offset을 이어갈 수 있도록 구성하였다.

단, checkpoint는 Spark job의 재시작을 보조하는 장치이지, Kafka broker 자체의 로그 유실이나 multi-broker HA를 보장하는 것은 아니다. 운영 환경에서는 Kafka replication factor, retention, checkpoint 저장소 내구성을 함께 설계해야 한다.

---

## 8. Medallion Architecture

### 8.1 Bronze Layer: raw ad events

Bronze 계층은 Kafka에서 수집한 광고 이벤트를 원본에 가깝게 저장하는 append-only 계층이다. 장애 발생 시 재처리 source 역할을 하며, downstream 로직 변경 시 Bronze부터 다시 재처리할 수 있다.

```text
bronze_ad_events  141,738 rows
```

### 8.2 Silver Layer: processed events

Silver 계층은 Bronze 데이터를 분석 가능한 형태로 정제한 계층이다.

```text
silver_processed_events  139,866 rows
```

### 8.3 Gold Layer: campaign summary

Gold 계층은 BI 대시보드와 KPI 조회를 위한 집계 계층이다.

| 지표 | 설명 |
|---|---|
| impressions | 광고 노출 수 |
| clicks | 광고 클릭 수 |
| conversions | 전환 수 |
| ctr | clicks / impressions |
| cvr | conversions / clicks |

```text
gold_campaign_summary  2,409 rows
```

---

## 9. Iceberg 테이블 구현

Bronze/Silver/Gold 계층을 Iceberg 테이블로 구성하였다.

```text
local.ad_lakehouse.bronze_ad_events
local.ad_lakehouse.silver_processed_events
local.ad_lakehouse.gold_campaign_summary
```

| Table | Row Count |
|---|---:|
| bronze_ad_events | 141,738 |
| silver_processed_events | 139,866 |
| gold_campaign_summary | 2,409 |

![Iceberg tables created](docs/images/07_iceberg_tables_created.png)

Iceberg를 사용한 이유는 snapshot 기반 테이블 변경 이력, files metadata 기반 파일 상태 점검, compaction, time travel/rollback 가능성, schema/partition evolution 때문이다.

---

## 10. Iceberg Metadata 기반 Health Query

운영자가 테이블 상태를 빠르게 확인할 수 있도록 Iceberg metadata table을 활용한 health query를 구성하였다.

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
06_gold_kpi_check
impressions  100000
clicks       35411
conversions  4455
CTR          0.35411
CVR          0.125808
```

![Iceberg snapshot metadata](docs/images/08_iceberg_snapshot_metadata.png)

![Iceberg files metadata](docs/images/09_iceberg_files_metadata.png)

---

## 11. Iceberg Compaction 자동화

Streaming ingestion은 주기적으로 작은 parquet 파일을 생성할 수 있다. 작은 파일이 많아지면 query planning 비용과 file open 비용이 증가한다. 이를 관리하기 위해 Iceberg `rewrite_data_files` compaction을 수행하였다.

초기에는 Airflow DAG로 Iceberg compaction을 자동화하려고 했다. 그러나 로컬 Docker 환경에서 Airflow 컨테이너와 Spark 컨테이너의 Iceberg warehouse 파일 권한이 달라 `Permission denied` 문제가 발생하였다.

따라서 최종 compaction은 실제 write 권한이 확인된 `spark-master` 컨테이너에서 실행되도록 구성하고, Windows Task Scheduler 기반 cron-style script로 자동화하였다.

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

자동화 검증:

```text
LastTaskResult : 0
return_code: 0
[OK] Iceberg compaction completed successfully.
```

![Task Scheduler result 0](docs/images/11_compaction_scheduler_success.png)

---

## 12. MinIO 기반 Object Storage 구성

AWS S3를 직접 사용하지 않고 MinIO를 S3-compatible storage로 사용하였다.

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

![MinIO warehouse uploaded](docs/images/05_minio_warehouse_uploaded.png)

---

## 13. Monitoring / Alert 구현

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

![Normal health report](docs/images/03_normal_health_report.png)

### 13.1 AlertSender 구조

현재 PoC에서는 파일 기반 alert log를 기본으로 사용한다. 다만 health check 로직이 alert 전송 방식에 강하게 묶이지 않도록 `AlertSender` 구조를 두었다.

```text
FileAlertSender      → 로컬 alerts.log 기록
WebhookAlertSender   → Slack/Discord/Teams webhook 전송
MultiAlertSender     → 파일 기록 + 외부 webhook 동시 전송
```

### 13.2 Log Retention 정책

운영 환경에서는 로그가 무한히 증가하지 않도록 retention 정책이 필요하다.

| 로그 종류 | 제안 보관 정책 |
|---|---|
| active alert log | 최근 7~30일 |
| maintenance report | 최근 30일 |
| 오래된 로그 | object storage archive 또는 삭제 |
| 중요한 장애 로그 | 별도 incident report로 보존 |

---

## 14. Dashboard 구현

```text
dashboard/monitoring_dashboard.html
dashboard/business_dashboard.html
dashboard/run_live_business_dashboard.py
dashboard/live_business_dashboard.html
```

Live dashboard는 실제 Kafka topic을 직접 조회하는 BI가 아니라, 동일 이벤트 로그를 event-time 순서로 replay하여 near-real-time KPI 변화를 보여주는 시연용 dashboard이다.

![Monitoring dashboard](docs/images/02_monitoring_dashboard.png)

![Live dashboard](docs/images/12_live_dashboard_mid.png)

---

## 15. 장애 시나리오 및 복구 전략

피드백 반영 후, 단순 요구사항 구현과 장애 상황을 구분하였다.

### 15.1 실제 장애로 검증한 시나리오

| 시나리오 | 증상 | 탐지 | 복구 |
|---|---|---|---|
| Kafka broker down | topic 조회 실패, producer/consumer 중단 | `[CRITICAL] kafka_topic_check` | `docker compose up -d kafka` |
| MinIO storage down | bucket/path 접근 실패 | `[CRITICAL] minio_storage_check` | `docker compose up -d minio` |
| Gold output missing | dashboard/KPI 조회 실패 | `[ALERT] gold path check` | Silver/Gold 재생성 또는 백업 복구 |
| Iceberg compaction failure | maintenance 실패 | `[CRITICAL] Iceberg compaction failed` | 권한 확인 후 spark-master 경로로 실행 |

![MinIO down alert](docs/images/06_minio_down_alert.png)

![MinIO recovery OK](docs/images/07_minio_recovery_ok.png)

### 15.2 요구사항 검증에 가까운 항목

| 항목 | 의미 |
|---|---|
| Bronze/Silver/Gold row count | Medallion 계층 생성 검증 |
| Iceberg table list | 테이블 생성 검증 |
| snapshot/files metadata query | Iceberg 운영 가시성 검증 |
| live dashboard | KPI 시각화 검증 |

---

## 16. 운영 자동화 및 HA 설계 범위 명확화

본 프로젝트는 로컬 Docker 기반 PoC이므로, Kubernetes/EKS 기반 HA 구성은 실제 구현 범위에 포함하지 않았다.

현재 Spark Structured Streaming job은 Airflow가 직접 트리거하거나 재시작을 관리하는 구조가 아니다. 로컬 Docker 환경에서 Kafka topic을 구독하는 streaming job으로 실행하였다. Airflow는 health check와 maintenance 자동화 실험에 사용했으며, compaction은 권한 문제로 인해 최종적으로 Windows Task Scheduler 기반 cron-style script로 대체하였다.

Kubernetes/EKS 도입은 자동으로 HA를 보장하지 않는다. 운영 환경에서는 다음을 함께 설계해야 한다.

| 영역 | 고려 사항 |
|---|---|
| Spark Streaming | driver restart policy, checkpoint storage durability |
| Kafka | multi-broker, replication factor, retention |
| Node Pool | streaming job용 node pool과 batch/maintenance job용 node pool 분리 |
| Karpenter | 최소/최대 노드 수, instance type, interruption 대응 |
| Resource | pod request/limit, executor memory/cores |
| Availability | Pod Disruption Budget, anti-affinity, multi-AZ 구성 |
| Storage | S3 기반 checkpoint, Iceberg warehouse 내구성 |

예시 확장 설계:

```text
EKS managed node group
  - system node pool: 2 nodes
  - streaming node pool: min 2 / max 5 nodes
  - batch-maintenance node pool: min 0 / max 5 nodes
Karpenter
  - batch job 발생 시 spot/on-demand node provision
  - streaming job은 안정성을 위해 on-demand 우선
```

이는 현재 구현 완료 범위가 아니라, 운영 환경 확장 시 검토할 HA 설계 방향이다.

---

## 17. Late Conversion 처리 관점

광고 데이터에서는 `conversion`이 `impression`이나 `click`보다 늦게 들어올 수 있다. 본 프로젝트 결과에서도 2026-06-16에는 impression/click이 집중되어 있고, 이후 날짜에는 delayed conversion만 존재하는 구간이 나타난다.

```text
2026-06-16  impressions=100000  clicks=35411  conversions=1404
2026-06-17~2026-07-16  delayed conversions only
```

현재 구현은 Silver 데이터를 기준으로 Gold campaign summary를 생성하는 batch summary 구조이다. 운영 환경에서는 다음 방식으로 확장할 수 있다.

- 최근 N일 window 재집계
- Iceberg `MERGE INTO` 기반 summary 갱신
- conversion attribution window 기준 재처리
- snapshot 기반 KPI 변경 이력 추적

---

## 18. 10x / 100x 확장 전략

| 영역 | 10x 성장 전략 | 100x 성장 전략 |
|---|---|---|
| Kafka | topic partition 수 증가 | consumer group 병렬성 확대, multi-broker |
| Spark | executor/worker 수 증가 | streaming/batch cluster 분리 |
| Iceberg | compaction 주기 단축 | partition evolution, manifest 관리 |
| Gold | summary 조회 유지 | campaign/date/hour 사전 집계 |
| Monitoring | metadata query 자동화 | alert rule 세분화 |
| Backfill | 수동 재처리 | 운영 streaming과 분리된 batch cluster |

---

## 19. 피드백 반영 및 설계 보완 사항

초기 설계안 이후 다음 항목을 보완하였다.

1. Kafka Producer 기반 실시간 이벤트 유입 모사 구현
2. Spark Structured Streaming 기반 Bronze 적재 구현
3. Bronze/Silver/Gold 계층 역할 구체화
4. Iceberg 테이블 생성 및 snapshot/files metadata query 추가
5. 운영 health check script 및 report/alert log 추가
6. 장애 시나리오와 요구사항 검증 항목 구분
7. Iceberg `rewrite_data_files` compaction 실행 및 자동화
8. MinIO/Spark SQL/HTML dashboard 기반 AWS 대체 구조 정리
9. 실시간 replay dashboard 추가
10. Airflow/K8s/HA 범위 명확화
11. Alert interface 및 log retention 설계 보완
12. 10x/100x 확장 전략 및 late conversion 처리 방향 정리

---

## 20. 선생님께 검토받고 싶은 부분

1. 현재 로컬 Docker 기반 구현 범위가 필수 과제 조건을 충족하는지 확인받고 싶다.
2. AWS 직접 사용 대신 MinIO/Spark SQL/HTML dashboard로 대체한 표현이 적절한지 확인받고 싶다.
3. Spark Streaming을 Airflow가 직접 트리거하지 않고, 로컬 streaming job으로 실행한 현재 구조가 제출 범위에서 충분한지 확인받고 싶다.
4. Airflow compaction 실패 후 Task Scheduler로 전환한 방식이 cron-style maintenance 자동화로 인정 가능한지 확인받고 싶다.
5. 장애 시나리오를 Kafka/MinIO/Gold missing 중심으로 정리했는데, 추가로 어떤 장애를 더 넣으면 좋은지 조언받고 싶다.
6. 파일 기반 alert log와 AlertSender 구조를 제시했는데, 실제 webhook까지 구현하는 것이 필요한지 확인받고 싶다.
7. log retention 정책은 README 설계로 충분한지, 실제 rotation script를 추가하는 것이 좋은지 확인받고 싶다.
8. 남은 시간에 OpenRTB/request topic을 추가하는 것보다 현재 구현의 안정성과 설명을 강화하는 것이 나은지 확인받고 싶다.
9. Gold summary를 현재 batch summary로 두고, late conversion MERGE INTO는 향후 확장으로 제시해도 되는지 확인받고 싶다.
10. 발표에서는 end-to-end 실행 결과, Iceberg metadata health query, compaction 자동화, 장애 감지/복구를 중심으로 강조해도 되는지 조언받고 싶다.

---

## 21. 프로젝트 산출물 구조

```text
final-project/
  README.md
  code/
    pipelines/
    health-queries/
    maintenance/
  monitoring/
  dashboard/
  orchestration/
  docs/
    images/
```

생성 결과물인 `warehouse/`, `iceberg_warehouse/`, checkpoint, parquet 파일은 GitHub 업로드 대상에서 제외한다.

---

## 22. 제출용 주요 캡처 목록

| 번호 | 캡처 내용 |
|---:|---|
| 01 | 전체 아키텍처 |
| 02 | Monitoring dashboard |
| 03 | 정상 health report |
| 04 | MinIO bucket console |
| 05 | MinIO warehouse uploaded |
| 06 | MinIO down alert |
| 07 | MinIO recovery OK |
| 08 | Iceberg table 생성 결과 |
| 09 | Iceberg snapshot metadata |
| 10 | Iceberg files metadata |
| 11 | Task Scheduler `LastTaskResult: 0` |
| 12 | Iceberg maintenance report `return_code: 0` |
| 13 | Iceberg maintenance alert `[OK]` |
| 14 | Live dashboard running |

---

## 23. 결론

본 프로젝트는 로컬 Docker 환경에서 광고 이벤트 Lakehouse의 핵심 경로를 실제로 실행 가능한 형태로 구현하고, 운영자가 점검해야 하는 health, metadata, compaction, 장애 감지, dashboard 지점을 검증하는 데 집중하였다.

최종 구현 범위:

```text
데이터 생성 및 이벤트화 완료
Kafka 이벤트 유입 모사 완료
Spark Structured Streaming 기반 Bronze 적재 완료
Bronze/Silver/Gold 계층 구성 완료
Apache Iceberg 테이블 생성 완료
Iceberg snapshot/files metadata health query 완료
MinIO 기반 S3 대체 저장소 구성 완료
pipeline health check 및 alert log 구현 완료
Iceberg compaction 실행 및 Task Scheduler 자동화 완료
비즈니스/운영/live replay dashboard 구성 완료
```

현재 구현은 프로덕션 전체 시스템이 아니라 로컬 PoC이지만, 광고 이벤트 파이프라인에서 중요한 수집, 정제, 집계, Iceberg 운영, 자동화, 장애 감지, 확장 설계의 핵심 요소를 검증하였다.
