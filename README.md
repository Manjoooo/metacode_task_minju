# Criteo 광고 이벤트 기반 Lakehouse 파이프라인

## 1. 프로젝트 개요

본 프로젝트는 Criteo Attribution Dataset을 활용하여 광고 이벤트 데이터를 수집, 정제, 집계하는 로컬 기반 데이터 파이프라인을 구현한 프로젝트.

광고 플랫폼에서는 사용자가 광고를 본 시점, 클릭한 시점, 전환한 시점이 서로 다르게 발생한다. 특히 conversion은 impression 또는 click 이후 지연되어 들어올 수 있고, producer 재전송이나 streaming job 재시작으로 인해 동일 이벤트가 중복 적재될 수도 있다.

본 프로젝트에서는 이러한 광고 데이터 특성을 반영하여 Criteo 원본 데이터를 impression, click, conversion 이벤트 스트림으로 변환하고, Kafka와 Spark를 이용하여 Bronze, Silver, Gold 계층으로 처리하는 파이프라인을 구현하였다.

```text
Criteo 원본 데이터
        ↓
이벤트 스트림 생성
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
Bronze Raw Zone
        ↓
Silver Processed Events
        ↓
Gold Campaign Summary
        ↓
Monitoring / Alerting 설계
```

현재 구현은 로컬 Docker 환경에서 Kafka와 Spark를 실행하고, Spark Structured Streaming 및 Spark batch job을 통해 Bronze, Silver, Gold 계층을 생성하는 방식으로 구성하였다.

AWS S3, Glue, Athena, QuickSight는 실제 사용하지 않았으며, 현재 프로젝트에서는 비용이 발생하지 않는 로컬 환경에서 파이프라인을 구현하였다. 운영 환경에서는 로컬 Parquet warehouse를 S3 또는 S3-compatible object storage, Iceberg table, Glue Catalog, Athena, Airflow 기반 구조로 확장할 수 있도록 설계하였다.

---

## 2. 프로젝트 목표

본 프로젝트의 목표는 단순히 광고 데이터를 저장하는 것이 아니라, 실제 광고 데이터 파이프라인에서 발생할 수 있는 운영 이슈를 고려한 Lakehouse 구조를 설계하고 구현하는 것이다.

주요 목표는 다음과 같다.

1. Criteo Attribution Dataset을 광고 이벤트 스트림 형태로 변환한다.
2. impression, click, conversion 이벤트를 Kafka topic으로 분리 발행한다.
3. Spark Structured Streaming을 이용하여 Kafka 데이터를 Bronze raw zone에 append-only로 저장한다.
4. Silver 계층에서 event_id 기준 중복 제거를 수행한다.
5. Gold 계층에서 campaign 단위 KPI를 계산한다.
6. Spark Streaming 중단, Producer 중복 재전송, Late Conversion 등 실제 운영 장애 시나리오를 정의한다.
7. 각 장애 시나리오에 대해 감지 지표, 알림 조건, 복구 방식을 설계한다.

---

## 3. 사용 기술

| 구분           | 현재 구현                         | 운영 환경 확장 방향                          |
| ------------ | ----------------------------- | ------------------------------------ |
| 데이터셋         | Criteo Attribution Dataset    | 실제 광고 로그                             |
| 이벤트 수집       | Kafka                         | Kafka cluster                        |
| Producer     | Python, kafka-python          | 운영 Producer / API Gateway            |
| Streaming 처리 | Spark Structured Streaming    | Spark on Kubernetes / EMR            |
| Batch 처리     | Spark                         | Spark batch / Airflow DAG            |
| 저장 포맷        | Parquet                       | Apache Iceberg table                 |
| 저장소          | Local Docker warehouse        | S3 / S3-compatible object storage    |
| 메타데이터 관리     | 로컬 경로 기반 관리                   | Glue Catalog / Iceberg Catalog       |
| 쿼리           | Spark job / local script      | Athena / Spark SQL                   |
| 모니터링         | alerts.log / health report 설계 | Slack, Discord, Email, Airflow alert |
| 실행 환경        | Local Docker                  | Cloud / Kubernetes                   |
| 대시보드 확장      | Gold table 기반 설계              | Streamlit / QuickSight               |

현재 구현은 로컬 Docker 기반 MVP이며, Iceberg, S3, Glue, Athena, QuickSight는 향후 운영 환경 확장 방향으로 설계하였다.

---

## 4. 디렉터리 구조

```text
final-project/
├── code/
│   └── pipelines/
│       ├── prepare_streaming_sample.py
│       ├── kafka_producer.py
│       ├── kafka_to_raw_files.py
│       ├── bronze_to_silver.py
│       └── gold_campaign_summary.py
│
├── data/
│   ├── original/
│   │   └── pcb_dataset_final.tsv
│   ├── sample_ad_events.csv
│   └── sample_ad_events_100k.csv
│
├── docs/
│   └── failure_scenarios.md              # 추가 예정
│
├── monitoring/
│   ├── check_pipeline_health.py          # 추가 예정
│   ├── alerts/
│   │   └── alerts.log                    # 추가 예정
│   └── reports/
│       └── health_report.md              # 추가 예정
│
├── screenshots/
│   ├── 01_kafka_topics_created.png
│   ├── 02_kafka_event_routing_verified.png
│   ├── 03_bronze_raw_parquet_partitioned.png
│   ├── 04_bronze_raw_count_verified.png
│   ├── 05_silver_dedup_verified.png
│   ├── 06_silver_partitioned_output.png
│   ├── 07_gold_campaign_summary_verified.png
│   ├── 08_gold_partitioned_output.png
│   ├── 09_final_100k_bronze_count_verified.png
│   ├── 10_final_100k_silver_dedup_verified.png
│   ├── 11_final_100k_silver_partitioned_output.png
│   ├── 12_final_100k_gold_campaign_summary_verified.png
│   └── 13_final_100k_gold_partitioned_output.png
│
├── warehouse/
│   ├── checkpoints/
│   ├── raw/
│   │   └── ad_events/
│   ├── silver/
│   │   └── processed_events/
│   └── gold/
│       └── campaign_summary/
│
└── README.md
```

---

## 5. 데이터셋 설명

본 프로젝트에서는 Criteo Attribution Dataset을 사용하였다.

원본 데이터는 impression 중심의 광고 로그 데이터이며, 각 row는 하나의 광고 노출 및 그 이후의 클릭/전환 정보를 포함한다. 원본 row가 곧바로 하나의 이벤트를 의미하는 것은 아니기 때문에, 본 프로젝트에서는 원본 row를 이벤트 스트림 형태로 재구성하였다.

### 5-1. 원본 데이터 보존

원본 데이터는 다음 위치에 보존하였다.

```text
data/original/pcb_dataset_final.tsv
```

원본을 직접 수정하지 않고, 별도의 streaming sample 파일을 생성하여 파이프라인 입력으로 사용하였다.

### 5-2. 이벤트 스트림 변환 규칙

Criteo 원본 row를 다음 규칙에 따라 event stream으로 변환하였다.

| 조건                        | 생성 이벤트                       |
| ------------------------- | ---------------------------- |
| 모든 row                    | impression event             |
| click = 1                 | click event                  |
| conversion = 1            | conversion event             |
| click = 0, conversion = 1 | view-through conversion으로 허용 |

즉, 모든 row는 최소 1개의 impression event를 생성하고, click 또는 conversion이 있는 경우 추가 이벤트를 생성한다.

### 5-3. 최종 이벤트 생성 결과

최종 실행에서는 Criteo 원본 데이터 중 100,000 rows를 사용하여 약 30일 기간의 광고 이벤트 스트림을 생성하였다.

생성 명령어:

```powershell
python code\pipelines\prepare_streaming_sample.py --input data\pcb_dataset_final.tsv --output data\sample_ad_events_100k.csv --max-rows 100000 --base-time "2026-06-16 00:00:00"
```

생성 결과:

```text
Input rows: 100000
Output events: 140561

event_type counts:
impression    100000
click          35411
conversion      5150

event_time range:
2026-06-16 00:00:00 ~ 2026-07-16 08:20:24
```

최종 sample event stream은 다음 파일에 저장하였다.

```text
data/sample_ad_events_100k.csv
```

---

## 6. Kafka Topic 설계

광고 이벤트는 event_type에 따라 서로 다른 Kafka topic으로 분리하였다.

| event_type | Kafka topic    |
| ---------- | -------------- |
| impression | ad-impressions |
| click      | ad-clicks      |
| conversion | ad-conversions |

Topic 생성 명령어는 다음과 같다.

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-impressions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-clicks --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-conversions --partitions 3 --replication-factor 1
```

Topic 생성 확인 결과:

```text
ad-clicks
ad-conversions
ad-impressions
```

로컬 Docker 환경에서는 단일 Kafka broker를 사용하였으므로 replication factor는 1로 설정하였다. 운영 환경에서는 broker를 3대 이상 구성하고 replication factor를 3으로 설정하는 방식으로 확장할 수 있다.

### 증빙 캡처

```text
screenshots/01_kafka_topics_created.png
```

---

## 7. Kafka Producer 구현

`kafka_producer.py`는 event stream CSV를 읽고, `event_type`에 따라 적절한 Kafka topic으로 이벤트를 발행한다.

```text
impression  → ad-impressions
click       → ad-clicks
conversion  → ad-conversions
```

최종 100k 이벤트 스트림 발행 명령어:

```powershell
python code\pipelines\kafka_producer.py --csv data\sample_ad_events_100k.csv --bootstrap-servers localhost:9092 --max-events 999999999 --sleep 0 --log-every 10000
```

최종 발행 결과:

```text
[producer] total_sent=140561

sent_counts:
impression = 100000
click      = 35411
conversion = 5150
```

Kafka consumer로 각 topic에 JSON 메시지가 정상적으로 들어간 것을 확인하였다.

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-impressions --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-clicks --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-conversions --from-beginning --max-messages 3
```

### 증빙 캡처

```text
screenshots/02_kafka_event_routing_verified.png
```

---

## 8. Bronze Layer: Raw Zone

Bronze 계층은 Kafka에서 수집한 이벤트를 원본에 가깝게 저장하는 append-only raw zone이다.

Bronze의 목적은 정제나 집계가 아니라 원본 보존, 장애 추적, 재처리 source 확보이다. 따라서 중복 이벤트가 발생해도 Bronze에서는 삭제하지 않는다.

### 8-1. Bronze 저장 방식

Spark Structured Streaming으로 Kafka 3개 topic을 구독하고, Parquet 형식으로 저장하였다.

```text
Kafka Topics
  - ad-impressions
  - ad-clicks
  - ad-conversions
        ↓
Spark Structured Streaming
        ↓
warehouse/raw/ad_events
```

Bronze 파티션 기준은 다음과 같다.

```text
event_type
raw_date
raw_hour
```

저장 구조:

```text
warehouse/raw/ad_events/
├── _spark_metadata/
├── event_type=click/
├── event_type=conversion/
└── event_type=impression/
```

### 8-2. Bronze에 저장한 주요 컬럼

| 컬럼              | 설명                            |
| --------------- | ----------------------------- |
| event_id        | 이벤트 고유 ID                     |
| source_row_id   | 원본 Criteo row ID              |
| event_type      | impression, click, conversion |
| event_time      | 이벤트 발생 시각                     |
| producer_time   | producer 발행 시각                |
| ingest_time     | Spark 수집 시각                   |
| kafka_topic     | Kafka topic                   |
| kafka_partition | Kafka partition               |
| kafka_offset    | Kafka offset                  |
| user_id         | 사용자 ID                        |
| campaign_id     | 캠페인 ID                        |
| cost            | 광고 비용                         |
| revenue_proxy   | 전환 revenue proxy              |

### 8-3. Bronze 검증 결과

최종 100k 이벤트 스트림 기준 Bronze 적재 결과는 다음과 같다.

```text
=== count by event_type ===
impression = 100000
click      = 35411
conversion = 5150

=== total count ===
140561
```

Producer가 발행한 총 이벤트 수와 Bronze total count가 일치함을 확인하였다.

### 8-4. Streaming 지연 사례

Producer는 전체 이벤트를 발행했지만, 첫 번째 Bronze 확인 시 일부 이벤트만 적재된 상태가 발생하였다.

```text
expected total = 140561
first bronze count = 131507
```

이후 Spark Structured Streaming을 동일 checkpoint 경로로 다시 실행하여 최종적으로 Bronze count가 140,561건으로 정상화되었다. 이 사례는 Spark Streaming 중단 또는 적재 지연 상황에서 checkpoint 기반 재시작으로 복구할 수 있음을 보여준다.

### 증빙 캡처

```text
screenshots/03_bronze_raw_parquet_partitioned.png
screenshots/04_bronze_raw_count_verified.png
screenshots/09_final_100k_bronze_count_verified.png
```

---

## 9. Silver Layer: Processed Events

Silver 계층은 Bronze raw 데이터를 분석 가능한 형태로 정제한 계층이다.

Silver에서는 다음 처리를 수행하였다.

1. `event_time`, `producer_time`, `ingest_time` timestamp 정리
2. `event_date`, `event_hour` 생성
3. `event_type`별 flag 생성
4. `event_id` 기준 중복 제거
5. downstream Gold 집계를 위한 표준 스키마 생성

### 9-1. Deduplication 전략

Bronze는 raw 보존 목적이므로 중복을 삭제하지 않는다. 대신 Silver에서 `event_id` 기준으로 중복 제거를 수행한다.

동일 `event_id`가 여러 개 존재하는 경우, `producer_ts`가 가장 늦고 `kafka_offset`이 가장 큰 record를 선택하였다.

```text
partition by event_id
order by producer_ts desc, kafka_offset desc
row_number = 1
```

### 9-2. Silver 검증 결과

최종 100k 이벤트 스트림 기준 Silver 결과는 다음과 같다.

```text
=== Silver count by event_type ===
impression = 100000
click      = 35411
conversion = 4455
```

Bronze conversion count는 5,150건이었으나, Silver에서는 event_id 기준 deduplication 이후 conversion이 4,455건으로 정리되었다. 이는 Bronze raw 계층에는 중복 가능성이 있는 이벤트를 그대로 보존하고, Silver 분석 계층에서 중복 제거를 수행하는 구조를 보여준다.

### 9-3. Silver 저장 구조

Silver는 다음 기준으로 파티셔닝하였다.

```text
event_type
event_date
event_hour
```

저장 구조:

```text
warehouse/silver/processed_events/
├── _SUCCESS
├── event_type=click/
├── event_type=conversion/
└── event_type=impression/
```

### 증빙 캡처

```text
screenshots/05_silver_dedup_verified.png
screenshots/06_silver_partitioned_output.png
screenshots/10_final_100k_silver_dedup_verified.png
screenshots/11_final_100k_silver_partitioned_output.png
```

---

## 10. Gold Layer: Campaign Summary

Gold 계층은 BI 대시보드와 비즈니스 KPI 조회를 위한 집계 계층이다.

Gold에서는 Silver `processed_events`를 기준으로 `event_date`, `campaign_id` 단위의 campaign summary를 생성하였다.

### 10-1. 집계 지표

| 지표            | 설명                          |
| ------------- | --------------------------- |
| impressions   | impression event 수          |
| clicks        | click event 수               |
| conversions   | conversion event 수          |
| ad_cost       | impression cost 합계          |
| revenue_proxy | conversion revenue proxy 합계 |
| CTR           | clicks / impressions        |
| CVR           | conversions / clicks        |
| ROAS          | revenue_proxy / ad_cost     |

### 10-2. Gold 검증 결과

Gold 생성 전 Silver count:

```text
impression = 100000
click      = 35411
conversion = 4455
```

Gold campaign summary 생성 결과:

```text
Gold total campaign-date rows = 2409
```

Gold sample:

```text
event_date  campaign_id  impressions  clicks  conversions  ad_cost     revenue_proxy  ctr       cvr       roas
2026-06-16  32368244     1098         560     84           0.96000032  1.33964217     0.510018  0.150000  1.395460
2026-06-16  5544859      378          208     83           0.05001827  0.33200000     0.550265  0.399038  6.637575
2026-06-16  10341182     5025         2171    77           1.34662486  5.18199072     0.432040  0.035468  3.848132
```

### 10-3. Gold 저장 구조

Gold는 `event_date` 기준으로 파티셔닝하였다.

```text
warehouse/gold/campaign_summary/
├── _SUCCESS
├── event_date=2026-06-16/
├── event_date=2026-06-17/
├── ...
└── event_date=2026-07-16/
```

Gold 결과는 2026-06-16부터 2026-07-16까지 날짜별 partition으로 저장되었다.

### 증빙 캡처

```text
screenshots/07_gold_campaign_summary_verified.png
screenshots/08_gold_partitioned_output.png
screenshots/12_final_100k_gold_campaign_summary_verified.png
screenshots/13_final_100k_gold_partitioned_output.png
```

---

## 11. 현재 구현 완료 범위

현재까지 구현 완료된 범위는 다음과 같다.

| 단계                | 구현 여부 | 설명                                                 |
| ----------------- | ----- | -------------------------------------------------- |
| 원본 데이터 보존         | 완료    | Criteo 원본 TSV를 `data/original`에 보존                 |
| 이벤트 스트림 생성        | 완료    | Criteo 100,000 rows를 140,561개 이벤트로 변환              |
| Kafka topic 구성    | 완료    | ad-impressions, ad-clicks, ad-conversions 생성       |
| Kafka Producer    | 완료    | event_type별 topic 분리 발행                            |
| Kafka Consumer 검증 | 완료    | topic별 JSON 메시지 확인                                 |
| Bronze 저장         | 완료    | Spark Structured Streaming으로 raw Parquet append 저장 |
| Bronze 파티션 검증     | 완료    | event_type/raw_date/raw_hour 파티션 확인                |
| Silver 정제         | 완료    | timestamp 정리 및 event_id dedup                      |
| Silver 파티션 검증     | 완료    | event_type/event_date/event_hour 파티션 확인            |
| Gold 집계           | 완료    | campaign_id + event_date 기준 KPI 계산                 |
| Gold 파티션 검증       | 완료    | event_date 파티션 확인                                  |
| Docker 결과 복사      | 완료    | `/tmp/warehouse` 결과를 프로젝트 `warehouse` 폴더로 복사       |
| 장애 시나리오 설계        | 진행 중  | 인프라 장애, 중복 이벤트, late conversion 3개로 재정리            |
| Monitoring script | 구현 예정 | health check 및 alerts.log 생성 예정                    |

---

## 12. 장애 시나리오 및 운영 대응 설계

본 프로젝트에서는 광고 데이터 파이프라인에서 실제로 발생할 수 있는 장애를 세 가지로 정의한다.

초기에는 중복 이벤트, late conversion, small file 문제처럼 데이터 품질 이슈 중심으로 장애를 정의하였다. 이후 피드백을 반영하여 인프라 HA 관점과 알림 가능한 지표를 포함하는 방향으로 장애 시나리오를 다시 정리하였다.

최종 장애 시나리오는 다음 세 가지이다.

```text
1. Spark Streaming 중단 / Kafka 적재 지연
2. Producer 재전송으로 인한 중복 이벤트
3. Late Conversion으로 인한 과거 KPI 변경
```

각 시나리오는 다음 기준으로 정리하였다.

```text
장애 상황
→ 영향
→ 감지 지표
→ 알림 조건
→ 복구 방식
→ 현재 로컬 구현 범위
→ 운영 환경 확장 방향
```

---

### 12-1. 시나리오 1: Spark Streaming 중단 / Kafka 적재 지연

#### 장애 상황

Spark Structured Streaming job이 중단되거나 지연되면 Kafka에는 이벤트가 쌓여 있지만 Bronze 계층에는 데이터가 적재되지 않을 수 있다.

실제 운영 환경에서는 다음과 같은 이유로 발생할 수 있다.

* Spark driver 또는 container 종료
* executor memory 부족
* Kafka consumer 오류
* 네트워크 지연
* checkpoint 경로 문제
* Docker 또는 Kubernetes 재시작

#### 영향

Streaming ingestion이 멈추면 Bronze raw zone이 최신 상태로 갱신되지 않는다. 이 경우 Silver/Gold도 함께 지연되며, 대시보드나 campaign summary가 오래된 데이터를 보여줄 수 있다.

#### 감지 지표

* Bronze total count
* expected event count와 Bronze actual count 차이
* 최근 Bronze `ingest_time`
* Spark streaming process 상태
* checkpoint 갱신 여부
* Kafka consumer lag

#### 알림 조건

다음 조건 중 하나라도 발생하면 알림 대상으로 본다.

* 일정 시간 이상 Bronze 신규 데이터가 적재되지 않음
* expected count보다 Bronze count가 부족함
* Spark streaming job이 종료됨
* Kafka lag가 지속적으로 증가함

알림 예시는 다음과 같다.

```text
[ALERT] Bronze count mismatch: expected=140561, actual=131507
[ALERT] Bronze ingestion stale: no new data in recent window
```

#### 복구 방식

Spark Structured Streaming은 checkpoint를 사용하여 Kafka offset 처리 상태를 기록한다. 따라서 동일한 checkpoint 경로로 streaming job을 재시작하면 이미 처리한 offset 이후부터 이어서 처리할 수 있다.

복구 후에는 Bronze count를 다시 확인하여 expected count와 일치하는지 검증한다.

#### 현재 프로젝트에서의 적용

현재 로컬 환경에서는 단일 Kafka broker와 단일 Spark streaming job으로 구성되어 있다. 따라서 완전한 HA 구성을 구현한 것은 아니지만, checkpoint 기반 재시작과 count mismatch 감지를 통해 ingestion 장애를 확인하고 복구하는 구조를 설계하였다.

실제 실행 과정에서도 Producer는 140,561건을 모두 발행했지만, 첫 번째 Bronze 확인 시 131,507건만 적재된 상태가 발생하였다. 이후 동일 checkpoint 경로로 Spark Streaming을 다시 실행하여 최종적으로 Bronze count가 140,561건으로 정상화되었다.

#### 운영 환경 확장

운영 환경으로 확장할 경우 다음과 같은 HA 구성이 필요하다.

* Kafka broker 3대 이상 구성
* topic replication factor 설정
* producer `acks=all` 설정
* Spark job을 Airflow 또는 Kubernetes에서 관리
* streaming job failure 발생 시 자동 재시작
* Kafka lag와 Bronze ingest delay 알림 연동

---

### 12-2. 시나리오 2: Producer 재전송으로 인한 중복 이벤트

#### 장애 상황

Producer가 네트워크 오류, timeout, 수동 재실행 등으로 동일한 이벤트를 다시 Kafka에 발행할 수 있다. 이 경우 Bronze에는 동일한 `event_id`를 가진 이벤트가 중복 저장된다.

#### 영향

중복 이벤트가 제거되지 않은 상태로 Gold 집계에 사용되면 impressions, clicks, conversions가 실제보다 크게 계산된다. 특히 conversion이 중복되면 revenue_proxy와 ROAS가 왜곡될 수 있다.

#### 감지 지표

* Bronze total count
* Bronze distinct `event_id` count
* duplicate count
* duplicate rate
* Bronze count와 Silver count 차이

#### 알림 조건

다음 조건을 알림 기준으로 둔다.

* `duplicate_count > 0`
* `duplicate_rate > 1%`
* Bronze/Silver count 차이가 일정 기준 이상 증가

알림 예시는 다음과 같다.

```text
[ALERT] Duplicate events detected: duplicate_count=5000, duplicate_rate=3.4%
```

#### 복구 방식

Bronze는 raw append-only 계층이므로 중복 이벤트도 삭제하지 않고 보존한다. 대신 Silver 계층에서 `event_id` 기준 deduplication을 수행하고, Gold는 반드시 Silver 데이터를 기준으로 재집계한다.

이 구조를 통해 raw 데이터는 추적 가능하게 보존하면서도, 분석용 KPI는 중복으로 인해 왜곡되지 않도록 한다.

#### 현재 프로젝트에서의 적용

현재 파이프라인은 Bronze와 Silver를 분리하고 있으며, Silver 처리 단계에서 `event_id` 기준 중복 제거를 수행한다.

장애 재현은 같은 이벤트 파일 일부를 Producer로 다시 발행하는 방식으로 수행할 수 있다.

```text
정상 상태:
Bronze total = 140,561

장애 주입:
기존 이벤트 일부를 다시 Producer로 발행

감지:
Bronze total count 증가
distinct event_id와 total count 차이 발생
duplicate_rate 증가

복구:
Silver dedup 재실행
Gold campaign_summary 재집계
```

---

### 12-3. 시나리오 3: Late Conversion으로 인한 과거 KPI 변경

#### 장애 상황

광고 데이터에서는 impression 또는 click이 발생한 뒤 conversion이 며칠 뒤에 들어올 수 있다. 예를 들어 사용자가 광고를 클릭한 당일에는 구매하지 않았지만, 며칠 뒤 구매를 완료할 수 있다.

이 경우 단순히 conversion 발생일 기준으로 Gold를 집계하면, 과거 campaign 성과가 뒤늦게 변경되는 문제가 발생한다.

#### 영향

Late conversion을 고려하지 않으면 다음과 같은 문제가 생길 수 있다.

* 과거 campaign의 CVR, ROAS가 실제보다 낮게 보임
* conversion 발생일에는 impressions/clicks 없이 conversions만 존재하는 row가 생김
* 광고 예산 최적화 판단이 왜곡될 수 있음
* 과거 Gold partition 재처리가 필요해짐

#### 감지 지표

* `impressions = 0` 또는 `clicks = 0`인데 `conversions > 0`인 Gold row 수
* late conversion count
* late conversion rate
* conversion event_time과 원본 impression/click event_time의 차이
* 과거 날짜 partition에 새롭게 반영되어야 하는 conversion 수

#### 알림 조건

다음 조건을 late conversion 의심 상황으로 본다.

* 특정 campaign에서 late conversion row가 일정 기준 이상 발생
* `impressions = 0`이고 `conversions > 0`인 Gold row 발생
* 과거 날짜 Gold partition에 conversion 재반영이 필요함

알림 예시는 다음과 같다.

```text
[ALERT] Late conversion suspected: event_date=2026-06-17, campaign_id=10341182, conversions=33
```

#### 복구 방식

현재 로컬 구현에서는 event_date 기준 Gold `campaign_summary`를 생성한다. 이후 개선 방향으로는 conversion을 `impression_id` 또는 `click_id` 기준으로 원본 impression/click과 연결하고, attribution 기준 날짜로 campaign summary를 다시 계산하는 구조를 고려한다.

운영 환경에서는 Iceberg `MERGE INTO`를 사용하여 과거 Gold partition을 업데이트할 수 있다.

```text
현재 구조:
event_date 기준 campaign_summary

개선 구조:
attribution_date 기준 campaign_summary
→ late conversion 발생 시 과거 partition update
```

#### 현재 프로젝트에서의 적용

현재 Gold 결과에서도 일부 날짜에 impressions/clicks 없이 conversions만 존재하는 row가 확인된다. 이를 late conversion 의심 케이스로 감지하고, monitoring job에서 alerts.log에 기록하는 방식으로 운영 시나리오를 구성한다.

---

## 13. Monitoring & Alerting 설계

장애 시나리오를 실제 운영 관점으로 다루기 위해, 본 프로젝트에서는 별도의 monitoring layer를 추가할 예정이다.

로컬 구현에서는 비용이 들지 않는 방식으로 `monitoring/check_pipeline_health.py`를 실행하여 Bronze/Silver/Gold 상태를 점검하고, 이상 조건이 발생하면 `monitoring/alerts/alerts.log`에 기록한다.

운영 환경에서는 동일한 알림 조건을 Slack, Discord webhook, Email, Airflow alert 등으로 확장할 수 있다.

### 13-1. Monitoring 대상

| 모니터링 항목                         | 목적                        |
| ------------------------------- | ------------------------- |
| Bronze total count              | Kafka에서 들어온 raw 적재 건수 확인  |
| Silver total count              | dedup 이후 분석 기준 이벤트 수 확인   |
| Gold row count                  | campaign summary 생성 여부 확인 |
| duplicate count                 | 중복 이벤트 발생 여부 확인           |
| duplicate rate                  | 중복 이벤트 비율 확인              |
| latest ingest_time              | streaming 적재 지연 여부 확인     |
| late conversion suspicious rows | late conversion 의심 케이스 확인 |
| partition file count            | small file 증가 여부 확인       |

### 13-2. Alert 조건

| 장애 유형           | Alert 조건                      | Alert 예시                            |
| --------------- | ----------------------------- | ----------------------------------- |
| Streaming 적재 지연 | expected count > Bronze count | `[ALERT] Bronze count mismatch`     |
| Streaming 중단    | 최근 Bronze ingest 갱신 없음        | `[ALERT] Bronze ingestion stale`    |
| 중복 이벤트          | duplicate_rate > 1%           | `[ALERT] Duplicate events detected` |
| Late conversion | conversions만 존재하는 Gold row 발생 | `[ALERT] Late conversion suspected` |
| Gold 생성 실패      | Gold row count = 0            | `[ALERT] Gold summary is empty`     |

### 13-3. Alert 저장 구조

```text
monitoring/
  ├── check_pipeline_health.py
  ├── alerts/
  │   └── alerts.log
  └── reports/
      └── health_report.md
```

`alerts.log`는 장애 발생 여부를 누적 기록하는 파일이다.

예시:

```text
2026-06-20 23:21:10 [ALERT] Bronze count mismatch: expected=140561, actual=131507
2026-06-20 23:26:31 [RESOLVED] Bronze count matched: expected=140561, actual=140561
2026-06-20 23:40:12 [ALERT] Duplicate events detected: duplicate_count=5000, duplicate_rate=3.4%
2026-06-20 23:45:08 [RECOVERY] Silver dedup and Gold aggregation completed
```

### 13-4. 운영 환경 확장

현재 프로젝트는 로컬 Docker 기반으로 구현했지만, 운영 환경에서는 다음과 같이 확장할 수 있다.

| 현재 로컬 구현                | 운영 환경 확장                                 |
| ----------------------- | ---------------------------------------- |
| 단일 Kafka broker         | Kafka broker 3대 이상 + replication         |
| 수동 Spark job 실행         | Airflow/Kubernetes 기반 자동 재시작             |
| local Parquet warehouse | S3 또는 S3-compatible object storage       |
| alerts.log              | Slack/Discord/Email alert                |
| 수동 health check         | Airflow schedule 기반 정기 health check      |
| event_date 기준 Gold      | Iceberg MERGE 기반 attribution Gold update |

---

## 14. 실행 방법

### 14-1. Docker 환경 실행

Kafka/Spark Docker compose 폴더에서 실행한다.

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 부캠\kafka_강의자료_메타코드M\kafka-docker-envs\kafka-spark-airflow"

docker compose up -d
docker compose ps
```

---

### 14-2. Kafka topic 생성

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-impressions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-clicks --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-conversions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

---

### 14-3. 이벤트 스트림 생성

final-project 폴더에서 실행한다.

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project"

python code\pipelines\prepare_streaming_sample.py --input data\pcb_dataset_final.tsv --output data\sample_ad_events_100k.csv --max-rows 100000 --base-time "2026-06-16 00:00:00"
```

---

### 14-4. Kafka Producer 실행

```powershell
python code\pipelines\kafka_producer.py --csv data\sample_ad_events_100k.csv --bootstrap-servers localhost:9092 --max-events 999999999 --sleep 0 --log-every 10000
```

---

### 14-5. Kafka 메시지 확인

Kafka/Spark compose 폴더에서 실행한다.

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-impressions --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-clicks --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-conversions --from-beginning --max-messages 3
```

---

### 14-6. Kafka → Bronze 실행

`kafka_to_raw_files.py`를 Spark 컨테이너로 복사한다.

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\kafka_to_raw_files.py" spark-master:/tmp/kafka_to_raw_files.py
```

Spark Structured Streaming 실행:

```powershell
docker compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /tmp/kafka_to_raw_files.py --bootstrap-servers kafka:29092 --topics ad-impressions,ad-clicks,ad-conversions --output /tmp/warehouse/raw/ad_events --checkpoint /tmp/warehouse/checkpoints/kafka_to_raw_ad_events --trigger-seconds 10
```

Bronze 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/raw/ad_events
```

---

### 14-7. Bronze count 확인

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\check_bronze.py" spark-master:/tmp/check_bronze.py

docker compose exec spark-master spark-submit /tmp/check_bronze.py
```

---

### 14-8. Bronze → Silver 실행

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\bronze_to_silver.py" spark-master:/tmp/bronze_to_silver.py

docker compose exec spark-master spark-submit /tmp/bronze_to_silver.py --input /tmp/warehouse/raw/ad_events --output /tmp/warehouse/silver/processed_events
```

Silver 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/silver/processed_events
```

---

### 14-9. Silver → Gold 실행

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\gold_campaign_summary.py" spark-master:/tmp/gold_campaign_summary.py

docker compose exec spark-master spark-submit /tmp/gold_campaign_summary.py --input /tmp/warehouse/silver/processed_events --output /tmp/warehouse/gold/campaign_summary
```

Gold 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/gold/campaign_summary
```

---

### 14-10. Docker 결과를 로컬 프로젝트로 복사

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project"

docker cp spark-master:/tmp/warehouse/. "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\warehouse"
```

확인:

```powershell
dir warehouse
dir warehouse\raw\ad_events
dir warehouse\silver\processed_events
dir warehouse\gold\campaign_summary
```

---

## 15. 제출용 캡처 목록

| 번호 | 파일명                                                | 설명                             |
| -- | -------------------------------------------------- | ------------------------------ |
| 01 | `01_kafka_topics_created.png`                      | Kafka topic 3개 생성 확인           |
| 02 | `02_kafka_event_routing_verified.png`              | event_type별 topic 라우팅 확인       |
| 03 | `03_bronze_raw_parquet_partitioned.png`            | Bronze raw Parquet 파티션 생성 확인   |
| 04 | `04_bronze_raw_count_verified.png`                 | Bronze event_type별 count 확인    |
| 05 | `05_silver_dedup_verified.png`                     | Silver dedup 결과 확인             |
| 06 | `06_silver_partitioned_output.png`                 | Silver 파티션 생성 확인               |
| 07 | `07_gold_campaign_summary_verified.png`            | Gold campaign summary KPI 확인   |
| 08 | `08_gold_partitioned_output.png`                   | Gold 파티션 생성 확인                 |
| 09 | `09_final_100k_bronze_count_verified.png`          | 100k final Bronze count 확인     |
| 10 | `10_final_100k_silver_dedup_verified.png`          | 100k final Silver dedup 확인     |
| 11 | `11_final_100k_silver_partitioned_output.png`      | 100k final Silver partition 확인 |
| 12 | `12_final_100k_gold_campaign_summary_verified.png` | 100k final Gold KPI 확인         |
| 13 | `13_final_100k_gold_partitioned_output.png`        | 100k final Gold partition 확인   |
| 14 | `14_monitoring_alert_log.png`                      | monitoring alert log 확인 예정     |
| 15 | `15_failure_recovery_report.png`                   | 장애 복구 리포트 확인 예정                |

---

## 16. 현재 한계와 확장 방향

현재 구현은 로컬 Docker 기반 Parquet Lakehouse MVP이다. 따라서 운영형 Lakehouse로 확장하기 위해서는 다음 보완이 필요하다.

### 16-1. Iceberg 적용

현재 Bronze, Silver, Gold는 Parquet 파일 기반으로 저장하였다. 향후에는 Silver와 Gold 계층을 Iceberg table로 구성하여 다음 기능을 적용할 수 있다.

* snapshot 관리
* schema evolution
* time travel
* MERGE INTO 기반 late conversion 반영
* rewrite_data_files 기반 compaction
* files/history/snapshots metadata table 조회

### 16-2. Late Conversion 보정

현재 Gold는 Silver 기준 batch 집계로 생성하였다. 향후 late conversion 이벤트가 뒤늦게 들어오는 경우, 최근 N일 window를 재계산하거나 Iceberg MERGE INTO로 기존 campaign_summary를 갱신하도록 확장할 수 있다.

### 16-3. Airflow 자동화

현재 각 pipeline은 수동 명령어로 실행하였다. 향후 Airflow DAG를 통해 다음 작업을 자동화할 수 있다.

* Kafka ingestion 상태 확인
* Bronze → Silver 정제
* Silver → Gold 집계
* health check job 실행
* alert log 생성
* compaction job 실행
* backfill job 실행
* failure retry 및 alert

### 16-4. Dashboard 연결

Gold campaign_summary는 BI dashboard의 source table로 사용할 수 있다. 향후 Streamlit 또는 QuickSight를 통해 다음 지표를 시각화할 수 있다.

* campaign별 impressions
* clicks
* conversions
* CTR
* CVR
* ROAS
* late conversion count
* duplicate event count
* file count 및 small file 모니터링

### 16-5. 인프라 HA 확장

현재 로컬 구현은 단일 Kafka broker와 단일 Spark job 기반이다. 운영 환경에서는 다음과 같이 HA 구성을 확장할 수 있다.

* Kafka broker 3대 이상 구성
* topic replication factor 3 설정
* producer `acks=all` 설정
* Spark job 자동 재시작
* checkpoint 기반 streaming 복구
* Airflow/Kubernetes 기반 job orchestration
* Slack/Discord/Email 기반 alerting

---

## 17. 프로젝트 요약

본 프로젝트는 Criteo 광고 데이터를 기반으로 이벤트 스트림을 생성하고, Kafka와 Spark를 이용하여 Bronze, Silver, Gold 계층으로 이어지는 광고 데이터 파이프라인을 구현하였다.

최종 실행에서는 Criteo 원본 데이터 100,000 rows를 기반으로 30일 기간의 광고 이벤트 스트림 140,561건을 생성하였다. 생성된 이벤트는 event_type에 따라 Kafka topic으로 분리 발행되었고, Spark Structured Streaming을 통해 Bronze raw zone에 적재되었다.

최종 구현 결과는 다음과 같다.

```text
Input:
- Criteo source rows = 100000

Generated events:
- total events = 140561
- impression = 100000
- click = 35411
- conversion = 5150

Kafka topics:
- ad-impressions
- ad-clicks
- ad-conversions

Bronze:
- total count = 140561
- impression = 100000
- click = 35411
- conversion = 5150

Silver:
- impression = 100000
- click = 35411
- conversion = 4455

Gold:
- campaign-date rows = 2409
- metrics = impressions, clicks, conversions, ad_cost, revenue_proxy, CTR, CVR, ROAS
```

또한 단순한 정상 파이프라인 구현에서 끝나지 않고, 다음 세 가지 장애 시나리오를 운영 관점에서 재정리하였다.

```text
1. Spark Streaming 중단 / Kafka 적재 지연
2. Producer 재전송으로 인한 중복 이벤트
3. Late Conversion으로 인한 과거 KPI 변경
```

각 장애 시나리오에 대해 감지 지표, 알림 조건, 복구 방식을 정의하였고, 향후 `monitoring/check_pipeline_health.py`와 `alerts.log`를 통해 로컬 환경에서도 장애 감지와 알림 기록이 가능하도록 확장할 예정이다.

이를 통해 광고 이벤트 수집, raw 보존, 중복 제거, 캠페인 KPI 집계, 장애 감지 설계까지 이어지는 데이터 엔지니어링 파이프라인의 핵심 흐름을 구현하였다.
