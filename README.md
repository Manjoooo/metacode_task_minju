# Criteo 광고 이벤트 기반 Lakehouse 파이프라인

## 1. 프로젝트 개요

본 프로젝트는 Criteo Attribution Dataset을 활용하여 광고 이벤트 데이터를 수집, 정제, 집계하는 로컬 기반 데이터 파이프라인을 구현한 프로젝트이다.

광고 플랫폼에서는 사용자가 광고를 본 시점, 클릭한 시점, 전환한 시점이 서로 다르게 발생한다. 또한 실제 운영 환경에서는 Kafka broker 장애, streaming job 중단, batch 집계 실패처럼 데이터가 정상적으로 흘러가지 않는 상황도 발생할 수 있다.

본 프로젝트에서는 Criteo 원본 데이터를 impression, click, conversion 이벤트 스트림으로 변환하고, Kafka와 Spark를 이용하여 Bronze, Silver, Gold 계층으로 처리하는 파이프라인을 구현하였다.

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
```

현재 구현은 로컬 Docker 환경에서 Kafka와 Spark를 실행하고, Spark Structured Streaming 및 Spark batch job을 통해 Bronze, Silver, Gold 계층을 생성하는 방식으로 구성하였다.

AWS S3, Glue, Athena, QuickSight는 실제 사용하지 않았으며, 현재 프로젝트에서는 비용이 발생하지 않는 로컬 환경에서 파이프라인을 구현하였다. 다만 운영 환경에서는 로컬 Parquet warehouse를 S3 또는 S3-compatible object storage, Iceberg table, Glue Catalog, Athena, Airflow 기반 구조로 확장할 수 있도록 설계하였다.

---

## 2. 프로젝트 목표

본 프로젝트의 목표는 단순히 광고 데이터를 저장하는 것이 아니라, 실제 광고 이벤트 파이프라인에서 필요한 수집, 정제, 집계, 장애 감지 구조를 함께 설계하는 것이다.

주요 목표는 다음과 같다.

1. Criteo Attribution Dataset을 광고 이벤트 스트림 형태로 변환한다.
2. impression, click, conversion 이벤트를 Kafka topic으로 분리 발행한다.
3. Spark Structured Streaming을 이용하여 Kafka 데이터를 Bronze raw zone에 append-only로 저장한다.
4. Silver 계층에서 event_id 기준 중복 제거를 수행한다.
5. Gold 계층에서 campaign 단위 KPI를 계산한다.
6. 운영 장애 시나리오를 Kafka, Streaming, Batch 계층으로 나누어 정의한다.
7. 각 장애에 대해 감지 지표, 알림 조건, 복구 방식을 설계한다.
8. 알림은 특정 로그 파일에만 의존하지 않고, 향후 webhook으로 확장 가능한 인터페이스 구조로 설계한다.

---

## 3. 사용 기술

| 구분           | 현재 구현                            | 운영 환경 확장 방향                       |
| ------------ | -------------------------------- | --------------------------------- |
| 데이터셋         | Criteo Attribution Dataset       | 실제 광고 로그                          |
| 이벤트 수집       | Kafka                            | Kafka cluster                     |
| Producer     | Python, kafka-python             | 운영 Producer / API Gateway         |
| Streaming 처리 | Spark Structured Streaming       | Spark on Kubernetes / EMR         |
| Batch 처리     | Spark                            | Spark batch / Airflow DAG         |
| 저장 포맷        | Parquet                          | Apache Iceberg table              |
| 저장소          | Local Docker warehouse           | S3 / S3-compatible object storage |
| 메타데이터 관리     | 로컬 경로 기반 관리                      | Glue Catalog / Iceberg Catalog    |
| 쿼리           | Spark job / local script         | Athena / Spark SQL                |
| 모니터링         | AlertSender 설계, local file alert | Slack / Discord / Email webhook   |
| 실행 환경        | Local Docker                     | Cloud / Kubernetes                |
| 대시보드 확장      | Gold table 기반 설계                 | Streamlit / QuickSight            |

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
│   └── failure_scenarios.md              # 장애 시나리오 및 복구 절차 정리 예정
│
├── monitoring/
│   ├── check_pipeline_health.py          # 구현 예정
│   ├── alert_sender.py                   # 구현 예정
│   ├── alerts/
│   │   └── alerts.log                    # local alert 저장용
│   └── reports/
│       └── health_report.md              # health check 결과 저장용
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

### 8-4. Streaming 지연 및 재시작 사례

Producer는 전체 이벤트를 발행했지만, 첫 번째 Bronze 확인 시 일부 이벤트만 적재된 상태가 발생하였다.

```text
expected total = 140561
first bronze count = 131507
```

이후 Spark Structured Streaming을 동일 checkpoint 경로로 다시 실행하여 최종적으로 Bronze count가 140,561건으로 정상화되었다.

이 사례는 Spark Streaming job이 중단되거나 충분히 처리되기 전에 종료된 경우, checkpoint 기반으로 재시작하여 잔여 Kafka offset을 이어서 처리할 수 있음을 보여준다.

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

## 11. 데이터 정합성 처리

중복 이벤트와 late conversion은 운영 장애라기보다는 광고 데이터 파이프라인에서 기본적으로 고려해야 하는 데이터 정합성 요구사항으로 분리하였다.

### 11-1. event_id 기준 중복 제거

Producer 재시도, 수동 재실행, Kafka replay 등으로 동일한 `event_id`가 중복 적재될 수 있다.

본 프로젝트에서는 Bronze는 raw append-only 계층으로 유지하고, Silver에서 `event_id` 기준 deduplication을 수행한다.

```text
Bronze:
- raw event 보존
- kafka_topic, kafka_partition, kafka_offset 저장
- 중복 이벤트도 삭제하지 않음

Silver:
- event_id 기준 deduplication
- producer_ts desc, kafka_offset desc 기준 최신 record 선택

Gold:
- Silver 기준으로 KPI 집계
```

이를 통해 원본 추적 가능성은 유지하면서, 분석 계층에서는 중복으로 인한 KPI 왜곡을 방지한다.

### 11-2. Late Conversion 고려

광고 데이터에서는 impression 또는 click 이후 conversion이 지연되어 들어올 수 있다.

현재 Gold는 `event_date` 기준 campaign summary로 생성하였다. 향후 운영 환경에서는 conversion을 impression_id 또는 click_id와 연결하여 attribution 기준 날짜로 재집계하는 구조를 고려할 수 있다.

```text
현재 구조:
event_date 기준 campaign_summary

개선 구조:
attribution_date 기준 campaign_summary
→ late conversion 발생 시 과거 partition update
```

Iceberg를 적용할 경우 `MERGE INTO`를 이용하여 과거 Gold partition을 업데이트하는 방식으로 확장할 수 있다.

---

## 12. 운영 장애 시나리오 설계

본 프로젝트에서는 장애 시나리오를 데이터 품질 이슈가 아니라, 실제 운영 중 파이프라인이 멈추거나 freshness가 깨지는 상황을 기준으로 재정리하였다.

최종 장애 시나리오는 다음 세 가지이다.

```text
1. Kafka broker 장애 / Producer 발행 실패
2. Spark Streaming job 중단 / checkpoint 기반 복구
3. Silver-Gold batch job 실패 / Gold freshness SLA 위반
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

### 12-1. 시나리오 1: Kafka broker 장애 / Producer 발행 실패

#### 장애 상황

Kafka broker가 내려가거나 topic에 메시지를 발행할 수 없는 상황이다.

실제 운영 환경에서는 다음과 같은 원인으로 발생할 수 있다.

* Kafka broker container 또는 pod down
* Kafka broker는 떠 있지만 topic 응답 지연
* producer timeout
* 네트워크 단절
* Kafka disk full
* topic metadata 조회 실패

#### 영향

Producer가 이벤트를 Kafka에 발행하지 못하면 이후 Spark Streaming, Bronze, Silver, Gold 전체 파이프라인이 지연된다.

즉, downstream 파이프라인은 정상이어도 source ingestion 자체가 멈추는 장애가 된다.

#### 감지 지표

* Kafka container 또는 broker 상태
* topic list 조회 가능 여부
* producer send error count
* producer retry count
* topic별 message 증가 여부
* producer가 마지막으로 성공한 send timestamp

#### 알림 조건

다음 조건 중 하나라도 발생하면 알림 대상으로 본다.

* Kafka broker down
* topic list 조회 실패
* producer send 실패 발생
* 일정 시간 동안 topic message 증가 없음
* producer success count가 기대치보다 작음

알림 예시는 다음과 같다.

```text
[CRITICAL] Kafka broker is not reachable
[ALERT] Producer send failed: topic=ad-clicks, error=timeout
[ALERT] Topic message count is not increasing
```

#### 현재 로컬 구현 범위

현재 로컬 환경은 단일 Kafka broker를 사용한다. 따라서 broker 장애 시 자동 failover가 가능한 HA 구성은 아니다.

로컬에서는 다음 수준까지 구현한다.

* Kafka container 상태 점검
* Kafka topic list 조회 가능 여부 확인
* Producer 실행 시 send error 발생 여부 기록
* 장애 발생 시 AlertSender 인터페이스를 통해 알림 전송

#### 운영 환경 확장 방향

운영 환경에서는 Kafka 자체 HA 구성이 필요하다.

예시 구성은 다음과 같다.

```text
Kafka broker:
- broker 3대 이상
- topic replication.factor = 3
- min.insync.replicas = 2
- producer acks = all
- producer retries 설정
- broker pod anti-affinity 설정
```

Kubernetes 환경에서는 Kafka broker 같은 stateful service를 일반 worker node에 무작위 배치하기보다는, 안정적인 node group에 배치하고 broker pod가 서로 다른 node에 올라가도록 anti-affinity를 설정한다.

broker 1대가 down되더라도 leader election을 통해 다른 replica가 요청을 처리하도록 구성한다. 단, 현재 로컬 MVP에서는 이 HA 구조를 직접 구현하지 않고, 운영 확장 설계로 제시한다.

---

### 12-2. 시나리오 2: Spark Streaming job 중단 / checkpoint 기반 복구

#### 장애 상황

Spark Structured Streaming job이 죽거나 멈춰서 Kafka에는 메시지가 있지만 Bronze로 적재되지 않는 상황이다.

실제 운영 환경에서는 다음과 같은 원인으로 발생할 수 있다.

* Spark driver process 종료
* executor OOM
* Kafka connection timeout
* checkpoint path 오류
* Spark container 또는 pod 재시작
* 리소스 부족으로 streaming 처리 지연

#### 영향

Streaming ingestion이 멈추면 Bronze raw zone이 최신 상태로 갱신되지 않는다. 이 경우 Silver/Gold도 함께 지연되며, 대시보드나 campaign summary가 오래된 데이터를 보여줄 수 있다.

#### 감지 지표

* Spark streaming process 상태
* Bronze latest ingest_time
* checkpoint directory modified time
* expected event count와 Bronze actual count 차이
* Kafka consumer lag
* Bronze partition 최신 날짜 및 시간

#### 알림 조건

다음 조건 중 하나라도 발생하면 알림 대상으로 본다.

* Spark streaming process down
* Bronze latest ingest_time이 일정 시간 이상 갱신되지 않음
* checkpoint가 일정 시간 이상 갱신되지 않음
* Kafka lag 증가
* expected count보다 Bronze count가 부족함

알림 예시는 다음과 같다.

```text
[ALERT] Spark streaming job is not running
[ALERT] Bronze ingestion stale: latest_ingest_time=2026-06-16 00:15:00
[ALERT] Bronze count mismatch: expected=140561, actual=131507
```

#### 현재 로컬 구현 범위

현재 로컬에서는 Airflow나 Kubernetes를 이용한 자동 재시작은 구현하지 않는다.

대신 다음 수준까지 구현한다.

* monitoring script가 Bronze count와 latest ingest_time을 점검
* 이상 발생 시 AlertSender 인터페이스를 통해 알림 전송
* 복구는 동일 checkpoint 경로로 `spark-submit`을 재실행
* 복구 후 Bronze count가 expected count와 일치하는지 확인

실제 실행 과정에서도 Producer는 140,561건을 모두 발행했지만, 첫 번째 Bronze 확인 시 131,507건만 적재된 상태가 발생하였다. 이후 동일 checkpoint 경로로 Spark Streaming을 다시 실행하여 최종적으로 Bronze count가 140,561건으로 정상화되었다.

#### 복구 방식

Spark Structured Streaming은 checkpoint를 사용하여 Kafka offset 처리 상태를 기록한다. 따라서 동일한 checkpoint 경로로 streaming job을 재시작하면 이미 처리한 offset 이후부터 이어서 처리할 수 있다.

로컬 복구 절차는 다음과 같다.

```text
1. Bronze count mismatch 또는 ingest stale 감지
2. AlertSender를 통해 알림 발생
3. 동일 checkpoint 경로로 Spark Streaming job 재실행
4. Bronze count 재확인
5. expected count와 일치하면 resolved 처리
```

#### Airflow 확장 방향

Airflow를 도입한다고 자동으로 HA가 구성되는 것은 아니다. Airflow는 Spark job을 제출하고, 실패한 task를 재시도하는 오케스트레이션 역할을 한다.

운영 환경에서는 다음과 같이 구성할 수 있다.

```text
Airflow DAG:
- SparkSubmitOperator 또는 BashOperator로 spark-submit 실행
- task retry = 3
- retry_delay = 5 minutes
- task 실패 시 webhook alert 전송
- 동일 checkpoint 경로를 사용하여 streaming job 재실행
```

즉, Airflow는 Spark job 자체의 HA를 보장하는 것이 아니라, 실패한 job을 감지하고 다시 제출하는 역할을 담당한다.

#### Kubernetes 확장 방향

Kubernetes 환경에서는 Spark job을 Spark Operator의 `SparkApplication`으로 제출할 수 있다.

예시 설계는 다음과 같다.

```text
Spark on Kubernetes:
- SparkApplication으로 streaming job 제출
- driver restartPolicy = OnFailure
- executor pod 실패 시 재생성
- checkpoint는 pod local storage가 아니라 S3/MinIO 같은 외부 저장소 사용
- driver와 executor가 동일 node에 몰리지 않도록 anti-affinity 설정
```

Karpenter를 사용하는 경우에는 streaming job과 batch job의 리소스 특성에 맞게 node pool을 분리할 수 있다.

예시 설계는 다음과 같다.

```text
Karpenter / node pool:
- streaming job용 node pool 별도 구성
- 최소 2개 node 유지
- driver pod와 executor pod가 같은 node에 몰리지 않도록 anti-affinity 설정
- batch job이 몰릴 때는 Karpenter가 worker node를 추가 provision
- Kafka broker 같은 stateful service는 별도 고정 node group에 배치
```

현재 프로젝트에서는 위 Kubernetes/Karpenter 구성을 실제 구현하지 않고, 로컬 Docker 기반 MVP의 한계와 운영 확장 방향으로 제시한다.

---

### 12-3. 시나리오 3: Silver-Gold batch job 실패 / Gold freshness SLA 위반

#### 장애 상황

Bronze에는 데이터가 정상적으로 적재되었지만, Silver 정제 job 또는 Gold 집계 job이 실패하여 분석 테이블이 최신 상태로 갱신되지 않는 상황이다.

실제 운영 환경에서는 다음과 같은 원인으로 발생할 수 있다.

* `bronze_to_silver.py` 실패
* `gold_campaign_summary.py` 실패
* schema mismatch
* input path 또는 output path 오류
* 메모리 부족
* 이전 output overwrite 실패
* `_SUCCESS` 파일 미생성

#### 영향

Bronze에는 최신 데이터가 존재하지만 Silver 또는 Gold가 갱신되지 않으면 BI 대시보드는 오래된 KPI를 보여준다.

이 경우 ingestion은 성공했지만 analytics layer의 freshness가 깨지는 장애가 된다.

#### 감지 지표

* Silver latest event_date
* Gold latest event_date
* Gold row count
* Gold `_SUCCESS` 파일 존재 여부
* Bronze latest event_time과 Gold latest event_date 차이
* Gold partition 생성 여부
* batch job exit code

#### 알림 조건

다음 조건 중 하나라도 발생하면 알림 대상으로 본다.

* Bronze에는 최신 날짜 데이터가 있는데 Gold에 해당 날짜 partition이 없음
* Gold row count = 0
* Gold `_SUCCESS` 파일이 없음
* Gold 최신 날짜가 Bronze 최신 날짜보다 1일 이상 늦음
* Silver job 성공 없이 Gold job이 실행됨
* batch job exit code가 0이 아님

알림 예시는 다음과 같다.

```text
[ALERT] Gold freshness SLA violated: bronze_latest=2026-07-16, gold_latest=2026-07-14
[ALERT] Gold _SUCCESS file is missing
[ALERT] Gold row count is zero
```

#### 현재 로컬 구현 범위

현재 로컬에서는 Silver와 Gold job을 수동 명령어로 실행한다. 따라서 Airflow 기반 DAG retry는 아직 구현하지 않았다.

로컬에서는 다음 수준까지 구현한다.

* Silver output path 존재 여부 확인
* Gold output path 존재 여부 확인
* Gold `_SUCCESS` 파일 존재 여부 확인
* Gold row count 확인
* Bronze 최신 날짜와 Gold 최신 날짜 비교
* 이상 발생 시 AlertSender 인터페이스로 알림 전송

#### 복구 방식

복구 절차는 다음과 같다.

```text
1. Gold freshness 위반 또는 Gold 생성 실패 감지
2. AlertSender를 통해 알림 발생
3. Silver job 재실행
4. Gold job 재실행
5. Gold row count와 latest event_date 재확인
6. 정상화되면 resolved 처리
```

특정 날짜 partition만 문제가 있는 경우에는 해당 날짜 window만 재처리하는 backfill 구조로 확장할 수 있다.

#### Airflow 확장 방향

운영 환경에서는 Bronze → Silver → Gold 순서를 Airflow DAG로 관리할 수 있다.

예시 설계는 다음과 같다.

```text
Airflow DAG:
1. check_bronze_freshness
2. run_silver_job
3. check_silver_output
4. run_gold_job
5. check_gold_freshness
6. send_alert_if_failed
```

구체적인 정책은 다음과 같다.

```text
- Silver task 실패 시 Gold task 실행 방지
- task retry = 3
- retry_delay = 5 minutes
- 실패 시 webhook alert 전송
- 특정 event_date를 파라미터로 받아 backfill 가능
```

---

## 13. Alerting 설계

장애 시나리오를 실제 운영 관점으로 다루기 위해, 본 프로젝트에서는 특정 로그 파일에만 의존하지 않고 알림 채널을 교체할 수 있는 `AlertSender` 인터페이스를 설계한다.

### 13-1. AlertSender 구조

```text
monitoring/
  ├── check_pipeline_health.py
  ├── alert_sender.py
  ├── alerts/
  │   └── alerts.log
  └── reports/
      └── health_report.md
```

알림 구조는 다음과 같다.

```text
Monitoring Check
        ↓
Alert Rule 판단
        ↓
AlertSender interface
        ↓
FileAlertSender / WebhookAlertSender
```

### 13-2. AlertSender 구현 방향

| 구현체                | 목적                               | 현재 적용 여부 |
| ------------------ | -------------------------------- | -------- |
| FileAlertSender    | 로컬 개발 환경에서 alerts.log에 기록        | 우선 구현    |
| WebhookAlertSender | Slack 또는 Discord webhook으로 알림 전송 | 확장 예정    |
| EmailAlertSender   | 운영 환경에서 Email 알림 전송              | 확장 예정    |

로컬에서는 `FileAlertSender`를 사용하여 `monitoring/alerts/alerts.log`에 알림을 남긴다. 다만 파일 로그는 최종 알림 구조가 아니라, webhook 알림을 붙이기 전 로컬 확인용 구현체로 둔다.

### 13-3. Alert Rule

| 장애 유형             | Alert 조건                            | Alert 예시                                   |
| ----------------- | ----------------------------------- | ------------------------------------------ |
| Kafka broker 장애   | topic list 조회 실패                    | `[CRITICAL] Kafka broker is not reachable` |
| Producer 발행 실패    | send error 발생                       | `[ALERT] Producer send failed`             |
| Streaming 중단      | Bronze ingest_time 갱신 없음            | `[ALERT] Bronze ingestion stale`           |
| Streaming 지연      | expected count > Bronze count       | `[ALERT] Bronze count mismatch`            |
| Gold freshness 위반 | Gold 최신 날짜가 Bronze보다 늦음             | `[ALERT] Gold freshness SLA violated`      |
| Gold 생성 실패        | Gold row count = 0 또는 `_SUCCESS` 없음 | `[ALERT] Gold summary is empty`            |

### 13-4. Log retention 정책

파일 기반 알림 로그는 무한히 쌓이지 않도록 보존 정책을 둔다.

로컬 개발 환경에서는 다음과 같이 관리한다.

```text
- alerts.log는 일 단위로 rotate
- 최근 7일 로그만 보관
- health_report.md는 최신 실행 결과를 덮어쓰기
- 장애 재현 결과는 docs/failure_scenarios.md에 별도 기록
```

운영 환경에서는 다음과 같이 확장할 수 있다.

```text
- CloudWatch / Elasticsearch / Loki 등에 로그 적재
- 원본 알림 이력은 S3 archive에 장기 보관
- Slack/Discord/Email은 실시간 알림 채널로 사용
```

---

## 14. Monitoring 구현 계획

`monitoring/check_pipeline_health.py`는 다음 항목을 점검한다.

| 점검 항목                                  | 목적                    |
| -------------------------------------- | --------------------- |
| Kafka topic 조회 가능 여부                   | Kafka broker 상태 확인    |
| Bronze total count                     | raw 적재 건수 확인          |
| Bronze latest ingest_time              | streaming 지연 여부 확인    |
| Silver output 존재 여부                    | 정제 job 성공 여부 확인       |
| Gold output 존재 여부                      | 집계 job 성공 여부 확인       |
| Gold `_SUCCESS` 존재 여부                  | Gold write 성공 여부 확인   |
| Gold row count                         | Gold table 비어 있음 감지   |
| Bronze latest date vs Gold latest date | Gold freshness SLA 확인 |
| duplicate count                        | 데이터 정합성 참고 지표         |
| late conversion suspicious row         | 광고 도메인 참고 지표          |

현재 구현 대상은 다음과 같다.

```text
1. FileAlertSender 구현
2. Kafka topic check
3. Bronze count check
4. Bronze latest ingest_time check
5. Gold _SUCCESS check
6. Gold freshness check
7. alerts.log 기록
8. health_report.md 생성
```

---

## 15. 실행 방법

### 15-1. Docker 환경 실행

Kafka/Spark Docker compose 폴더에서 실행한다.

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 부캠\kafka_강의자료_메타코드M\kafka-docker-envs\kafka-spark-airflow"

docker compose up -d
docker compose ps
```

---

### 15-2. Kafka topic 생성

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-impressions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-clicks --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-conversions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

---

### 15-3. 이벤트 스트림 생성

final-project 폴더에서 실행한다.

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project"

python code\pipelines\prepare_streaming_sample.py --input data\pcb_dataset_final.tsv --output data\sample_ad_events_100k.csv --max-rows 100000 --base-time "2026-06-16 00:00:00"
```

---

### 15-4. Kafka Producer 실행

```powershell
python code\pipelines\kafka_producer.py --csv data\sample_ad_events_100k.csv --bootstrap-servers localhost:9092 --max-events 999999999 --sleep 0 --log-every 10000
```

---

### 15-5. Kafka 메시지 확인

Kafka/Spark compose 폴더에서 실행한다.

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-impressions --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-clicks --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-conversions --from-beginning --max-messages 3
```

---

### 15-6. Kafka → Bronze 실행

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

### 15-7. Bronze count 확인

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\check_bronze.py" spark-master:/tmp/check_bronze.py

docker compose exec spark-master spark-submit /tmp/check_bronze.py
```

---

### 15-8. Bronze → Silver 실행

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\bronze_to_silver.py" spark-master:/tmp/bronze_to_silver.py

docker compose exec spark-master spark-submit /tmp/bronze_to_silver.py --input /tmp/warehouse/raw/ad_events --output /tmp/warehouse/silver/processed_events
```

Silver 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/silver/processed_events
```

---

### 15-9. Silver → Gold 실행

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\gold_campaign_summary.py" spark-master:/tmp/gold_campaign_summary.py

docker compose exec spark-master spark-submit /tmp/gold_campaign_summary.py --input /tmp/warehouse/silver/processed_events --output /tmp/warehouse/gold/campaign_summary
```

Gold 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/gold/campaign_summary
```

---

### 15-10. Docker 결과를 로컬 프로젝트로 복사

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

## 16. 제출용 캡처 목록

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
| 15 | `15_health_report_verified.png`                    | health report 생성 확인 예정         |

---

## 17. 현재 한계와 확장 방향

현재 구현은 로컬 Docker 기반 Parquet Lakehouse MVP이다. 따라서 운영형 Lakehouse로 확장하기 위해서는 다음 보완이 필요하다.

### 17-1. Iceberg 적용

현재 Bronze, Silver, Gold는 Parquet 파일 기반으로 저장하였다. 향후에는 Silver와 Gold 계층을 Iceberg table로 구성하여 다음 기능을 적용할 수 있다.

* snapshot 관리
* schema evolution
* time travel
* MERGE INTO 기반 late conversion 반영
* rewrite_data_files 기반 compaction
* files/history/snapshots metadata table 조회

### 17-2. Airflow 자동화

현재 각 pipeline은 수동 명령어로 실행하였다. 향후 Airflow DAG를 통해 다음 작업을 자동화할 수 있다.

* Kafka health check
* Bronze freshness check
* Bronze → Silver 정제
* Silver → Gold 집계
* Gold freshness check
* failure retry 및 webhook alert

단, Airflow는 인프라 HA를 자동으로 보장하지 않는다. Airflow는 job 실행 순서 관리, 실패 감지, retry, alert를 담당하는 오케스트레이션 도구로 사용한다.

### 17-3. Kubernetes / Karpenter 확장

운영 환경에서는 Spark job을 Kubernetes 위에서 실행하고, Spark Operator를 통해 driver/executor pod를 관리할 수 있다.

Karpenter를 사용하는 경우에는 다음과 같은 구성이 가능하다.

* streaming job용 node pool 분리
* 최소 2개 node 유지
* driver와 executor anti-affinity 설정
* batch job 증가 시 worker node 자동 provision
* Kafka broker는 별도 안정적인 node group에 배치

현재 프로젝트에서는 위 구성을 실제 구현하지 않고, 로컬 Docker MVP의 한계와 운영 확장 방향으로 제시한다.

### 17-4. Dashboard 연결

Gold campaign_summary는 BI dashboard의 source table로 사용할 수 있다. 향후 Streamlit 또는 QuickSight를 통해 다음 지표를 시각화할 수 있다.

* campaign별 impressions
* clicks
* conversions
* CTR
* CVR
* ROAS
* late conversion count
* duplicate event count
* freshness status

---

## 18. 프로젝트 요약

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

또한 중복 이벤트와 late conversion은 데이터 정합성 요구사항으로 분리하고, 운영 장애는 다음 세 가지로 재정의하였다.

```text
1. Kafka broker 장애 / Producer 발행 실패
2. Spark Streaming job 중단 / checkpoint 기반 복구
3. Silver-Gold batch job 실패 / Gold freshness SLA 위반
```

각 장애 시나리오에 대해 감지 지표, 알림 조건, 복구 방식을 정의하였고, 향후 `AlertSender` 인터페이스를 통해 local file alert와 webhook alert를 모두 지원할 수 있도록 확장할 예정이다.

이를 통해 광고 이벤트 수집, raw 보존, 중복 제거, 캠페인 KPI 집계, 운영 장애 감지 설계까지 이어지는 데이터 엔지니어링 파이프라인의 핵심 흐름을 구현하였다.
