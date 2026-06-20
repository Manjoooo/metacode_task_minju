# Criteo 광고 이벤트 기반 Lakehouse 파이프라인

## 1. 프로젝트 개요

본 프로젝트는 Criteo Attribution Dataset을 활용하여 광고 이벤트 데이터를 수집, 정제, 집계하는 로컬 기반 데이터 파이프라인을 구현한 프로젝트이다.

광고 플랫폼에서는 사용자가 광고를 본 시점, 클릭한 시점, 전환한 시점이 서로 다르게 발생한다. 특히 conversion은 impression 또는 click 이후 지연되어 들어올 수 있고, producer 재전송이나 streaming job 재시작으로 인해 동일 이벤트가 중복 적재될 수도 있다.

본 프로젝트에서는 이러한 광고 데이터 특성을 반영하여 다음과 같은 구조를 구현하였다.

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

현재 구현은 로컬 Docker 환경에서 Kafka, Spark, Airflow 컨테이너를 실행하고, Spark Structured Streaming 및 Spark batch job을 통해 Bronze, Silver, Gold 계층을 생성하는 방식으로 구성하였다.

---

## 2. 프로젝트 목표

본 프로젝트의 목표는 단순히 광고 데이터를 저장하는 것이 아니라, 실제 광고 데이터 파이프라인에서 발생할 수 있는 운영 이슈를 고려한 Lakehouse 구조를 설계하고 구현하는 것이다.

주요 목표는 다음과 같다.

1. Criteo Attribution Dataset을 광고 이벤트 스트림 형태로 변환한다.
2. impression, click, conversion 이벤트를 Kafka topic으로 분리 발행한다.
3. Spark Structured Streaming을 이용하여 Kafka 데이터를 Bronze raw zone에 append-only로 저장한다.
4. Silver 계층에서 event_id 기준 중복 제거를 수행한다.
5. Gold 계층에서 campaign 단위 KPI를 계산한다.
6. producer 재전송, late conversion, small file 증가 등 실제 운영 장애 시나리오에 대응할 수 있는 구조를 설계한다.

---

## 3. 사용 기술

| 구분              | 사용 기술                                                    |
| --------------- | -------------------------------------------------------- |
| 데이터셋            | Criteo Attribution Dataset                               |
| 이벤트 수집          | Kafka                                                    |
| Producer        | Python, kafka-python                                     |
| Streaming 처리    | Spark Structured Streaming                               |
| Batch 처리        | Spark                                                    |
| 저장 포맷           | Parquet                                                  |
| 실행 환경           | Local Docker                                             |
| 오케스트레이션 확장      | Airflow                                                  |
| Lakehouse 확장 방향 | Apache Iceberg, AWS S3, Glue Catalog, Athena, QuickSight |

현재 구현은 로컬 Docker 기반 MVP이며, Iceberg, S3, Glue, Athena, QuickSight는 향후 확장 가능한 구조로 설계하였다.

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
│   └── ad_events_sample.csv
│
├── docs/
│
├── screenshots/
│   ├── 01_kafka_topics_created.png
│   ├── 02_kafka_event_routing_verified.png
│   ├── 03_bronze_raw_parquet_partitioned.png
│   ├── 04_bronze_raw_count_verified.png
│   ├── 05_silver_dedup_verified.png
│   ├── 06_silver_partitioned_output.png
│   ├── 07_gold_campaign_summary_verified.png
│   └── 08_gold_partitioned_output.png
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

최종적으로 생성된 sample event stream은 다음 파일에 저장하였다.

```text
data/sample_ad_events.csv
```

이벤트 생성 결과는 다음과 같다.

```text
Input rows: 1000
Output events: 1395

event_type counts:
impression    1000
click          351
conversion      44
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

### 증빙 캡처

```text
screenshots/01_kafka_topics_created.png
```

---

## 7. Kafka Producer 구현

`kafka_producer.py`는 `sample_ad_events.csv`를 읽고, `event_type`에 따라 적절한 Kafka topic으로 이벤트를 발행한다.

```text
impression  → ad-impressions
click       → ad-clicks
conversion  → ad-conversions
```

실행 명령어:

```powershell
python code\pipelines\kafka_producer.py --csv data\sample_ad_events.csv --bootstrap-servers localhost:9092 --max-events 500 --sleep 0.001
```

검증 결과:

```text
[producer] total_sent=500
[producer] sent_counts:
  impression: 449
  click: 49
  conversion: 2
```

Kafka consumer로 각 topic을 확인하였다.

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-impressions --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-clicks --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-conversions --from-beginning --max-messages 3
```

각 topic에서 JSON 메시지가 정상적으로 조회되었다.

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
│   └── raw_date=2026-06-16/
│       └── raw_hour=00/
├── event_type=conversion/
│   └── raw_date=2026-06-16/
│       └── raw_hour=00/
└── event_type=impression/
    └── raw_date=2026-06-16/
        └── raw_hour=00/
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

Producer를 50개, 500개 두 번 실행했기 때문에 Bronze에는 총 550건이 저장되었다.

```text
=== count by event_type ===
impression = 496
click      = 52
conversion = 2

=== total count ===
550
```

Bronze에서 중복을 제거하지 않았기 때문에 producer 재전송으로 발생한 중복 이벤트가 그대로 보존되었다.

### 증빙 캡처

```text
screenshots/03_bronze_raw_parquet_partitioned.png
screenshots/04_bronze_raw_count_verified.png
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

Bronze에서 확인된 중복 event_id 수는 50개였다.

```text
duplicate_event_id_total = 50
```

중복 제거 후 Silver 결과:

```text
=== Silver count by event_type after dedup ===
conversion = 2
impression = 449
click      = 49

=== Silver total count after dedup ===
500
```

즉, Bronze의 550건 중 producer 재전송으로 발생한 중복 50건이 Silver에서 제거되었다.

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
│   └── event_date=2026-06-16/
│       └── event_hour=00/
├── event_type=conversion/
│   └── event_date=2026-06-16/
│       └── event_hour=00/
└── event_type=impression/
    └── event_date=2026-06-16/
        └── event_hour=00/
```

### 증빙 캡처

```text
screenshots/05_silver_dedup_verified.png
screenshots/06_silver_partitioned_output.png
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
impression = 449
click      = 49
conversion = 2
```

Gold campaign summary 생성 결과:

```text
Gold total campaign-date rows = 196
```

Gold sample:

```text
event_date  campaign_id  impressions  clicks  conversions  ad_cost     revenue_proxy  ctr       cvr   roas
2026-06-16  9100693      5            2       1            0.00183579  0.00461747     0.4       0.5   2.515255
2026-06-16  2869134      3            1       1            0.00011955  0.004         0.333333  1.0   33.458789
```

### 10-3. Gold 저장 구조

Gold는 `event_date` 기준으로 파티셔닝하였다.

```text
warehouse/gold/campaign_summary/
├── _SUCCESS
└── event_date=2026-06-16/
    └── part-00000-....snappy.parquet
```

### 증빙 캡처

```text
screenshots/07_gold_campaign_summary_verified.png
screenshots/08_gold_partitioned_output.png
```

---

## 11. 현재 구현 완료 범위

현재까지 구현 완료된 범위는 다음과 같다.

| 단계                | 구현 여부 | 설명                                                 |
| ----------------- | ----- | -------------------------------------------------- |
| 원본 데이터 보존         | 완료    | Criteo 원본 TSV를 `data/original`에 보존                 |
| 이벤트 스트림 생성        | 완료    | Criteo row를 impression/click/conversion 이벤트로 변환    |
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

---

## 12. 장애 시나리오 설계

본 프로젝트에서는 광고 데이터 파이프라인에서 실제로 발생할 수 있는 장애를 다음과 같이 정의한다.

### 12-1. 시나리오 1: Producer 재전송으로 인한 중복 이벤트 발생

#### 상황

Producer가 네트워크 장애, 재시도, 수동 재실행 등의 이유로 동일한 이벤트를 다시 Kafka에 발행할 수 있다.

본 프로젝트에서는 producer를 50건 발행한 뒤 다시 500건을 발행하여 앞부분 이벤트가 중복되는 상황을 재현하였다.

#### 문제점

* impression, click, conversion 수가 실제보다 크게 계산될 수 있다.
* conversion이 중복되면 revenue가 과대 집계된다.
* CTR, CVR, ROAS 등 핵심 KPI가 왜곡된다.

#### 대응 방식

* Bronze는 raw append-only 계층이므로 중복 이벤트를 삭제하지 않고 보존한다.
* Bronze에는 `kafka_topic`, `kafka_partition`, `kafka_offset`, `ingest_time`을 저장하여 추적 가능성을 확보한다.
* Silver에서 `event_id` 기준으로 deduplication을 수행한다.
* Gold는 Bronze가 아니라 Silver를 기준으로 계산한다.

#### 검증 결과

```text
Bronze total count = 550
duplicate_event_id_total = 50
Silver total count after dedup = 500
```

이 결과를 통해 Bronze는 장애 추적용 raw 계층으로 사용하고, Silver는 분석 기준 테이블로 사용하는 구조를 검증하였다.

---

### 12-2. 시나리오 2: Late Conversion으로 과거 캠페인 KPI가 뒤늦게 변경되는 문제

#### 상황

광고 데이터에서는 impression 또는 click 이후 conversion이 즉시 발생하지 않고, 수 시간에서 수일 뒤에 들어올 수 있다. 이 경우 이미 계산된 과거 날짜의 campaign summary가 뒤늦게 변경되어야 한다.

#### 문제점

* 과거 캠페인의 conversion 수가 뒤늦게 증가한다.
* revenue, CVR, ROAS가 과거 날짜 기준으로 수정되어야 한다.
* 단순 append 집계만 사용하면 summary table이 최신 상태를 반영하지 못한다.

#### 대응 방향

* Silver에서 impression_id, click_id, conversion_id를 연결하여 conversion delay를 계산한다.
* conversion event_time과 impression event_time의 차이를 기준으로 late conversion 여부를 판단한다.
* Gold는 최근 N일 window를 재집계하거나 Iceberg MERGE INTO 방식으로 campaign_summary를 갱신한다.

#### 향후 구현 방향

```text
Silver processed_events
        ↓
conversion event와 impression event 연결
        ↓
conversion_delay_sec 계산
        ↓
is_late_conversion flag 생성
        ↓
Gold campaign_summary 재계산 또는 MERGE
```

---

### 12-3. 시나리오 3: Streaming micro-batch로 인한 Small File 증가

#### 상황

Spark Structured Streaming은 micro-batch 단위로 Parquet 파일을 생성한다. 이벤트가 작거나 micro-batch 주기가 짧으면 작은 파일이 많이 생성될 수 있다.

#### 문제점

* 파일 수가 많아져 query planning 비용이 증가한다.
* 평균 파일 크기가 작아져 scan 효율이 떨어진다.
* BI dashboard 조회 속도가 느려질 수 있다.
* Iceberg 적용 시 manifest 및 metadata 관리 비용이 증가한다.

#### 대응 방향

* file count와 평균 file size를 주기적으로 모니터링한다.
* Iceberg 적용 시 `rewrite_data_files`를 이용해 compaction을 수행한다.
* streaming이 쓰는 최신 partition과 충돌하지 않도록 D-1 또는 D-2 partition을 대상으로 compaction한다.
* Airflow DAG로 compaction job을 자동화하고, 실패 시 retry 정책을 적용한다.

---

### 12-4. 시나리오 4: 정제 로직 오류로 인한 Backfill 필요

#### 상황

이벤트 정제 로직, campaign_id 매핑, conversion attribution 로직에 오류가 뒤늦게 발견될 수 있다.

#### 문제점

* 이미 생성된 Silver processed_events가 잘못될 수 있다.
* Gold campaign_summary의 KPI가 잘못 계산될 수 있다.
* dashboard가 잘못된 수치를 보여줄 수 있다.

#### 대응 방향

* Bronze raw zone은 append-only로 보존하므로 특정 날짜 window를 다시 읽어 재처리할 수 있다.
* Silver는 event_id 기준 dedup을 다시 수행한다.
* Gold는 해당 기간의 campaign summary를 overwrite 또는 MERGE 방식으로 재계산한다.

---

## 13. 실행 방법

### 13-1. Docker 환경 실행

Kafka/Spark/Airflow Docker compose 폴더에서 실행한다.

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 부캠\kafka_강의자료_메타코드M\kafka-docker-envs\kafka-spark-airflow"

docker compose up -d
docker compose ps
```

---

### 13-2. Kafka topic 생성

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-impressions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-clicks --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ad-conversions --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

---

### 13-3. 이벤트 스트림 생성

final-project 폴더에서 실행한다.

```powershell
cd "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project"

python code\pipelines\prepare_streaming_sample.py --input data\pcb_dataset_final.tsv --output data\sample_ad_events.csv --max-rows 1000 --base-time "2026-06-16 00:00:00"
```

---

### 13-4. Kafka Producer 실행

```powershell
python code\pipelines\kafka_producer.py --csv data\sample_ad_events.csv --bootstrap-servers localhost:9092 --max-events 500 --sleep 0.001
```

---

### 13-5. Kafka 메시지 확인

Kafka/Spark compose 폴더에서 실행한다.

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-impressions --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-clicks --from-beginning --max-messages 3

docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ad-conversions --from-beginning --max-messages 2
```

---

### 13-6. Kafka → Bronze 실행

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

### 13-7. Bronze count 확인

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\check_bronze.py" spark-master:/tmp/check_bronze.py

docker compose exec spark-master spark-submit /tmp/check_bronze.py
```

---

### 13-8. Bronze → Silver 실행

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\bronze_to_silver.py" spark-master:/tmp/bronze_to_silver.py

docker compose exec spark-master spark-submit /tmp/bronze_to_silver.py --input /tmp/warehouse/raw/ad_events --output /tmp/warehouse/silver/processed_events
```

Silver 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/silver/processed_events
```

---

### 13-9. Silver → Gold 실행

```powershell
docker cp "C:\Users\wendy\OneDrive\Desktop\엔지니어 프로젝트_김민주\final-project\code\pipelines\gold_campaign_summary.py" spark-master:/tmp/gold_campaign_summary.py

docker compose exec spark-master spark-submit /tmp/gold_campaign_summary.py --input /tmp/warehouse/silver/processed_events --output /tmp/warehouse/gold/campaign_summary
```

Gold 파일 확인:

```powershell
docker compose exec spark-master ls -R /tmp/warehouse/gold/campaign_summary
```

---

### 13-10. Docker 결과를 로컬 프로젝트로 복사

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

## 14. 제출용 캡처 목록

| 번호 | 파일명                                     | 설명                           |
| -- | --------------------------------------- | ---------------------------- |
| 01 | `01_kafka_topics_created.png`           | Kafka topic 3개 생성 확인         |
| 02 | `02_kafka_event_routing_verified.png`   | event_type별 topic 라우팅 확인     |
| 03 | `03_bronze_raw_parquet_partitioned.png` | Bronze raw Parquet 파티션 생성 확인 |
| 04 | `04_bronze_raw_count_verified.png`      | Bronze event_type별 count 확인  |
| 05 | `05_silver_dedup_verified.png`          | Silver dedup 결과 확인           |
| 06 | `06_silver_partitioned_output.png`      | Silver 파티션 생성 확인             |
| 07 | `07_gold_campaign_summary_verified.png` | Gold campaign summary KPI 확인 |
| 08 | `08_gold_partitioned_output.png`        | Gold 파티션 생성 확인               |

---

## 15. 현재 한계와 확장 방향

현재 구현은 로컬 Docker 기반 Parquet Lakehouse MVP이다. 따라서 운영형 Lakehouse로 확장하기 위해서는 다음 보완이 필요하다.

### 15-1. Iceberg 적용

현재 Bronze, Silver, Gold는 Parquet 파일 기반으로 저장하였다. 향후에는 Silver와 Gold 계층을 Iceberg table로 구성하여 다음 기능을 적용할 수 있다.

* snapshot 관리
* schema evolution
* time travel
* MERGE INTO 기반 late conversion 반영
* rewrite_data_files 기반 compaction
* files/history/snapshots metadata table 조회

### 15-2. Late Conversion 보정

현재 Gold는 Silver 기준 batch 집계로 생성하였다. 향후 late conversion 이벤트가 뒤늦게 들어오는 경우, 최근 N일 window를 재계산하거나 Iceberg MERGE INTO로 기존 campaign_summary를 갱신하도록 확장할 수 있다.

### 15-3. Airflow 자동화

현재 각 pipeline은 수동 명령어로 실행하였다. 향후 Airflow DAG를 통해 다음 작업을 자동화할 수 있다.

* Kafka ingestion 상태 확인
* Bronze → Silver 정제
* Silver → Gold 집계
* compaction job 실행
* backfill job 실행
* failure retry 및 alert

### 15-4. Dashboard 연결

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

---

## 16. 프로젝트 요약

본 프로젝트는 Criteo 광고 데이터를 기반으로 이벤트 스트림을 생성하고, Kafka와 Spark를 이용하여 Bronze, Silver, Gold 계층으로 이어지는 광고 데이터 파이프라인을 구현하였다.

특히 producer 재전송으로 인한 duplicate event를 실제로 재현하고, Bronze에서는 raw를 보존하되 Silver에서 event_id 기준으로 중복을 제거하여 Gold KPI 왜곡을 방지하는 구조를 검증하였다.

현재 구현 결과는 다음과 같다.

```text
Kafka topic:
- ad-impressions
- ad-clicks
- ad-conversions

Bronze:
- total count = 550
- impression = 496
- click = 52
- conversion = 2

Silver:
- duplicate_event_id_total = 50
- total count after dedup = 500
- impression = 449
- click = 49
- conversion = 2

Gold:
- campaign-date rows = 196
- metrics = impressions, clicks, conversions, ad_cost, revenue_proxy, CTR, CVR, ROAS
```

이를 통해 광고 이벤트 수집, raw 보존, 중복 제거, 캠페인 KPI 집계까지 이어지는 데이터 엔지니어링 파이프라인의 핵심 흐름을 구현하였다.
