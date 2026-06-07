# 최종 프로젝트 설계문서 - 김민주

# Criteo 광고 이벤트 기반 Lakehouse 운영 파이프라인

## 0. 프로젝트 목적

본 프로젝트는 Criteo 광고 이벤트 데이터를 활용하여 광고 로그 수집부터 정제, 집계, 운영 모니터링까지 포함하는 Lakehouse 기반 데이터 파이프라인을 설계하고 구현하는 것을 목표로 한다.

실제 서비스에서 실시간 광고 이벤트를 직접 받을 수 없기 때문에, 본 프로젝트에서는 CSV 형태의 광고 이벤트 데이터를 Kafka Producer가 row 단위로 발행하여 실시간 스트리밍 데이터처럼 모사한다. 이후 Spark Structured Streaming이 Kafka topic을 구독하여 raw 데이터를 저장하고, 이후 단계에서 Iceberg 기반 Silver / Gold 테이블로 확장한다.

본 프로젝트의 핵심은 단순히 데이터를 적재하는 것이 아니라, 광고 도메인에서 실제로 발생할 수 있는 운영 문제를 Lakehouse 구조로 해결하는 것이다. 특히 late conversion, 중복 이벤트 적재, streaming ingestion으로 인한 small file 문제를 주요 장애 및 운영 시나리오로 정의하고, 이를 Iceberg의 MERGE, snapshot, metadata table, compaction 기능을 활용해 해결하는 구조를 설계한다.

---

## 1. 가정 환경

### 1-1. 도메인

본 프로젝트의 도메인은 광고 플랫폼 이벤트 로그 처리이다.

광고 플랫폼에서는 사용자가 광고를 보는 `impression`, 광고를 클릭하는 `click`, 실제 구매나 가입으로 이어지는 `conversion` 이벤트가 발생한다. 광고 데이터의 특징은 conversion이 click 이후 늦게 들어올 수 있다는 점이다. 따라서 단순히 데이터를 append하는 구조가 아니라, 지연 전환 반영, 중복 이벤트 제거, 캠페인 단위 집계 갱신, 운영 상태 모니터링이 가능한 구조가 필요하다.

본 프로젝트에서는 광고 데이터인 Criteo Attribution Dataset을 사용한다.

---

## 2. 데이터 규모 및 트래픽 패턴

### 2-1. 현재 규모

현재 광고 플랫폼은 다음과 같은 작은 규모의 서비스를 가정한다.

| 항목           | 가정         |
| ------------ | ---------- |
| 일 이벤트 수      | 1,000,000건 |
| 시간당 평균 이벤트 수 | 약 40,000건  |
| 초당 평균 이벤트 수  | 약 12건      |
| 평균 이벤트 크기    | 약 1KB      |
| 일 raw 데이터 크기 | 약 1GB      |
| 월 raw 데이터 크기 | 약 30GB     |
| 연 raw 데이터 크기 | 약 365GB    |

### 2-2. 트래픽 패턴

광고 도메인의 특성을 반영하여 다음과 같은 트래픽 패턴을 가정한다.

| 구분        | 패턴                        |
| --------- | ------------------------- |
| 평일 피크 시간  | 오전 8시 ~ 12시               |
| 피크 시간 트래픽 | 평소 대비 2~3배                |
| 주말 트래픽    | 평일 평균 대비 1.5배             |
| 캠페인 프로모션  | 특정 campaign_id에 이벤트 집중 가능 |

### 2-3. 성장 시나리오

| 시점     | 이벤트 규모       | 예상 raw 데이터  |
| ------ | ------------ | ----------- |
| 현재     | 일 100만 이벤트   | 약 1GB/day   |
| 6개월 후  | 일 1,000만 이벤트 | 약 10GB/day  |
| 1~2년 후 | 일 1억 이벤트     | 약 100GB/day |

본 프로젝트는 현재 규모에서는 로컬 Docker 또는 AWS 실습 환경에서 구현하되, 장기적으로 일 1억 이벤트까지 증가했을 때의 스케일 아웃 전략을 함께 설계한다.

---

## 3. 기술 스택

| 영역       | 기술                         |
| -------- | -------------------------- |
| 데이터 소스   | Criteo 광고 이벤트 데이터          |
| 이벤트 수집   | Kafka                      |
| 스트리밍 처리  | Spark Structured Streaming |
| 저장소      | AWS S3                     |
| 테이블 포맷   | Apache Iceberg             |
| 메타데이터 관리 | AWS Glue Catalog           |
| 쿼리 엔진    | Athena, Spark SQL          |
| BI 대시보드  | QuickSight                 |
| 오케스트레이션  | Airflow                    |
| 협업 및 제출  | GitHub Repository          |

---

## 4. 전체 파이프라인 구조

본 프로젝트의 전체 파이프라인은 다음과 같이 설계한다.

```text
Criteo CSV / Sample Ad Events
        ↓
Kafka Producer
        ↓
Kafka Topic: ad-events
        ↓
Spark Structured Streaming
        ↓
Bronze: raw/ad-events append-only zone
        ↓
Spark Batch / Incremental Processing
        ↓
Silver: processed_events Iceberg Table
        ↓
Spark Aggregation + Iceberg MERGE
        ↓
Gold: campaign_summary Iceberg Table
        ↓
Athena / QuickSight Dashboard
```

본 파이프라인은 단순히 Kafka 이벤트를 저장하는 구조가 아니라, 광고 데이터에서 실제로 발생할 수 있는 운영 문제를 처리하기 위한 구조로 설계한다.

* Bronze 계층은 원본 이벤트와 Kafka metadata를 보존하여 장애 추적과 재처리가 가능하도록 한다.
* Silver 계층은 event_id 기준 중복 제거, event_time/ingest_time 정제, late conversion 여부 계산을 수행한다.
* Gold 계층은 campaign_id와 event_date 기준 summary를 생성하며, late conversion으로 인해 과거 KPI가 변경될 수 있으므로 Iceberg `MERGE INTO`를 사용해 갱신한다.
* Iceberg metadata table을 이용해 snapshot freshness, file count, average file size, duplicate count, late conversion count를 모니터링한다.
* Airflow를 이용해 compaction job을 자동화하여 streaming ingestion으로 발생하는 small file 문제를 관리한다.

### 4-1. 장애 시나리오와 파이프라인 연결

본 프로젝트의 장애 시나리오는 각 계층에서 다음과 같이 처리된다.

```text
[Source]
Criteo CSV
  → Kafka Producer

[Ingestion]
Kafka Topic: ad-events
  → Spark Structured Streaming
  → checkpointLocation 설정
  → topic / partition / offset / ingest_time 포함 저장

[Bronze]
raw_ad_events
  → append-only
  → 중복 삭제 안 함
  → 장애 추적 / 재처리 source

[Silver]
processed_events
  → timestamp 변환
  → event_type 검증
  → event_id deduplication
  → late_conversion 계산
  → 잘못된 user_id / campaign_id 제거

[Gold]
campaign_summary
  → event_date + campaign_id 기준 집계
  → 최근 7일 또는 14일 window 재집계
  → Iceberg MERGE INTO로 summary 갱신

[Operation]
Iceberg metadata tables
  → snapshots
  → files
  → history
  → duplicate count
  → late conversion count
  → file count / avg file size

[Maintenance]
Airflow
  → D-1 또는 D-2 partition compaction
  → retry policy
  → compaction 실패해도 ingestion은 계속 진행
```

| 장애 시나리오         | 해결 위치                      | 구현 포인트                                                     |
| --------------- | -------------------------- | ---------------------------------------------------------- |
| Late Conversion | Silver + Gold              | Silver에서 late flag 계산, Gold에서 7일 또는 14일 window 재집계 후 MERGE |
| 중복 이벤트          | Bronze + Silver            | Bronze는 원본 보존, Silver에서 event_id 기준 deduplication          |
| Small File      | Iceberg Metadata + Airflow | files metadata 확인, rewrite_data_files 자동화                  |
| Backfill        | Bronze → Silver → Gold     | raw에서 다시 읽고 Silver/Gold 재계산                                |

3차시에서는 위 구조 중 다음 구간을 우선 구현한다.

```text
Criteo CSV / Sample Ad Events
        ↓
Kafka Producer
        ↓
Kafka Topic: ad-events
        ↓
Spark Structured Streaming
        ↓
Bronze Raw Zone
```

Silver, Gold, Airflow 자동화, QuickSight 대시보드는 이후 차시에서 순차적으로 구현한다.

---

## 5. 메달리온 아키텍처 설계

### 5-1. Bronze Layer: Raw Zone

Bronze 계층은 Kafka에서 수집한 광고 이벤트를 원본에 가깝게 저장하는 계층이다.

본 프로젝트에서는 Bronze 계층을 S3 또는 로컬 파일 기반 append-only raw zone으로 설계한다. raw 계층의 핵심 목적은 원본 보존, 장애 추적, 재처리이므로 정제나 갱신보다 append-only 저장을 우선한다.

Bronze 계층의 역할은 다음과 같다.

* Kafka에서 들어온 이벤트를 원본에 가깝게 저장
* raw_date, raw_hour 기준 파티셔닝
* Kafka topic, partition, offset 정보 보존
* ingest_time 저장
* downstream 정제 로직 오류 발생 시 재처리 source로 활용
* 중복 이벤트가 발생해도 원본 추적을 위해 즉시 삭제하지 않음

Bronze 계층은 깨끗한 분석 테이블이 아니라, 장애 추적과 재처리를 위한 원본 보존 계층이다. 따라서 중복 이벤트가 들어오더라도 이 단계에서 삭제하지 않고, Silver 계층에서 정제 및 중복 제거를 수행한다.

### 5-2. Silver Layer: processed_events

Silver 계층은 Bronze raw 데이터를 분석 가능한 형태로 정제한 Iceberg 테이블이다.

Silver 계층에서 수행할 처리는 다음과 같다.

* event_time, ingest_time timestamp 변환
* event_type 값 검증
* event_id 기준 중복 제거
* 잘못된 campaign_id 또는 user_id 제거
* late conversion 여부 계산
* downstream summary 계산을 위한 표준 스키마 제공

Silver 계층부터 Iceberg를 적용하는 이유는 정제 로직 변경, 지연 이벤트 반영, 중복 제거 결과 관리가 필요하기 때문이다.

Silver `processed_events`는 downstream에서 사용하는 기준 테이블이므로, Gold summary는 Bronze가 아니라 Silver를 기준으로 계산한다.

### 5-3. Gold Layer: campaign_summary

Gold 계층은 BI 대시보드와 비즈니스 KPI 조회를 위한 집계 테이블이다.

Gold 테이블은 `event_date`, `campaign_id` 기준으로 다음 지표를 저장한다.

| 지표                   | 설명                   |
| -------------------- | -------------------- |
| impressions          | 광고 노출 수              |
| clicks               | 광고 클릭 수              |
| conversions          | 전환 수                 |
| revenue              | 전환 금액 합계             |
| CTR                  | clicks / impressions |
| CVR                  | conversions / clicks |
| avg_conversion_delay | 평균 전환 지연 시간          |
| updated_at           | summary 갱신 시각        |

Gold 계층은 지연 conversion이 들어왔을 때 과거 날짜의 summary가 변경될 수 있으므로, 단순 append가 아니라 Iceberg `MERGE INTO`를 사용하여 갱신한다.

Gold summary의 key는 다음과 같이 정의한다.

```text
event_date + campaign_id
```

동일한 날짜와 캠페인에 대한 summary가 여러 번 계산되더라도, Iceberg `MERGE INTO`를 사용해 기존 row를 update하거나 새로운 row를 insert한다. 이를 통해 backfill이나 반복 재집계 상황에서도 중복 summary row가 생성되지 않도록 한다.

---

## 6. 이 프로젝트에서 Iceberg가 필요한 이유

본 프로젝트에서 Iceberg를 사용하는 이유는 단순히 데이터를 저장하기 위해서가 아니라, 광고 데이터의 운영 특성을 처리하기 위해서이다.

### 6-1. Late Conversion 반영

광고 데이터에서는 사용자가 광고를 클릭한 시점과 실제 conversion이 발생하는 시점 사이에 시간 차이가 존재한다. 이미 집계된 과거 날짜에 conversion이 늦게 들어오면 기존 summary를 수정해야 한다.

Iceberg는 `MERGE INTO`를 통해 기존 summary row를 안정적으로 갱신할 수 있으므로, late conversion 반영에 적합하다.

### 6-2. Snapshot 기반 추적

Iceberg는 테이블 변경 이력을 snapshot 단위로 관리한다. 이를 통해 운영자는 특정 시점에 어떤 commit이 발생했는지, 최근 데이터 적재가 정상적으로 이루어졌는지 확인할 수 있다.

예를 들어 Gold summary가 특정 시점 이후 갑자기 감소하거나 증가한 경우, Iceberg snapshot history를 통해 어느 시점에 어떤 commit이 발생했는지 확인할 수 있다.

### 6-3. Metadata Table 기반 운영 모니터링

Iceberg의 `snapshots`, `files`, `history`, `manifests` metadata table을 활용하면 운영자는 테이블의 최신성, 파일 수, 평균 파일 크기, snapshot history를 확인할 수 있다.

본 프로젝트에서는 이러한 metadata table을 운영 헬스체크 쿼리와 QuickSight Operation Metrics 탭에 활용한다.

### 6-4. Small File 관리

Streaming ingestion은 micro-batch 단위로 작은 파일을 계속 생성할 수 있다. Iceberg의 `rewrite_data_files`를 활용하면 small file을 병합하여 Athena 조회 성능과 파일 관리 효율을 개선할 수 있다.

본 프로젝트에서는 Airflow DAG를 이용해 compaction job을 자동화하고, 최신 streaming partition과 충돌하지 않도록 D-1 또는 D-2 이전 partition을 대상으로 실행한다.

---

## 7. 장애 및 운영 시나리오

본 프로젝트에서는 선생님 예시를 그대로 사용하는 대신, 광고 도메인에서 발생할 수 있는 운영 문제를 중심으로 다음 세 가지 장애 시나리오를 정의한다.

---

### 7-1. 시나리오 1: Late Conversion으로 과거 KPI가 변경되는 문제

#### 상황

사용자가 6월 1일에 광고를 클릭했지만, 실제 conversion 이벤트는 6월 3일에 도착할 수 있다. 이 경우 이미 생성된 6월 1일 캠페인 summary의 conversion 수와 revenue가 실제보다 낮게 기록된다.

#### 문제점

* 과거 날짜의 CVR, revenue가 실제보다 낮게 보임
* 광고 예산 최적화 판단이 왜곡될 수 있음
* 단순 append summary 구조에서는 과거 KPI 수정이 어려움

#### 대응 방향

* Bronze raw zone에 모든 이벤트를 원본 그대로 보존한다.
* Silver `processed_events`에서 `event_time`, `ingest_time`을 기준으로 late conversion 여부를 계산한다.
* Gold `campaign_summary`는 최근 7일 또는 14일 window를 반복 재집계한다.
* Iceberg `MERGE INTO`를 사용해 `event_date + campaign_id` 기준 기존 summary row를 갱신한다.
* QuickSight 운영 탭에 late conversion count와 average conversion delay를 표시한다.

#### 사용할 기술

| 문제            | 사용할 기술                               |
| ------------- | ------------------------------------ |
| 지연 이벤트 반영     | Spark batch / incremental processing |
| 과거 summary 갱신 | Iceberg MERGE INTO                   |
| 변경 이력 추적      | Iceberg snapshots                    |
| 운영 모니터링       | QuickSight, Athena health query      |

#### 구현 예시

Gold summary를 생성할 때 매번 전체 기간을 재집계하지 않고, 최근 7일 또는 14일 window만 다시 계산한다.

```sql
MERGE INTO glue_catalog.ad_lakehouse.campaign_summary AS target
USING recalculated_summary AS source
ON target.event_date = source.event_date
AND target.campaign_id = source.campaign_id
WHEN MATCHED THEN UPDATE SET
  impressions = source.impressions,
  clicks = source.clicks,
  conversions = source.conversions,
  revenue = source.revenue,
  ctr = source.ctr,
  cvr = source.cvr,
  avg_conversion_delay = source.avg_conversion_delay,
  updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT *
;
```

---

### 7-2. 시나리오 2: Streaming Job 재시작 또는 Producer 재전송으로 인한 중복 이벤트 적재 문제

#### 상황

Kafka Producer가 중간에 실패한 뒤 같은 CSV 파일을 다시 전송하거나, Spark Structured Streaming job이 장애 후 재시작되는 과정에서 동일한 `event_id`를 가진 이벤트가 raw zone에 중복 저장될 수 있다.

Spark Structured Streaming은 `checkpointLocation`을 통해 Kafka offset과 처리 상태를 복구할 수 있지만, 수동 재처리, producer 재전송, checkpoint 초기화 상황에서는 동일 이벤트가 다시 적재될 가능성이 있다.

#### 문제점

* impression, click, conversion 수가 실제보다 크게 계산됨
* conversion 중복 시 revenue가 중복 집계됨
* CTR, CVR, revenue 등 핵심 KPI가 왜곡됨
* downstream summary table의 신뢰도가 낮아짐

#### 대응 방향

* Bronze raw zone은 원본 보존 목적이므로 중복 이벤트도 삭제하지 않고 저장한다.
* Bronze에는 Kafka `topic`, `partition`, `offset`, `ingest_time`을 함께 저장해 중복 발생 원인을 추적할 수 있게 한다.
* Spark Structured Streaming job에는 `checkpointLocation`을 설정하여 장애 후 offset 복구가 가능하도록 한다.
* Silver `processed_events` 생성 시 `event_id` 기준 deduplication을 수행한다.
* 동일 `event_id`가 여러 개 존재하면 `ingest_time` 기준 최초 또는 최신 record 하나만 선택한다.
* Gold `campaign_summary`는 Bronze가 아니라 deduplication이 끝난 Silver 테이블만 기준으로 계산한다.
* 운영 헬스체크 쿼리로 duplicate event count를 매일 확인한다.

#### 사용할 기술

| 문제       | 사용할 기술                                        |
| -------- | --------------------------------------------- |
| 장애 복구    | Spark Structured Streaming checkpointLocation |
| 중복 원본 추적 | Bronze raw append-only zone                   |
| Kafka 추적 | topic, partition, offset metadata 보존          |
| 중복 제거    | Spark window function / row_number            |
| 멱등성 확보   | event_id unique key                           |
| 운영 확인    | Athena health query, QuickSight operation tab |

#### 구현 예시

Silver 정제 단계에서 `event_id` 기준으로 중복을 제거한다.

```sql
WITH ranked_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY ingest_time ASC
    ) AS rn
  FROM bronze_raw_events
)
SELECT *
FROM ranked_events
WHERE rn = 1;
```

이 방식은 Bronze에 중복 원본을 남겨 장애 원인을 추적하면서도, downstream summary 계산에서는 중복을 제거해 KPI 왜곡을 막기 위한 설계이다.

---

### 7-3. 시나리오 3: 캠페인 트래픽 폭증으로 small file이 증가하는 문제

#### 상황

주말 프로모션이나 특정 대형 캠페인 집행으로 인해 짧은 시간 동안 이벤트가 급격히 증가할 수 있다. Spark Structured Streaming은 micro-batch 단위로 데이터를 저장하기 때문에 작은 parquet file이 많이 생성될 수 있다.

#### 문제점

* Iceberg table의 data file 수가 과도하게 증가함
* Athena query planning 시간이 증가함
* 평균 file size가 작아져 scan 효율이 떨어짐
* dashboard 조회 속도가 느려짐
* manifest와 metadata 관리 비용이 증가함

#### 대응 방향

* Iceberg `files` metadata table을 이용해 file count와 average file size를 모니터링한다.
* file count가 기준치를 넘거나 평균 file size가 지나치게 작으면 compaction을 수행한다.
* Airflow DAG를 이용해 매일 새벽 compaction job을 실행한다.
* 최신 streaming partition과 충돌을 줄이기 위해 당일 partition이 아니라 D-1 또는 D-2 이전 partition을 대상으로 compaction한다.
* Iceberg commit conflict가 발생하면 Airflow retry 정책에 따라 다음 실행 주기에서 재시도한다.
* QuickSight 운영 탭에 file count와 avg file size를 표시한다.

#### 사용할 기술

| 문제            | 사용할 기술                             |
| ------------- | ---------------------------------- |
| 파일 수 모니터링     | Iceberg files metadata table       |
| small file 병합 | Iceberg rewrite_data_files         |
| 충돌 회피         | D-1 또는 D-2 partition 대상 compaction |
| 자동화           | Airflow DAG, retry policy          |
| 운영 대시보드       | QuickSight operation tab           |

#### 구현 예시

Iceberg metadata table을 이용해 file count와 평균 file size를 확인한다.

```sql
SELECT
  COUNT(*) AS file_count,
  AVG(file_size_in_bytes) AS avg_file_size
FROM glue_catalog.ad_lakehouse.processed_events.files;
```

compaction은 Airflow DAG에서 다음 SQL을 실행하는 방식으로 설계한다.

```sql
CALL glue_catalog.system.rewrite_data_files(
  table => 'ad_lakehouse.processed_events'
);
```

다만 streaming ingestion이 진행 중인 최신 partition과 충돌하지 않도록, 실제 운영에서는 D-1 또는 D-2 이전 partition만 대상으로 compaction을 수행한다.

---

### 7-4. 추가 고려 시나리오: 정제 로직 오류로 인한 Backfill 필요

#### 상황

이벤트 정제 로직이나 campaign_id 매핑 로직에 오류가 뒤늦게 발견될 수 있다. 이 경우 이미 생성된 Silver `processed_events`와 Gold `campaign_summary`를 특정 기간 기준으로 다시 계산해야 한다.

#### 대응 방향

* Bronze raw zone은 append-only로 보존하므로 특정 날짜 window를 다시 읽어 재처리할 수 있다.
* Silver는 `event_id` 기준 deduplication을 다시 수행한다.
* Gold는 `event_date + campaign_id` 기준 Iceberg `MERGE INTO`로 갱신한다.
* 동일한 backfill job을 여러 번 실행해도 같은 key의 summary row가 중복 생성되지 않도록 멱등성을 유지한다.

#### 사용할 기술

| 문제          | 사용할 기술                      |
| ----------- | --------------------------- |
| 원본 재처리      | Bronze raw append-only zone |
| 정제 재수행      | Spark batch processing      |
| 중복 방지       | event_id deduplication      |
| summary 재계산 | Iceberg MERGE INTO          |
| 이력 추적       | Iceberg snapshot history    |

---

## 8. 운영 헬스체크 설계

운영자가 매일 5분 안에 파이프라인 상태를 확인할 수 있도록 다음 헬스체크 쿼리를 설계한다.

| 번호 | 헬스체크 항목                          | 목적                                      |
| -- | -------------------------------- | --------------------------------------- |
| 1  | latest snapshot time             | 최근 테이블 갱신 여부 확인                         |
| 2  | daily row count                  | 일자별 데이터 누락 또는 급증 확인                     |
| 3  | file count                       | small file 증가 여부 확인                     |
| 4  | average file size                | compaction 필요 여부 확인                     |
| 5  | duplicate event count            | 중복 이벤트 발생 여부 확인                         |
| 6  | late conversion count            | 지연 전환 발생 패턴 확인                          |
| 7  | event_type distribution          | impression/click/conversion 비율 이상 여부 확인 |
| 8  | summary update time              | Gold summary 최신성 확인                     |
| 9  | processed vs summary consistency | Silver와 Gold 집계 정합성 확인                  |

### 8-1. latest snapshot time

```sql
SELECT
  committed_at,
  snapshot_id,
  operation
FROM glue_catalog.ad_lakehouse.processed_events.snapshots
ORDER BY committed_at DESC
LIMIT 1;
```

### 8-2. daily row count

```sql
SELECT
  event_date,
  COUNT(*) AS row_count
FROM glue_catalog.ad_lakehouse.processed_events
GROUP BY event_date
ORDER BY event_date DESC;
```

### 8-3. file count와 average file size

```sql
SELECT
  COUNT(*) AS file_count,
  AVG(file_size_in_bytes) AS avg_file_size
FROM glue_catalog.ad_lakehouse.processed_events.files;
```

### 8-4. duplicate event count

```sql
SELECT
  COUNT(*) AS duplicate_event_count
FROM (
  SELECT
    event_id,
    COUNT(*) AS cnt
  FROM glue_catalog.ad_lakehouse.raw_ad_events
  GROUP BY event_id
  HAVING COUNT(*) > 1
);
```

### 8-5. late conversion count

```sql
SELECT
  event_date,
  COUNT(*) AS late_conversion_count
FROM glue_catalog.ad_lakehouse.processed_events
WHERE event_type = 'conversion'
  AND is_late_conversion = true
GROUP BY event_date
ORDER BY event_date DESC;
```

### 8-6. processed vs summary consistency

```sql
SELECT
  s.event_date,
  s.campaign_id,
  s.conversions AS summary_conversions,
  p.conversions AS processed_conversions
FROM glue_catalog.ad_lakehouse.campaign_summary s
LEFT JOIN (
  SELECT
    event_date,
    campaign_id,
    COUNT(*) AS conversions
  FROM glue_catalog.ad_lakehouse.processed_events
  WHERE event_type = 'conversion'
  GROUP BY event_date, campaign_id
) p
ON s.event_date = p.event_date
AND s.campaign_id = p.campaign_id
WHERE s.conversions <> p.conversions;
```

---

## 9. 대시보드 설계

QuickSight 대시보드는 두 개의 탭으로 구성한다.

### 9-1. Business KPI 탭

비즈니스 KPI 탭에서는 광고 성과를 확인한다.

포함 지표는 다음과 같다.

* 일자별 impressions
* 일자별 clicks
* 일자별 conversions
* 일자별 revenue
* 캠페인별 CTR
* 캠페인별 CVR
* 캠페인별 revenue ranking

### 9-2. Operation Metrics 탭

운영 메트릭 탭에서는 파이프라인 상태를 확인한다.

포함 지표는 다음과 같다.

* latest snapshot time
* 일자별 processed row count
* file count
* average file size
* duplicate event count
* late conversion count
* latest summary update time
* processed vs summary consistency check

Operation Metrics 탭은 단순 시각화용이 아니라, 운영자가 파이프라인의 이상 여부를 빠르게 확인하기 위한 모니터링 화면으로 설계한다.

---

## 10. Iceberg Management 자동화 계획

최종 프로젝트에서는 Iceberg table management 자동화 중 최소 1개를 구현한다.

본 프로젝트에서는 우선 `Compaction 자동화`를 구현한다.

### 10-1. 자동화 대상

* `processed_events`
* `campaign_summary`

### 10-2. 자동화 방식

Airflow DAG를 이용해 매일 새벽 3시에 compaction job을 실행한다.

### 10-3. 실행 조건

* file count가 기준치 이상 증가한 경우
* average file size가 기준치보다 작은 경우
* streaming micro-batch로 small file이 과도하게 생성된 경우
* dashboard 조회 지연 또는 Athena scan 비용 증가가 확인된 경우

### 10-4. 실행 범위

Compaction은 streaming ingestion이 진행 중인 최신 partition과 충돌하지 않도록 D-1 또는 D-2 이전 partition을 대상으로 수행한다.

예를 들어 현재 날짜가 2026-06-07이면, compaction 대상은 2026-06-06 또는 2026-06-05 이전 partition으로 제한한다.

### 10-5. 실패 및 재시도 전략

* Iceberg commit conflict가 발생하면 해당 job은 실패로 기록한다.
* Airflow retry 정책을 통해 일정 시간 후 재시도한다.
* Compaction은 비즈니스 KPI 계산과 분리된 maintenance job으로 운영한다.
* Compaction이 실패해도 Bronze/Silver/Gold 적재 자체는 계속 수행되도록 설계한다.

### 10-6. 예상 SQL

```sql
CALL glue_catalog.system.rewrite_data_files(
  table => 'ad_lakehouse.processed_events'
);
```

---

## 11. 100x 스케일 아웃 시나리오

본 프로젝트는 현재 일 100만 이벤트를 처리하는 작은 광고 플랫폼을 가정하지만, 1~2년 내 일 1억 이벤트까지 증가할 수 있다고 본다.

### 11-1. 예상 병목과 대응 전략

| 병목 지점     | 100x 성장 시 문제                     | 대응 전략                                               |
| --------- | -------------------------------- | --------------------------------------------------- |
| Kafka     | consumer lag 증가                  | topic partition 수 증가                                |
| Spark     | micro-batch 처리 지연                | EMR 또는 EKS Spark로 전환                                |
| Raw zone  | raw 파일 수 증가                      | raw_date, raw_hour partition 관리                     |
| Iceberg   | data file, manifest, snapshot 증가 | compaction, rewrite_manifests, expire_snapshots 자동화 |
| Athena    | scan cost 증가                     | Gold summary 중심 조회                                  |
| Dashboard | 직접 raw 조회 시 지연                   | BI 전용 Gold table 제공                                 |
| Backfill  | 장기간 재처리 비용 증가                    | event_date window 기반 backfill job 분리                |

핵심 전략은 raw 데이터를 직접 조회하지 않고, Silver와 Gold 계층을 명확히 분리하는 것이다. BI는 Gold summary를 조회하도록 제한하고, 운영자는 Iceberg metadata table을 이용해 snapshot freshness, file count, summary update 상태를 확인한다.

---

## 12. 멱등성 및 재처리 가능성 설계

### 12-1. Raw Zone

Raw zone은 append-only로 설계한다. Kafka에서 들어온 이벤트는 원본 보존을 위해 삭제하지 않고 저장한다. 이는 향후 정제 로직 오류나 summary 계산 오류가 발견되었을 때 raw 데이터를 기준으로 재처리하기 위함이다.

또한 Kafka `topic`, `partition`, `offset`, `ingest_time`을 함께 저장하여 장애 발생 시 어떤 이벤트가 어느 시점에 들어왔는지 추적할 수 있게 한다.

### 12-2. Silver Layer

Silver `processed_events`는 `event_id` 기준으로 중복 제거를 수행한다. 동일 event_id가 여러 번 들어와도 downstream에서는 하나의 이벤트만 사용하도록 한다.

Silver 계층에서는 다음 로직을 적용한다.

* event_id 기준 deduplication
* event_time/ingest_time 변환
* event_type 검증
* late conversion 여부 계산
* 잘못된 campaign_id/user_id 제거

### 12-3. Gold Layer

Gold `campaign_summary`는 append-only가 아니라 MERGE 기반으로 갱신한다. `event_date + campaign_id`를 key로 사용하여 같은 날짜와 캠페인에 대한 summary가 중복 insert되지 않도록 한다.

이 구조를 사용하면 late conversion이 들어와 과거 날짜의 summary가 바뀌더라도 기존 row를 안정적으로 갱신할 수 있다.

### 12-4. Backfill

정제 로직이 변경되거나 late conversion 반영 범위를 늘려야 하는 경우 raw zone에서 특정 날짜 window를 다시 읽어 Silver와 Gold를 재계산한다. Gold summary는 MERGE 기반으로 갱신되므로 같은 backfill job을 여러 번 실행해도 동일 key의 row가 중복 생성되지 않는다.

---

## 13. 3차시 구현 범위

3차시 기준 구현 범위는 다음과 같다.

```text
Criteo CSV / Sample Ad Events
        ↓
Kafka Producer
        ↓
Kafka Topic: ad-events
        ↓
Spark Structured Streaming
        ↓
Bronze Raw Zone
```

3차시에서는 장애 시나리오 중 전체를 완성하기보다는, 이후 장애 대응이 가능하도록 raw ingestion 기반을 먼저 만든다.

현재 구현 또는 구현 예정 파일은 다음과 같다.

```text
code/pipelines/kafka_producer.py
code/pipelines/kafka_to_raw_files.py
data/ad_events_sample.csv
screenshots/01_kafka_producer_log.png
screenshots/02_spark_streaming_log.png
screenshots/03_raw_files_created.png
```

3차시 구현에서 확인할 핵심은 다음과 같다.

* Kafka Producer가 CSV 데이터를 row 단위로 Kafka topic에 발행하는지 확인한다.
* Spark Structured Streaming이 Kafka topic을 정상적으로 구독하는지 확인한다.
* Bronze raw zone에 이벤트가 append-only 방식으로 저장되는지 확인한다.
* Kafka `topic`, `partition`, `offset`, `ingest_time`을 함께 저장할 수 있도록 설계한다.
* Spark Structured Streaming의 `checkpointLocation`을 지정하여 job 재시작 시 offset 복구가 가능하도록 한다.

3차시 이후에는 다음을 순차적으로 구현한다.

* Silver `processed_events` Iceberg DDL 작성
* raw files → processed_events 정제 job 구현
* event_id 기준 deduplication 구현
* late conversion 여부 계산 로직 구현
* Gold `campaign_summary` Iceberg table 생성
* 최근 7일 또는 14일 window 기반 summary 재집계 구현
* late conversion 반영을 위한 MERGE 구현
* Iceberg metadata 기반 health query 작성
* duplicate event count, late conversion count, file count 확인 쿼리 작성
* Airflow 기반 compaction 자동화
* QuickSight dashboard 구성
* 10~15분 최종 발표 자료 작성

---

## 14. 레포지토리 구조

최종 레포지토리는 다음 구조를 목표로 한다.

```text
final-project/
├── README.md
├── infra/
│   ├── docker-compose.yml
│   └── setup_aws_s3_glue.md
├── code/
│   ├── ddl/
│   │   ├── 01_create_raw_events.sql
│   │   ├── 02_create_processed_events.sql
│   │   └── 03_create_campaign_summary.sql
│   ├── pipelines/
│   │   ├── kafka_producer.py
│   │   ├── kafka_to_raw_files.py
│   │   ├── raw_to_processed_iceberg.py
│   │   └── processed_to_campaign_summary.py
│   └── health-queries/
│       ├── 01_snapshot_freshness.sql
│       ├── 02_daily_row_count.sql
│       ├── 03_file_count_avg_size.sql
│       ├── 04_duplicate_event_id_check.sql
│       ├── 05_late_conversion_check.sql
│       └── 06_summary_consistency_check.sql
├── orchestration/
│   ├── airflow_compaction_dag.py
│   └── README.md
├── dashboard/
│   ├── business_kpi_mockup.png
│   ├── operation_metrics_mockup.png
│   └── README.md
├── docs/
│   ├── architecture.png
│   ├── failure_scenarios.md
│   └── scale_out_100x.md
└── screenshots/
    ├── 01_kafka_producer_log.png
    ├── 02_spark_streaming_log.png
    └── 03_raw_files_created.png
```

---

## 15. 최종 프로젝트 방향 요약

본 프로젝트는 단순히 Kafka와 Spark로 데이터를 적재하는 것이 아니라, 광고 도메인에서 발생할 수 있는 운영 문제를 Lakehouse 구조로 해결하는 것을 목표로 한다.

핵심 운영 문제는 다음 세 가지이다.

1. Late Conversion으로 인해 과거 KPI가 변경되는 문제
2. Streaming Job 재시작 또는 Producer 재전송으로 인해 event_id가 중복 적재되는 문제
3. 캠페인 트래픽 폭증으로 small file이 증가하는 문제

추가적으로 정제 로직 오류가 뒤늦게 발견될 경우를 대비해 Bronze raw zone 기반 backfill 가능성도 고려한다.

이를 해결하기 위해 Kafka, Spark Structured Streaming, S3, Iceberg, Glue Catalog, Athena, Airflow, QuickSight를 활용한다. Iceberg는 Silver와 Gold 계층에서 MERGE, snapshot, metadata table, compaction을 가능하게 하므로 운영 가능한 광고 데이터 파이프라인을 설계하는 데 핵심 역할을 한다.

이 프로젝트의 핵심은 다음과 같다.

* Bronze는 원본 보존과 재처리를 담당한다.
* Silver는 중복 제거, 정제, late conversion 계산을 담당한다.
* Gold는 비즈니스 KPI summary와 MERGE 기반 갱신을 담당한다.
* Iceberg metadata table은 운영 모니터링과 장애 확인에 활용한다.
* Airflow compaction job은 streaming ingestion으로 발생하는 small file 문제를 관리한다.

최종적으로 본 프로젝트는 광고 이벤트 데이터의 수집, 정제, 집계, 운영 모니터링, table management 자동화까지 포함하는 Lakehouse 기반 데이터 파이프라인 포트폴리오를 목표로 한다.
