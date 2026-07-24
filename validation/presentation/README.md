# 발표자료용 실행 검증

`validate_pipeline.sh`는 발표자료에 넣을 근거를 동일한 방식으로 재현하기 위한
검증 프로세스다.

검증 범위:

- Docker Compose 구성 유효성
- 핵심 서비스 12개의 `running/healthy` 상태
- Oracle CDC Source, PostgreSQL CDC Sink, 로그 Sink의 connector/task 상태
- Airflow 메타DB·스케줄러·DAG Processor 상태
- Cerebro ETL 업무 API의 배포 파이프라인 조회
- 고정 Kafka 토픽에 대한 실제 produce/consume
- 기존 업무 테이블과 분리된 검증 전용 Oracle/PostgreSQL 테이블 및 Debezium
  Source/JDBC Sink 커넥터 생성
- 전용 테이블의 `INSERT/UPDATE/DELETE`가 Kafka Connect를 거쳐 PostgreSQL에
  반영되는지 60초 제한으로 폴링하고 각 구간 지연시간 측정

실행:

```bash
cd /home/user/data-pipeline
chmod +x validation/presentation/validate_pipeline.sh
./validation/presentation/validate_pipeline.sh
```

결과는 `validation/presentation/results/`에 Markdown으로 남는다. 직접
produce/consume용 토픽 `cerebroetl.presentation.validation.v1`과 CDC 토픽
`ppt-validation-cdc.APPUSER.PPT_VALIDATION_EVENTS`, 검증 전용 커넥터 2개도
삭제하지 않으므로 실행 상태와 메시지를 다시 조회할 수 있다.

보안상 검증 결과에는 DB 비밀번호·토큰·Connector 전체 설정을 기록하지 않는다.
