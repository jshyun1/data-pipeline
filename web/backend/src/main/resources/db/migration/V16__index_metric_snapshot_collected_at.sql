-- pipeline_metric_snapshot은 파이프라인당 20초마다 한 행씩 쌓이는데(하루 약 3,700행,
-- 1년이면 130만행) 생성 이후 PK 말고는 인덱스가 없었다. 조회 쪽은 전부 "특정 기간의
-- 파이프라인별 시계열"을 보기 때문에 매번 전체 스캔 + 정렬이 된다:
--   - 대시보드 통계 구역의 시간대별 CDC 처리 건수(committed offset 증가분, 30초마다 폴링)
--   - CDC 처리/이벤트 로그 화면(CdcLogService)
--
-- (pipeline_id, collected_at) 순서라 윈도우 함수의 PARTITION BY/ORDER BY와 그대로 맞고,
-- 실측에서 정렬 노드(25만행 기준 quicksort 1974kB)가 사라지고 Index Scan으로 바뀐다.
-- 지금 크기(2.5만행)에서는 체감 차이가 없지만, 상시 가동하는 서버에서 행이 쌓일수록
-- 이 정렬이 디스크로 넘어가며 느려지는 것을 막는다.
CREATE INDEX idx_metric_snapshot_pipeline_collected
    ON pipeline_metric_snapshot (pipeline_id, collected_at);
