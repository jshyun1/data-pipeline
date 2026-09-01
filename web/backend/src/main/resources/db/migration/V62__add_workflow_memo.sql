-- 워크플로우 메모.
--
-- 캔버스만 보고는 "이 워크플로우가 왜 이렇게 생겼는지"를 알 수 없다. 담당자·주의사항·
-- 재실행 절차 같은 걸 그림 옆에 붙여 둘 자리가 필요하다. 화면(캔버스·속성창) 양쪽에서
-- 같은 값을 고친다.
ALTER TABLE etl_workflow ADD COLUMN IF NOT EXISTS memo TEXT;

COMMENT ON COLUMN etl_workflow.memo IS '워크플로우 메모(캔버스·속성창 공용)';
