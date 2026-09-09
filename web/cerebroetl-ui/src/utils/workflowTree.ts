import type { WorkflowSummary } from "../api/workflows";

/**
 * 워크플로우 계층 한 칸. 자식은 «그 워크플로우 캔버스에 노드로 놓인 하위 워크플로우»다.
 *
 * <p>예전에는 워크플로우 목록을 NiFi 업무 그룹(DW/DZ/Template)으로 묶어 보여줬다. 그런데
 * 실제로 알고 싶은 건 «total 을 돌리면 무엇이 같이 도는가»이고, 그건 그룹이 아니라 캔버스에
 * 그린 상하 관계다. 그래서 목록·팔레트 모두 이 계층으로 세운다.
 */
export interface WorkflowTreeItem {
  workflow: WorkflowSummary;
  children: WorkflowTreeItem[];
}

/**
 * 넘겨준 워크플로우들만으로 상하 계층을 세운다.
 *
 * <p>최상단은 «누구의 하위도 아닌» 워크플로우다. 하위 목록에 있어도 넘겨준 목록에 없는
 * 워크플로우(삭제됐거나 호출부가 걸러낸 것)는 없는 셈 친다.
 *
 * <p>순환(A 안에 B, B 안에 A)이 있으면 서로가 서로의 하위라 최상단이 하나도 안 나온다.
 * 그대로 두면 목록에서 통째로 사라지므로, 한 번도 못 그린 워크플로우는 마지막에 최상단으로
 * 올려 붙인다. 화면에서 워크플로우가 «없어지는» 일은 없어야 한다(순환 자체는 게시할 때
 * 서버 검증이 막는다).
 */
export function buildWorkflowHierarchy(workflows: WorkflowSummary[]): WorkflowTreeItem[] {
  const byId = new Map(workflows.map((w) => [w.id, w] as const));
  const parented = new Set<number>();
  workflows.forEach((w) => {
    (w.childWorkflowIds ?? []).forEach((childId) => {
      if (byId.has(childId)) {
        parented.add(childId);
      }
    });
  });

  const emitted = new Set<number>();
  // path 는 «지금 내려온 길»이다. 같은 워크플로우가 다시 나오면 순환이므로 멈춘다.
  const build = (workflow: WorkflowSummary, path: Set<number>): WorkflowTreeItem => {
    emitted.add(workflow.id);
    const nextPath = new Set(path).add(workflow.id);
    const children = (workflow.childWorkflowIds ?? [])
      .map((childId) => byId.get(childId))
      .filter((child): child is WorkflowSummary => Boolean(child) && !nextPath.has(child!.id))
      .map((child) => build(child, nextPath));
    return { workflow, children };
  };

  const roots = workflows.filter((w) => !parented.has(w.id)).map((w) => build(w, new Set()));
  const orphans = workflows.filter((w) => !emitted.has(w.id)).map((w) => build(w, new Set()));
  return [...roots, ...orphans];
}

/** 계층을 평평하게 편다(그린 순서 그대로). 선택 처리에서 하위까지 함께 볼 때 쓴다. */
export function flattenWorkflowTree(items: WorkflowTreeItem[]): WorkflowSummary[] {
  return items.flatMap((item) => [item.workflow, ...flattenWorkflowTree(item.children)]);
}

/**
 * 최상단부터 이 워크플로우까지의 경로 이름들. 예: ["total", "dz_com_daily"].
 *
 * <p>상위가 여럿일 수 있으므로(같은 워크플로우를 두 곳에서 품을 수 있다) «먼저 찾은 한 줄»을
 * 돌려준다. 화면은 «어디에 속해 있나»를 한눈에 보여주면 되고, 전부 나열하면 오히려 읽기 어렵다.
 *
 * <p>순환이 있어도 방문한 id 를 기억해 멈춘다. 목록에 없는 id 면 빈 배열이다.
 */
export function workflowPathOf(workflows: WorkflowSummary[], targetId: number): string[] {
  const byId = new Map(workflows.map((w) => [w.id, w] as const));
  const parentOf = new Map<number, number>();
  workflows.forEach((w) => {
    (w.childWorkflowIds ?? []).forEach((childId) => {
      if (byId.has(childId) && !parentOf.has(childId)) {
        parentOf.set(childId, w.id);
      }
    });
  });
  const target = byId.get(targetId);
  if (!target) {
    return [];
  }
  const names: string[] = [target.name];
  const seen = new Set<number>([targetId]);
  let current = parentOf.get(targetId);
  while (current !== undefined && !seen.has(current)) {
    seen.add(current);
    names.unshift(byId.get(current)!.name);
    current = parentOf.get(current);
  }
  return names;
}
