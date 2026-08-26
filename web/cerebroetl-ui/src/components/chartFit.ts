/**
 * 차트 축 라벨이 잘려 보이던 문제를 막는다.
 *
 * 대시보드 차트는 고정 높이를 쓰지 않고 카드가 남긴 공간을 채운다(`.chart-fit-shell` -
 * flex + min-height:0 안에 절대배치). autoFit 은 «마운트 시점»에 컨테이너를 재는데, 그
 * 순간에는 flex 높이가 아직 확정되지 않아 축이 그려질 자리를 잘못 잡는다. 그러면 x축
 * 라벨이 보이는 영역 밖으로 밀려 그래프만 덩그러니 남는다.
 *
 * 탭을 바꾸면 데이터가 바뀌며 다시 그려져 «저절로 고쳐지는» 것처럼 보였다(2026-08-26
 * ETL Job Top 5 제보). 그때는 이미 레이아웃이 끝나 있어서 제대로 재는 것뿐이다.
 *
 * 처음에는 다음 프레임에 한 번만 다시 맞췄는데, 그것으로는 부족했다. 이 카드들은 같은 행의
 * 다른 카드(KPI·프로세스 목록)가 데이터를 받아 커지면서 높이가 «나중에» 바뀌기 때문에,
 * 한 시점만 잡아서는 놓친다. 그래서 컨테이너 크기를 계속 지켜보다 바뀔 때마다 맞춘다.
 * 창 크기 변경·사이드바 접기에도 같이 대응된다.
 */
type FitTarget = {
  forceFit: () => Promise<unknown>;
  getContainer?: () => HTMLElement | null | undefined;
  on?: (event: string, handler: () => void) => void;
};

export function refitAfterLayout(chart: FitTarget): void {
  const refit = () => {
    void chart.forceFit();
  };

  // 레이아웃이 끝난 다음 프레임에 한 번.
  requestAnimationFrame(refit);

  const container = chart.getContainer?.();
  if (!container || typeof ResizeObserver === "undefined") {
    return;
  }

  // 관측 즉시 1회 콜백이 오는데 그건 이미 맞춰진 크기라 건너뛴다. 이후 변화에만 반응한다.
  let first = true;
  let scheduled = 0;
  const observer = new ResizeObserver(() => {
    if (first) {
      first = false;
      return;
    }
    // 연속 변화(애니메이션·드래그)에 매번 다시 그리면 무거우므로 프레임 단위로 합친다.
    if (scheduled) {
      return;
    }
    scheduled = requestAnimationFrame(() => {
      scheduled = 0;
      refit();
    });
  });
  observer.observe(container);

  // 차트가 사라질 때 관측을 끊는다. destroy 훅이 없으면 컨테이너가 DOM 에서 빠질 때까지
  // 남는데, ResizeObserver 는 대상이 사라지면 콜백이 오지 않으므로 누수 위험은 낮다.
  chart.on?.("afterdestroy", () => {
    if (scheduled) {
      cancelAnimationFrame(scheduled);
    }
    observer.disconnect();
  });
}
