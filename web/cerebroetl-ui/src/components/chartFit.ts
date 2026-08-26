/**
 * 최초 진입에서 차트 축 라벨이 잘려 보이던 문제를 막는다.
 *
 * 대시보드 차트는 고정 높이를 쓰지 않고 카드가 남긴 공간을 채운다(`.chart-fit-shell` -
 * flex + min-height:0 안에 절대배치). autoFit 은 «마운트 시점»에 컨테이너를 재는데, 그
 * 순간에는 flex 높이가 아직 확정되지 않아 축이 그려질 자리를 잘못 잡는다. 그러면 x축
 * 라벨이 보이는 영역 밖으로 밀려 그래프만 덩그러니 남는다.
 *
 * 탭을 바꾸면 데이터가 바뀌며 다시 그려져 «저절로 고쳐지는» 것처럼 보였는데(2026-08-26
 * ETL Job Top 5 제보), 그때는 이미 레이아웃이 끝나 있어서 제대로 재는 것뿐이다.
 *
 * 레이아웃이 끝난 다음 프레임에 한 번 더 맞춰 준다. 이미 크기가 맞으면 무해하다.
 */
export function refitAfterLayout(chart: { forceFit: () => Promise<unknown> }): void {
  requestAnimationFrame(() => {
    void chart.forceFit();
  });
}
