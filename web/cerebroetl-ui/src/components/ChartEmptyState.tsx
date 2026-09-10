/**
 * 차트 카드의 빈 상태. 디자인 초안의 «점선 원 아이콘 + 안내 문구» 모양이다.
 * 카드가 남긴 높이를 채우고 가운데에 놓인다(.empty-chart-placeholder).
 */
export function ChartEmptyState({ text }: { text: string }) {
  return (
    <div className="empty-chart-placeholder">
      <div className="empty-chart-state">
        <span className="empty-icon-circle" aria-hidden="true">
          <svg
            className="empty-icon"
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <polyline points="22 12 16 12 14 15 10 15 8 12 2 12" />
            <path d="M5.45 5.11L2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" />
          </svg>
        </span>
        <span>{text}</span>
      </div>
    </div>
  );
}
