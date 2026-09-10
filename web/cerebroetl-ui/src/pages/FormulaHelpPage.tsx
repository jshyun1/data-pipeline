import { CopyOutlined, QuestionCircleOutlined } from "@ant-design/icons";
import { Button, Input, message, Spin } from "antd";
import { useEffect, useMemo, useState } from "react";
import { listFormulaHelp, type FormulaHelpItem } from "../api/formulaHelp";

function initialLetter(value: string) {
  return value.trim().charAt(0).toUpperCase();
}

function copySyntax(item: FormulaHelpItem) {
  navigator.clipboard.writeText(item.syntax)
    .then(() => message.success("구문을 복사했습니다."))
    .catch(() => message.error("복사하지 못했습니다."));
}

export function FormulaHelpPage() {
  const [items, setItems] = useState<FormulaHelpItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [keyword, setKeyword] = useState("");
  const [letter, setLetter] = useState<string | null>(null);
  const [category, setCategory] = useState("전체");
  const [openId, setOpenId] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    listFormulaHelp()
      .then((result) => {
        if (!cancelled) {
          setItems(result);
          setError(null);
        }
      })
      .catch((ex) => {
        if (!cancelled) {
          setError(ex instanceof Error ? ex.message : "수식 도움말을 불러오지 못했습니다.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const letters = useMemo(() => (
    Array.from(new Set(items.map((item) => initialLetter(item.functionName)))).sort()
  ), [items]);
  const categories = useMemo(() => (
    ["전체", ...Array.from(new Set(items.map((item) => item.category)))]
  ), [items]);

  const filteredItems = useMemo(() => {
    const normalizedKeyword = keyword.trim().toLowerCase();
    return items.filter((item) => {
      const matchesKeyword = !normalizedKeyword ||
        item.functionName.toLowerCase().includes(normalizedKeyword) ||
        item.syntax.toLowerCase().includes(normalizedKeyword) ||
        item.summary.toLowerCase().includes(normalizedKeyword);
      const matchesLetter = !letter || initialLetter(item.functionName) === letter;
      const matchesCategory = category === "전체" || item.category === category;
      return matchesKeyword && matchesLetter && matchesCategory;
    });
  }, [category, items, keyword, letter]);

  return (
    <main className="formula-help-page">
      <header className="formula-help-titlebar">
        <strong>수식 도움말</strong>
      </header>
      <section className="formula-help-controls">
        <Input
          value={keyword}
          allowClear
          placeholder="필터 검색"
          onChange={(event) => setKeyword(event.target.value)}
        />
        <div className="formula-help-letters" aria-label="알파벳 필터">
          {letters.map((entry) => (
            <button
              key={entry}
              type="button"
              className={letter === entry ? "active" : ""}
              onClick={() => setLetter(letter === entry ? null : entry)}
            >
              {entry}
            </button>
          ))}
        </div>
        <div className="formula-help-categories" aria-label="분류 필터">
          {categories.map((entry) => (
            <button
              key={entry}
              type="button"
              className={category === entry ? "active" : ""}
              onClick={() => setCategory(entry)}
            >
              {entry}
            </button>
          ))}
        </div>
      </section>
      <section className="formula-help-list">
        {loading ? (
          <div className="formula-help-state"><Spin /></div>
        ) : error ? (
          <div className="formula-help-state">{error}</div>
        ) : filteredItems.length === 0 ? (
          <div className="formula-help-state">일치하는 함수가 없습니다.</div>
        ) : filteredItems.map((item) => {
          const open = openId === item.id;
          return (
            <article key={item.id} className={`formula-help-item${open ? " open" : ""}`}>
              <div className="formula-help-item-toolbar">
                <Button size="small" icon={<CopyOutlined />} onClick={() => copySyntax(item)}>
                  코드 복사
                </Button>
                <Button
                  size="small"
                  icon={<QuestionCircleOutlined />}
                  onClick={() => setOpenId(open ? null : item.id)}
                >
                  도움말
                </Button>
              </div>
              <p className="formula-help-summary">
                <strong>{item.syntax}</strong>
                <span>// {item.summary}</span>
              </p>
              {open ? (
                <div className="formula-help-curtain">
                  <h2>사용법</h2>
                  <p>{item.usageText}</p>
                  <h2>구문</h2>
                  <pre>{item.syntax}</pre>
                  <h2>예제</h2>
                  <table>
                    <thead>
                      <tr>
                        <th>수식</th>
                        <th>설명</th>
                        <th>결과</th>
                      </tr>
                    </thead>
                    <tbody>
                      {item.examples.map((example, index) => (
                        <tr key={`${item.id}-${index}`}>
                          <td>{example.formula}</td>
                          <td>{example.description}</td>
                          <td>{example.result}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {item.notice ? (
                    <>
                      <h2>주의사항</h2>
                      <p>{item.notice}</p>
                    </>
                  ) : null}
                  <div className="formula-help-close">
                    <Button size="small" onClick={() => setOpenId(null)}>닫기</Button>
                  </div>
                </div>
              ) : null}
            </article>
          );
        })}
      </section>
    </main>
  );
}
