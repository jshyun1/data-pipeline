"""공식 nifi-server-nar 에서 화면 war(nifi-ui-*.war)만 Cerebro 빌드 결과로 바꾼 NAR 을 만든다(build.sh 4단계).

war 는 «Angular 빌드 결과 + META-INF + WEB-INF» 구조다(2.2.0 기준 확인). META-INF · WEB-INF 는 공식본 그대로 두고
나머지(화면 파일)만 바꾼다. NAR 의 다른 항목(nifi-web-api war 등)은 손대지 않는다.

사용: python3 repack_nar.py <공식 NAR> <화면 빌드 결과 폴더> <만들 NAR>
"""
import io
import os
import sys
import zipfile

KEEP_PREFIXES = ("META-INF/", "WEB-INF/")


def rebuild_war(official_war: bytes, dist_dir: str) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(official_war))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as war:
        kept = 0
        for info in source.infolist():
            if info.filename.startswith(KEEP_PREFIXES):
                war.writestr(info, source.read(info.filename))
                kept += 1
        added = 0
        for root, _, files in os.walk(dist_dir):
            for name in sorted(files):
                path = os.path.join(root, name)
                war.write(path, os.path.relpath(path, dist_dir).replace(os.sep, "/"))
                added += 1
    print(f"화면 war: 공식 META-INF/WEB-INF {kept}개 유지, Cerebro 화면 파일 {added}개")
    return buffer.getvalue()


def main(official_nar: str, dist_dir: str, out_nar: str) -> None:
    if not os.path.isfile(os.path.join(dist_dir, "index.html")):
        sys.exit(f"화면 빌드 결과에 index.html 이 없습니다: {dist_dir}")
    source = zipfile.ZipFile(official_nar)
    wars = [n for n in source.namelist() if os.path.basename(n).startswith("nifi-ui-") and n.endswith(".war")]
    if len(wars) != 1:
        sys.exit(f"공식 NAR 에서 화면 war 를 하나로 특정하지 못했습니다: {wars}")
    with zipfile.ZipFile(out_nar, "w", zipfile.ZIP_DEFLATED) as nar:
        for info in source.infolist():
            data = source.read(info.filename)
            if info.filename == wars[0]:
                data = rebuild_war(data, dist_dir)
            nar.writestr(info, data)
    print(f"Cerebro 화면 NAR: {out_nar} ({os.path.getsize(out_nar) // 1024} KB, 바꾼 항목 {wars[0]})")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:4])
