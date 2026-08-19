import gzip, json, sys, copy

src, dst = sys.argv[1], sys.argv[2]

with gzip.open(src) as f:
    doc = json.load(f)

removed = []


def walk(g, path=''):
    p = path + '/' + g.get('name', '?')
    for kind in ('controllerServices', 'processors', 'reportingTasks'):
        for comp in g.get(kind, []) or []:
            props = comp.get('properties') or {}
            for k in [k for k, v in props.items() if isinstance(v, str) and v.startswith('enc{')]:
                del props[k]
                removed.append(f'{p} :: {comp.get("name")} :: {k}')
    for c in g.get('processGroups', []) or []:
        walk(c, p)


walk(doc['rootGroup'])

# 루트 밖(최상위)에 있는 리포팅 태스크/파라미터 컨텍스트도 훑는다
for rt in doc.get('reportingTasks', []) or []:
    props = rt.get('properties') or {}
    for k in [k for k, v in props.items() if isinstance(v, str) and v.startswith('enc{')]:
        del props[k]
        removed.append(f'(root)/reportingTask :: {rt.get("name")} :: {k}')

for ctx in doc.get('parameterContexts', []) or []:
    for pr in ctx.get('parameters', []) or []:
        p = pr.get('parameter', pr)
        v = p.get('value')
        if isinstance(v, str) and v.startswith('enc{'):
            p['value'] = None
            removed.append(f'(param) {ctx.get("name")} :: {p.get("name")}')

with gzip.open(dst, 'wt', encoding='utf-8') as f:
    json.dump(doc, f, ensure_ascii=False)

print(f'제거 {len(removed)}건')
for r in removed:
    print('  -', r)
