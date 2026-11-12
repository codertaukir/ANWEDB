#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git diff --quiet || { echo "Uncommitted changes hain. Pehle git commit ya git stash kar."; exit 1; }

FILES="crates/mitrdb-storage/src/wal.rs crates/mitrdb-txn/src/lib.rs"

cargo add -p mitrdb-storage parking_lot@0.12
cargo add -p mitrdb-txn parking_lot@0.12

python3 - $FILES <<'PY'
import re, sys

def split_top(s):
    out, depth, cur = [], 0, ''
    for ch in s:
        if ch == '{': depth += 1
        if ch == '}': depth -= 1
        if ch == ',' and depth == 0:
            out.append(cur.strip()); cur = ''
        else:
            cur += ch
    if cur.strip(): out.append(cur.strip())
    return [x for x in out if x]

MOVE = ('Mutex', 'Condvar', 'MutexGuard')

for path in sys.argv[1:]:
    s = open(path).read()
    moved = set()

    def fix_group(m):
        keep = []
        for it in split_top(m.group(1)):
            if it in MOVE: moved.add(it)
            else: keep.append(it)
        return 'use std::sync::{%s};' % ', '.join(keep) if keep else ''
    s = re.sub(r'use std::sync::\{(.*?)\};', fix_group, s, flags=re.S)

    def fix_single(m):
        moved.add(m.group(1)); return ''
    s = re.sub(r'use std::sync::(Mutex|Condvar|MutexGuard);[ \t]*\n?', fix_single, s)

    if moved:
        line = 'use parking_lot::{%s};\n' % ', '.join(sorted(moved))
        s = re.sub(r'(?m)^use ', line + 'use ', s, count=1)

    s = s.replace('std::sync::MutexGuard', 'parking_lot::MutexGuard')
    s = s.replace('std::sync::Condvar', 'parking_lot::Condvar')
    s = s.replace('std::sync::Mutex', 'parking_lot::Mutex')

    s = re.sub(r'\.lock\(\)\s*\.unwrap\(\)', '.lock()', s)
    s = re.sub(r'\.lock\(\)\s*\.expect\([^)]*\)', '.lock()', s)
    s = re.sub(r'(\w+)\s*=\s*([\w\.]+)\.wait\(\s*\1\s*\)\s*\.unwrap\(\)\s*;',
               r'\2.wait(&mut \1);', s)

    open(path, 'w').write(s)
    print(f"patched {path} (moved: {sorted(moved)})")
PY

echo
echo "=== Manual check lagne wali jagah (agar kuch dikhe toh fix kar) ==="
grep -nE "wait_timeout|wait_while|into_inner|is_poisoned|PoisonError|lock\(\)\.map_err|unwrap_or_else\(\|e\| e\.into_inner" $FILES || echo "none"

echo
echo "=== Build ==="
cargo build --release -p mitrdb-server --example write_mark_real 2>&1 | tail -40
