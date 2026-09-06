#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"

required=(
	README.md CHANGELOG.md CONTRIBUTING.md LICENSE SECURITY.md SUPPORT.md
	COMPATIBILITY.md example_test.go docs/README.md docs/algorithms.md
	docs/api.md docs/benchmarks.md docs/composition.md docs/faq.md
	docs/kubernetes.md docs/operations.md
)
for path in "${required[@]}"; do
	if [[ ! -s "${path}" ]]; then
		printf 'required documentation is missing or empty: %s\n' "${path}" >&2
		exit 1
	fi
done

python3 - <<'PY'
from pathlib import Path
from urllib.parse import unquote
import re


def anchors(document: Path) -> set[str]:
    seen: dict[str, int] = {}
    result: set[str] = set()
    fenced = False
    for line in document.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
            continue
        if fenced:
            continue
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not match:
            continue
        slug = match.group(1).strip().lower()
        slug = re.sub(r"[^\w\- ]", "", slug)
        slug = re.sub(r"\s+", "-", slug)
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        result.add(slug if count == 0 else f"{slug}-{count}")
    return result


documents = [
    path for path in Path(".").rglob("*.md")
    if ".golib-tooling" not in path.parts and ".verification" not in path.parts
]
anchor_cache = {document.resolve(): anchors(document) for document in documents}
for document in documents:
    prose: list[str] = []
    fenced = False
    for line in document.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
            continue
        if not fenced:
            prose.append(line)
    for raw_target in re.findall(r"\[[^]]*\]\(([^)]+)\)", "\n".join(prose)):
        target = unquote(raw_target.strip().split()[0])
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        relative, _, fragment = target.partition("#")
        linked = (document.parent / relative).resolve() if relative else document.resolve()
        if not linked.exists():
            raise SystemExit(f"broken relative link in {document}: {raw_target}")
        if fragment and linked.suffix.lower() == ".md":
            linked_anchors = anchor_cache.get(linked)
            if linked_anchors is None:
                linked_anchors = anchors(linked)
                anchor_cache[linked] = linked_anchors
            if fragment.lower() not in linked_anchors:
                raise SystemExit(f"broken local anchor in {document}: {raw_target}")
print("documentation links and anchors resolve")
PY

module_path="$(go list -m -f '{{.Path}}')"
go doc "${module_path}" >/dev/null

example_count="$(grep -Ec '^func Example[[:alnum:]_]*\(\)' example_test.go)"
output_count="$(grep -Ec '^[[:space:]]*// Output:' example_test.go)"
if [[ "${example_count}" -eq 0 || "${example_count}" -ne "${output_count}" ]]; then
	printf 'every executable example must be nonempty and declare output\n' >&2
	exit 1
fi

if grep -En '(, _ :=|_ = .*\.(Close|Record|Release)\(\)|defer .*\.(Close|Record|Release)\(\))' \
	README.md example_test.go; then
	printf 'public documentation must handle returned and cleanup errors\n' >&2
	exit 1
fi

go test -mod=readonly ./... -run '^Example' -count=1
printf 'documentation contract passed\n'
