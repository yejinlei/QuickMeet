#!/bin/bash
# Dispatch QuickMeet Epic sub-issues under parent YEJ-89.
# Stage 1 -> todo (fires immediately); stages 2-5 -> backlog (stage barriers).
set -uo pipefail

EPIC=01a09ee0-cb5a-7bab-914f-62921abdc5dc
PROJECT=e2eed30d-0ede-4868-8974-377918654a92
cd "$(dirname "$0")"

python - <<'PY' > plan.tsv
import json
for m in json.load(open('desc/manifest.json', encoding='utf-8')):
    print("\t".join([m['file'], m['title'], str(m['stage']), m['agent_id']]))
PY

while IFS=$'\t' read -r file title stage agent; do
  if [ -z "${file:-}" ]; then continue; fi
  status=backlog
  [ "$stage" = "1" ] && status=todo
  echo ">> $title | stage=$stage | status=$status"
  out=$(multica issue create \
      --title "$title" \
      --description-file "./$file" \
      --parent "$EPIC" \
      --project "$PROJECT" \
      --stage "$stage" \
      --status "$status" \
      --assignee-id "$agent" \
      --output json 2>err.txt)
  if [ -z "$out" ]; then
    echo "   FAILED: $(tr '\n' ' ' < err.txt)"
    continue
  fi
  echo "$out" | python -c "
import json,sys
d=json.load(sys.stdin)
print('   ok id=%s number=%s status=%s stage=%s' % (d.get('id'), d.get('identifier') or d.get('number'), d.get('status'), d.get('stage')))
" || echo "   raw: $out"
done < plan.tsv
