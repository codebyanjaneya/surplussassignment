import json, sys
e = json.load(sys.stdin)
print(f"  {e['eventId']}: status={e['status']} attempts={e['attempts']} err={e['lastError']}")
for a in e["history"]:
    print(f"     attempt {a['attemptNumber']} by {a['workerId']}: {a['result']} {a['error'] or ''}")
