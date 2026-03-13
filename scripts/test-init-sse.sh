#!/usr/bin/env -S bash

set -ueo pipefail

PORT=3000
URL="http://127.0.0.1:$PORT"
URL_INIT="${URL}/vs/time/sse"

SESSION="mcp-time-server$PORT"

pkill -9 curl || echo "no running curl, ok"

curl -v -s -N -X GET -H -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream"  "$URL_INIT" > sse.txt &

SERVER_PID=$!

sleep .1

URL_MSG="$URL$(sed -n 's/^data: //p' sse.txt | tr -d '\r' | head -n 1)"

echo "$URL_MSG"


INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"demo","version":"0.0.1"}}}'

NOTIFY='{"jsonrpc": "2.0","method": "notifications/initialized"}'
LIST='{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
CALL='{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_system_time","arguments":{"timezone":"UTC"}}}'

curl -v "$URL_MSG" -d "$INIT"
curl -v"$URL_MSG" -d "$NOTIFY"
curl -v "$URL_MSG" -d "$LIST"
for i in $(seq 1 3); do
	# Create a new JSON payload with a unique ID for each request.
	BODY=$(echo "$CALL" | sed "s/\"id\":3/\"id\":$((i + 2))/")
	curl "$URL_MSG" -d "$BODY"
	sleep .01
done

cat sse.txt

kill "$SERVER_PID" 2>/dev/null || true
