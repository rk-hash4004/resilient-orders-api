#!/bin/bash
# Retry queue recovery test: submit while inventory is DOWN, bring it UP after 4 s, show the final task.
B=localhost:8081
curl -s $B/mock/inventory/down; echo
ID=$(curl -s $B/self-healing/queue/submit | sed -n 's/.*"id": *"\([^"]*\)".*/\1/p')
echo "task id: $ID"
sleep 4
curl -s $B/mock/inventory/up; echo
sleep 8
curl -s "$B/self-healing/queue/status?id=$ID"; echo
