#!/usr/bin/env bash
# Starts/stops the whole managed-flink-sql-demo stack on ohad-demo-1: minikube,
# waits for the CFK (Kafka/SchemaRegistry/ControlCenter) and CMF/Flink pods to
# be healthy, then opens the three UI port-forwards. `stop` reverses it and
# pauses minikube (not delete) so the demo's state is preserved for next time.
#
# Usage:
#   scripts/demo.sh start
#   scripts/demo.sh stop
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="$REPO_DIR/.demo-pids"
export PATH="$HOME/bin:$PATH"
export CONFLUENT_CMF_URL="http://localhost:8080"

MINIKUBE_CPUS=6
MINIKUBE_MEMORY=12g
MINIKUBE_DISK=40g

CMF_NAMESPACE=flink-sql-demo
CFK_NAMESPACE=confluent
FLINK_ENVIRONMENT=sql-demo
ENRICHMENT_STATEMENT=customer-click-enrichment

wait_for_pods_ready() {
  local ns="$1" timeout_s="${2:-300}" waited=0
  echo "Waiting for pods in namespace '$ns' to be Ready..."
  while true; do
    local total ready
    total=$(kubectl get pods -n "$ns" --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [ "$total" -gt 0 ]; then
      ready=$(kubectl get pods -n "$ns" --no-headers 2>/dev/null \
        | awk '{split($2,a,"/"); if (a[1]==a[2] && $3=="Running") print}' | wc -l | tr -d ' ')
      if [ "$ready" -eq "$total" ]; then
        echo "  $ns: $ready/$total pods Ready."
        return 0
      fi
      echo "  $ns: $ready/$total pods Ready (waited ${waited}s)..."
    else
      echo "  $ns: no pods found yet (waited ${waited}s)..."
    fi
    if [ "$waited" -ge "$timeout_s" ]; then
      echo "  Timed out after ${timeout_s}s waiting for $ns to be Ready." >&2
      kubectl get pods -n "$ns" 2>&1 || true
      return 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
}

wait_for_http() {
  local url="$1" timeout_s="${2:-60}" waited=0
  while true; do
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 3 "$url" || echo "000")
    if [ "$code" = "200" ]; then
      return 0
    fi
    if [ "$waited" -ge "$timeout_s" ]; then
      echo "  Timed out waiting for $url (last code: $code)" >&2
      return 1
    fi
    sleep 3
    waited=$((waited + 3))
  done
}

cmd_start() {
  echo "== 1/4: minikube =="
  if minikube status 2>/dev/null | grep -q "^host: Running"; then
    echo "minikube already running."
  else
    minikube start --driver=docker --cpus="$MINIKUBE_CPUS" --memory="$MINIKUBE_MEMORY" --disk-size="$MINIKUBE_DISK"
  fi

  echo "== 2/4: waiting for CFK (Kafka/SchemaRegistry/ControlCenter) and CMF/Flink pods =="
  wait_for_pods_ready "$CFK_NAMESPACE" 420
  wait_for_pods_ready "$CMF_NAMESPACE" 300

  echo "== 3/4: starting port-forwards =="
  : > "$PIDFILE"

  nohup kubectl port-forward -n "$CMF_NAMESPACE" svc/cmf-service 8080:80 \
    > "$REPO_DIR/.cmf-port-forward.log" 2>&1 &
  echo "$!" >> "$PIDFILE"
  sleep 3

  nohup confluent flink statement web-ui-forward "$ENRICHMENT_STATEMENT" \
    --environment "$FLINK_ENVIRONMENT" --port 8090 \
    > "$REPO_DIR/.flink-ui-forward.log" 2>&1 &
  echo "$!" >> "$PIDFILE"

  nohup kubectl port-forward -n "$CFK_NAMESPACE" svc/controlcenter 9021:9021 \
    > "$REPO_DIR/.c3-port-forward.log" 2>&1 &
  echo "$!" >> "$PIDFILE"

  echo "== 4/4: confirming all three UIs are reachable =="
  ok=1
  wait_for_http "http://localhost:8080" 60 && echo "  CMF UI:        OK" || ok=0
  wait_for_http "http://localhost:8090" 90 && echo "  Flink job UI:  OK" || ok=0
  wait_for_http "http://localhost:9021" 90 && echo "  Control Center: OK" || ok=0

  if [ "$ok" -ne 1 ]; then
    echo "One or more UIs did not come up cleanly -- check .cmf-port-forward.log / .flink-ui-forward.log / .c3-port-forward.log in $REPO_DIR" >&2
    exit 1
  fi

  cat <<EOF

Demo is up.

Now run this on your Mac (separate terminal, leave it open) to view the UIs:
  gcloud compute ssh ohad-demo-1 --zone us-east1-d --tunnel-through-iap -- -L 8080:localhost:8080 -L 8090:localhost:8090 -L 9021:localhost:9021

Then open these in your Mac browser:
  CMF UI:          http://localhost:8080
  Flink job UI:    http://localhost:8090
  Control Center:  http://localhost:9021
EOF
}

cmd_stop() {
  echo "== 1/2: stopping port-forwards =="
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do
      [ -z "$pid" ] && continue
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null && echo "  killed PID $pid"
      else
        echo "  PID $pid already gone"
      fi
    done < "$PIDFILE"
    rm -f "$PIDFILE"
  else
    echo "  No pidfile at $PIDFILE -- nothing tracked to kill."
  fi

  echo "== 2/2: stopping minikube (pausing, not deleting) =="
  minikube stop

  sleep 2
  if minikube status 2>/dev/null | grep -q "^host: Running"; then
    echo "WARNING: minikube still reports Running." >&2
  else
    echo "minikube stopped."
  fi

  remaining=0
  for port in 8080 8090 9021; do
    if lsof -i ":$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "WARNING: something is still listening on port $port" >&2
      remaining=1
    fi
  done
  [ "$remaining" -eq 0 ] && echo "No port-forwards remain listening."
}

case "${1:-}" in
  start) cmd_start ;;
  stop) cmd_stop ;;
  *)
    echo "Usage: $0 {start|stop}" >&2
    exit 1
    ;;
esac
