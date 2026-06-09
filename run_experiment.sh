#!/bin/sh
#
# run_experiment.sh — Run on VM2 (10.0.0.2)
#
# Runs 4 sub-experiments:
#   1. 100% SET, Nagle OFF
#   2. 100% SET, Nagle ON
#   3. 95% SET + 5% GET, Nagle OFF
#   4. 95% SET + 5% GET, Nagle ON
#
# Prerequisites:
#   VM1: redis-server running on 10.0.0.1:6379
#   VM2: redis_bench compiled, redis-cli available
#

REDIS_HOST="10.0.0.1"
REDIS_PORT=6379
CLI="$HOME/patched-redis/src/redis-cli"
BENCH="$HOME/redis_bench"
RESULTS="$HOME/results"
INTERVAL=2
WARMUP=5
DURATION=12
RUNS=3
CPU=1

mkdir -p "$RESULTS"

RPS_LIST="1000 2500 5000 7500 10000 12500 15000 17500 20000 22500 25000 27500 30000 32500 35000 37500 40000 42500 45000 47500 50000 52500 55000 57500 60000 62500 65000 67500 70000 72500 75000 77500 80000"

# CSV header
echo "workload,nagle,target_rps,run,actual_rps,L_total_us,L_unacked_us,L_unread_client_us,L_ackdelay_server_us,L_unread_server_us" > "$RESULTS/latency.csv"

for WORKLOAD in set mixed; do
  for NAGLE in off on; do
    echo ""
    echo "########## WORKLOAD=$WORKLOAD NAGLE=$NAGLE ##########"
    echo ""

    for RPS in $RPS_LIST; do
      echo "=== RPS: $RPS ==="
      STOP=0
      for RUN in $(seq 1 $RUNS); do
        rm -f /tmp/qstate_*.txt

        taskset -c $CPU $BENCH $REDIS_HOST $REDIS_PORT $RPS $DURATION $WORKLOAD $NAGLE &
        BENCH_PID=$!
        sleep $WARMUP

        # Snapshot 1
        kill -USR1 $BENCH_PID
        sleep 0.1
        S1_SERVER=$($CLI -h $REDIS_HOST -p $REDIS_PORT QUEUESTATE)
        sleep $INTERVAL

        # Snapshot 2
        kill -USR1 $BENCH_PID
        sleep 0.1
        S2_SERVER=$($CLI -h $REDIS_HOST -p $REDIS_PORT QUEUESTATE)
        sleep 0.5

        S1_CLIENT=$(cat /tmp/qstate_0.txt 2>/dev/null)
        S2_CLIENT=$(cat /tmp/qstate_1.txt 2>/dev/null)

        wait $BENCH_PID 2>/dev/null

        python3 - <<PYEOF
import sys

s1c = [int(x) for x in """$S1_CLIENT""".split()]
s2c = [int(x) for x in """$S2_CLIENT""".split()]

# Redis QUEUESTATE output: first int is fd, then 12 queue fields
raw1 = """$S1_SERVER""".split()
s1s = [int(x) for x in raw1 if x.lstrip('-').isdigit()][1:13]
raw2 = """$S2_SERVER""".split()
s2s = [int(x) for x in raw2 if x.lstrip('-').isdigit()][1:13]

dt = $INTERVAL * 1e9

def Lat(p, n):
    Q = (n[1] - p[1]) / dt if dt > 0 else 0
    lam = (n[2] - p[2]) / (dt / 1e9) if dt > 0 else 0
    return Q / lam if lam > 0 else 0

L_ua = Lat(s1c[0:4], s2c[0:4])
L_ur_c = Lat(s1c[4:8], s2c[4:8])
L_ad_s = Lat(s1s[8:12], s2s[8:12])
L_ur_s = Lat(s1s[4:8], s2s[4:8])
Lt = L_ua - L_ad_s + L_ur_c + L_ur_s

# actual_rps from request counter (13th field in snapshot)
actual_rps = (s2c[12] - s1c[12]) / $INTERVAL if len(s2c) > 12 and len(s1c) > 12 else 0

print(f"  run=$RUN actual={actual_rps:.0f} L={Lt*1e6:.1f}us (ua={L_ua*1e6:.1f} ur_c={L_ur_c*1e6:.1f} ad_s={L_ad_s*1e6:.1f} ur_s={L_ur_s*1e6:.1f})")

with open("$RESULTS/latency.csv", "a") as f:
    f.write(f"$WORKLOAD,$NAGLE,$RPS,$RUN,{actual_rps:.0f},{Lt*1e6:.2f},{L_ua*1e6:.2f},{L_ur_c*1e6:.2f},{L_ad_s*1e6:.2f},{L_ur_s*1e6:.2f}\n")

if Lt * 1e6 > 1000:
    sys.exit(1)
PYEOF

        if [ $? -ne 0 ]; then
          echo "Latency > 1000us at RPS=$RPS, moving to next config."
          STOP=1
          break
        fi
      done
      [ $STOP -eq 1 ] && break
    done
  done
done

echo ""
echo "=== ALL DONE ==="
echo "Results: $RESULTS/latency.csv"
