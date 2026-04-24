#!/bin/bash
# =============================================================================
#  initial_load_parallel.sh
#
#  Author:  Alex Lima
#  Purpose: Automate GoldenGate Initial Load via REST API with controlled
#           parallelism — each table gets its own extract/dist/replicat pipeline.
#
#  Two-hub architecture (WEST GG + EAST GG):
#    Change pipeline : EWEST → trail ew → DPWE → trail dw → RWEST → EAST DB
#    Init pipeline   : EI<N> → trail e<N> → DP<N> → trail d<N> → RI<N> → EAST DB
#                      (one pipeline per table, rolling queue of N concurrent)
#
#  Parallelism model — global rolling queue (INIT_PARALLELISM = 3):
#
#    EXTRACT PHASE (controlled — max 3 concurrent EINITs):
#      Start 3 extracts immediately (EI01+DP01, EI02+DP02, EI03+DP03).
#      Each EINIT reads one table at SCN (source:tables) and self-stops.
#      DPEI starts immediately so data streams to EAST while EINIT writes.
#      As soon as an EINIT self-stops → slot freed → next table's EI/DP starts.
#
#    DELIVERY PHASE (background — no slot consumed):
#      The instant an extract slot completes, its delivery is launched in the
#      background: wait DPEI lag=0, create RI<N>, wait stable, stop RI<N>.
#      Delivery runs concurrently with ongoing extracts.
#
#    FK ORDERING (done-file gate):
#      Level-N deliveries wait for level-(N-1) done markers before starting
#      RI<N> on EAST — child tables are never applied before their parents.
#      The extract queue is not gated — extracts run freely across all levels.
#
#  Process naming (GG 8-char process limit, 2-char trail prefix limit):
#    EI01..EI09  — Initial Load Extracts   (trail prefix e1..e9  on WEST)
#    DP01..DP09  — Initial Load Dist Paths (trail prefix d1..d9  on EAST)
#    RI01..RI09  — Initial Load Replicats
#
#  Example with HR (7 tables, 2 FK levels, 3 slots):
#    Start  : EI01(COUNTRIES) + EI02(DEPARTMENTS) + EI03(EMPLOYEES)
#    Slot 1 : COUNTRIES done → delivery01 bg, EI04(JOBS) starts
#    Slot 2 : DEPARTMENTS done → delivery02 bg, EI05(LOCATIONS) starts
#    Slot 3 : EMPLOYEES done → delivery03 bg, EI06(REGIONS) starts
#    Slot 4 : JOBS done → delivery04 bg, EI07(JOB_HISTORY) starts
#    Slot 5 : LOCATIONS done → delivery05 bg
#    Slot 6 : REGIONS done → delivery06 bg
#    Slot 7 : JOB_HISTORY done → delivery07 bg (gates on level-0 markers)
#    End    : wait all 7 deliveries → Steps 9c + 10
#
#  Usage:
#    ./initial_load_parallel.sh [parallelism]     # parallelism defaults to 3
# =============================================================================

set -euo pipefail

start_time=$(date +%s)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/goldengate_setup_parallel.log"

# ── Parallelism & schema config ───────────────────────────────────────────────
INIT_PARALLELISM=${1:-3}   # Max concurrent EINIT slots
INIT_SCHEMA="HR"
TRAIL_INIT_SIZE_MB=250

# ── Load environment ──────────────────────────────────────────────────────────
source "$SCRIPT_DIR/.env"

# ── Global variables ──────────────────────────────────────────────────────────
GLOBAL_PASS=$OGG_ADMIN_PWD
OGG_USER="oggadmin"
RESPONSE_FILE="$SCRIPT_DIR/response_parallel.json"

# Done-marker dir — level-N delivery jobs poll for level-(N-1) markers
WAVE_SYNC_DIR="/tmp/gg_wavedone_$$"

# ── Process definitions (change pipeline) ─────────────────────────────────────
conn_properties=("WEST:$DOCKER_DB_WEST_IP:localhost" "EAST:$DOCKER_DB_EAST_IP:localhost")
extract_properties=("WEST:EWEST:ew:localhost")
distpath_properties=("WEST:DPWE:ew:localhost:$DOCKER_OGG_EAST_IP:dw")
replicat_properties=("EAST:RWEST:localhost:dw")

# ── Poll settings ─────────────────────────────────────────────────────────────
POLL_INTERVAL=15            # seconds between status checks in all poll loops
POLL_TIMEOUT=1800           # max wait per phase in seconds (30 min)
                            # Each extract slot and delivery slot enforces this
                            # timeout independently.  Raise for very large tables.
CHECKPOINT_TABLE="oggadmin.checkpoints"


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

print_step() {
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "════════════════════════════════════════════════════════════"
}
print_ok()   { echo "  ✔  $1"; }
print_warn() { echo "  ⚠  $1"; }
print_err()  { echo "  ✘  $1" >&2; }
slot_ok()    { echo "  ✔  [Slot $1 | $2] $3"; }
slot_warn()  { echo "  ⚠  [Slot $1 | $2] $3"; }
slot_err()   { echo "  ✘  [Slot $1 | $2] $3" >&2; }

get_ogg_port() {
    case $1 in
        "WEST") ogg_port="9090"; ogg_port_deployment="9091" ;;
        "EAST") ogg_port="8080"; ogg_port_deployment="8081" ;;
        *) echo "Invalid region: $1"; exit 1 ;;
    esac
}

# Slot-number → process/trail name helpers
#
# GoldenGate process name limit : 8 characters  → EI01..EI09, DP01..DP09, RI01..RI09
# GoldenGate trail prefix limit : 2 characters  → e1..e9 (WEST), d1..d9 (EAST)
#
# Each slot gets its own isolated pipeline:
#   EI<N>  (extract, source:tables)  writes trail e<N> on WEST
#   DP<N>  (dist path)               forwards  e<N> → d<N>  to EAST
#   RI<N>  (replicat, non-integrated) reads trail d<N> on EAST
#
# Maximum 9 concurrent slots (limited by single-digit trail suffix).
einit_name()  { printf "EI%02d" "$1"; }
dpei_name()   { printf "DP%02d" "$1"; }
rinit_name()  { printf "RI%02d" "$1"; }
trail_west()  { echo "e${1}"; }   # e1..e9  on WEST GG
trail_east()  { echo "d${1}"; }   # d1..d9  on EAST GG

# api_call <METHOD> <URL> [JSON_BODY]
# Writes the HTTP response body to $RESPONSE_FILE (per-slot temp file set by
# each background subprocess — avoids collisions between parallel jobs).
# Prints errors to stdout+log on non-2xx; pretty-prints JSON on success.
api_call() {
    local method=$1 url=$2 data=${3:-}
    local curl_args=(-s -o "$RESPONSE_FILE" -w "%{http_code}" -k -X "$method" "$url" \
        -H "Content-Type: application/json" -H "Accept: application/json" \
        -u "$OGG_USER:$GLOBAL_PASS")
    [[ -n "$data" ]] && curl_args+=(-d "$data")
    local http_status json_response
    http_status=$(curl "${curl_args[@]}")
    json_response=$(cat "$RESPONSE_FILE")
    if [[ "$http_status" -ne 200 && "$http_status" -ne 201 && "$http_status" -ne 202 ]]; then
        echo "Error: API call failed for $url (HTTP $http_status). Response:" | tee -a "$LOG_FILE"
        echo "$json_response" | tee -a "$LOG_FILE"
    else
        echo "$json_response" | jq '.'
    fi
}

fetch_json() { curl -s -k -u "$OGG_USER:$GLOBAL_PASS" "$1"; }

get_process_status() {
    local kind=$1 name=$2 host=$3 port=$4
    local tmp_file="/tmp/gg_status_${name}_$$.json"
    local http_status
    http_status=$(curl -s -o "$tmp_file" -w "%{http_code}" -k \
        -u "$OGG_USER:$GLOBAL_PASS" \
        "https://$host:$port/services/v2/${kind}/${name}")
    if [[ "$http_status" == "404" ]]; then
        rm -f "$tmp_file"; echo "NOT_FOUND"
    else
        local st
        st=$(jq -r '.response.status // .status // "UNKNOWN"' "$tmp_file" 2>/dev/null || echo "UNKNOWN")
        rm -f "$tmp_file"; echo "$st"
    fi
}

stop_and_delete() {
    local kind=$1 name=$2 host=$3 port=$4
    local status
    status=$(get_process_status "$kind" "$name" "$host" "$port")
    if [[ "$status" == "NOT_FOUND" ]]; then
        print_warn "$name not found — skipping."; return 0
    fi
    local status_up
    status_up=$(echo "$status" | tr '[:lower:]' '[:upper:]')
    if [[ "$status_up" == "ABENDED" ]]; then
        print_warn "$name is ABENDED — skipping stop, proceeding to delete."
    elif [[ "$status_up" != "STOPPED" ]]; then
        echo "  Stopping $name (current: $status)..."
        if [[ "$kind" == "sources" ]]; then
            curl -s -o /dev/null -k -X PATCH \
                -H "Content-Type: application/json" \
                -u "$OGG_USER:$GLOBAL_PASS" \
                -d '{"status":"stopped"}' \
                "https://$host:$port/services/v2/${kind}/${name}"
        else
            curl -s -o /dev/null -k -X POST \
                -H "Content-Type: application/json" \
                -u "$OGG_USER:$GLOBAL_PASS" \
                -d '{"command":"STOP","isReported":false}' \
                "https://$host:$port/services/v2/${kind}/${name}/command"
        fi
        local sw=0
        while [[ $sw -lt 120 ]]; do
            sleep 3; sw=$((sw + 3))
            local s su
            s=$(get_process_status "$kind" "$name" "$host" "$port")
            su=$(echo "$s" | tr '[:lower:]' '[:upper:]')
            print_ok "  $name status: $s"
            [[ "$su" == "STOPPED" || "$su" == "NOT_FOUND" || "$su" == "ABENDED" ]] && break
        done
    fi
    echo "  Deleting $name..."
    api_call "DELETE" "https://$host:$port/services/v2/${kind}/${name}" > /dev/null
    local waited=0
    while [[ $waited -lt 60 ]]; do
        local check
        check=$(get_process_status "$kind" "$name" "$host" "$port")
        [[ "$check" == "NOT_FOUND" ]] && break
        sleep 3; waited=$((waited + 3))
    done
    if [[ $waited -ge 60 ]]; then
        print_err "$name still exists after 60s — cannot continue."; exit 1
    fi
    print_ok "$name deleted."
}

wait_for_stopped() {
    local kind=$1 name=$2 host=$3 port=$4
    local elapsed=0
    while [[ $elapsed -lt $POLL_TIMEOUT ]]; do
        local status STATUS_UP
        status=$(get_process_status "$kind" "$name" "$host" "$port")
        STATUS_UP=$(echo "$status" | tr '[:lower:]' '[:upper:]')
        case "$STATUS_UP" in
            STOPPED)   return 0 ;;
            NOT_FOUND) print_err "$name disappeared — creation likely failed."; exit 1 ;;
            ABENDED)   print_err "$name ABENDED — check GoldenGate WebUI."; exit 1 ;;
        esac
        sleep $POLL_INTERVAL
        elapsed=$((elapsed + POLL_INTERVAL))
    done
    print_err "Timeout: $name did not stop within ${POLL_TIMEOUT}s."; exit 1
}

run_sql_as_sysdba() {
    local container=$1 pdb=$2 sql=$3
    docker exec -u oracle -i "$container" bash -s <<BASHEOF
\$ORACLE_HOME/bin/sqlplus -s "/ as sysdba" <<SQLEOF
SET FEEDBACK OFF ECHO ON
ALTER SESSION SET CONTAINER=$pdb;
$sql
EXIT;
SQLEOF
BASHEOF
}

delete_trail_files() {
    local container=$1; shift
    local trails=("$@")
    for trail in "${trails[@]}"; do
        local count
        count=$(docker exec "$container" bash -c \
            "find /u02/Deployment/var/lib/data/ -maxdepth 1 -name '${trail}*' -type f 2>/dev/null | wc -l")
        count=$(echo "$count" | tr -d '[:space:]')
        if [[ "$count" -eq 0 ]]; then
            print_warn "No trail files for '$trail' in $container — skipping."; continue
        fi
        echo "  Deleting $count trail file(s) for '$trail' in $container..."
        docker exec "$container" bash -c \
            "find /u02/Deployment/var/lib/data/ -maxdepth 1 -name '${trail}*' -type f -delete"
        print_ok "Trail '$trail' files deleted from $container."
    done
}

get_distpath_lag() {
    local name=$1 deployment=$2 host=$3 port=$4
    fetch_json "https://$host:$port/services/$deployment/distsrvr/v2/sources/$name/info" | \
        jq -r '.response.lag // 999'
}

get_replicat_read_pos() {
    local name=$1 host=$2 port=$3
    local json
    json=$(fetch_json "https://$host:$port/services/v2/replicats/$name/info/status")
    local seq offset
    seq=$(echo    "$json" | jq -r '.response.position.sequence // 0')
    offset=$(echo "$json" | jq -r '.response.position.offset   // 0')
    echo "$seq $offset"
}

get_max_trail_seq() {
    local container=$1 trail=$2
    local last_file
    last_file=$(docker exec "$container" bash -c \
        "ls /u02/Deployment/var/lib/data/${trail}* 2>/dev/null | sort | tail -1")
    if [[ -z "$last_file" ]]; then echo "0"; return; fi
    local seq
    seq=$(basename "$last_file" | sed "s/^${trail}0*//" | tr -d '[:space:]')
    echo "${seq:-0}"
}

get_table_list() {
    docker exec -u oracle -i dbWEST bash -s <<BASHEOF
export ORACLE_PASSWORD="$GLOBAL_PASS"
\$ORACLE_HOME/bin/sqlplus -s "oggadmin/\$ORACLE_PASSWORD@//localhost:1521/freepdb1" <<'SQLEOF'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON TRIMSPOOL ON
SELECT table_name
FROM   all_tables
WHERE  owner = UPPER('$INIT_SCHEMA')
  AND  table_name NOT LIKE 'DT\$_%' ESCAPE '\'
ORDER  BY table_name;
EXIT;
SQLEOF
BASHEOF
}

get_fk_deps() {
    docker exec -u oracle -i dbWEST bash -s <<BASHEOF
export ORACLE_PASSWORD="$GLOBAL_PASS"
\$ORACLE_HOME/bin/sqlplus -s "oggadmin/\$ORACLE_PASSWORD@//localhost:1521/freepdb1" <<'SQLEOF'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON TRIMSPOOL ON
SELECT TRIM(p.table_name) || ' ' || TRIM(c.table_name)
FROM   all_constraints c
JOIN   all_constraints p ON c.r_constraint_name = p.constraint_name
                        AND c.r_owner           = p.owner
WHERE  c.owner            = UPPER('$INIT_SCHEMA')
AND    c.constraint_type  = 'R'
AND    c.table_name NOT LIKE 'DT\$_%' ESCAPE '\'
AND    p.table_name NOT LIKE 'DT\$_%' ESCAPE '\'
AND    c.table_name      != p.table_name;
EXIT;
SQLEOF
BASHEOF
}

# compute_load_levels <table1> <table2> ...
#
# Assigns a numeric load level (0, 1, 2, ...) to every table using a
# Bellman-Ford relaxation over the FK dependency graph:
#
#   - All tables start at level 0.
#   - For every FK edge (parent → child): child.level = max(child.level, parent.level + 1)
#   - Repeat until no level changes (converges in at most N-1 iterations).
#
# This guarantees that parent tables always have a strictly lower level than
# their FK children, so deliveries can safely gate on parent-level done markers
# without risk of deadlock or ordering violations.
#
# Output: one "level tablename" line per table, unsorted (caller sorts by level).
compute_load_levels() {
    local all_tables=("$@")
    local n=${#all_tables[@]}
    local i k
    local fk_parents=() fk_children=()
    while IFS=' ' read -r par chi; do
        par=$(echo "$par" | tr -d '[:space:]')
        chi=$(echo "$chi" | tr -d '[:space:]')
        [[ -z "$par" || -z "$chi" || "$par" == "$chi" ]] && continue
        fk_parents+=("$par"); fk_children+=("$chi")
    done < <(get_fk_deps)
    local fk_count=${#fk_parents[@]}
    local levels=()
    for ((i=0; i<n; i++)); do levels[i]=0; done
    local changed=1 iters=0
    while [[ $changed -eq 1 && $iters -lt 20 ]]; do
        changed=0; iters=$((iters+1))
        for ((k=0; k<fk_count; k++)); do
            local par="${fk_parents[$k]}" chi="${fk_children[$k]}"
            local par_lvl=-1
            for ((i=0; i<n; i++)); do
                [[ "${all_tables[$i]}" == "$par" ]] && par_lvl=${levels[$i]} && break
            done
            [[ $par_lvl -eq -1 ]] && continue
            for ((i=0; i<n; i++)); do
                if [[ "${all_tables[$i]}" == "$chi" ]]; then
                    local req=$((par_lvl + 1))
                    if [[ ${levels[$i]} -lt $req ]]; then levels[$i]=$req; changed=1; fi
                    break
                fi
            done
        done
    done
    for ((i=0; i<n; i++)); do echo "${levels[$i]} ${all_tables[$i]}"; done
}

cleanup_parallel_processes() {
    get_ogg_port "WEST"; local wp=$ogg_port
    get_ogg_port "EAST"; local ep=$ogg_port

    echo "  Scanning for leftover RI* processes on EAST..."
    local ri_list
    ri_list=$(fetch_json "https://localhost:$ep/services/v2/replicats" | \
        jq -r '.response.items[]?.name // empty' 2>/dev/null | grep -E '^RI[0-9]+$' || true)
    for name in $ri_list; do
        echo "  Found $name — cleaning up..."
        stop_and_delete "replicats" "$name" "localhost" "$ep"
    done

    echo "  Scanning for leftover DP* processes on WEST..."
    local dp_list
    dp_list=$(fetch_json "https://localhost:$wp/services/v2/sources" | \
        jq -r '.response.items[]?.name // empty' 2>/dev/null | grep -E '^DP[0-9]+$' || true)
    for name in $dp_list; do
        echo "  Found $name — cleaning up..."
        stop_and_delete "sources" "$name" "localhost" "$wp"
    done

    echo "  Scanning for leftover EI* processes on WEST..."
    local ei_list
    ei_list=$(fetch_json "https://localhost:$wp/services/v2/extracts" | \
        jq -r '.response.items[]?.name // empty' 2>/dev/null | grep -E '^EI[0-9]+$' || true)
    for name in $ei_list; do
        echo "  Found $name — cleaning up..."
        stop_and_delete "extracts" "$name" "localhost" "$wp"
    done
}


# ─────────────────────────────────────────────────────────────────────────────
# run_extract_slot <table> <slot> <scn>
#
# EXTRACT PHASE — background job that holds one concurrency slot.
# Slot is freed (job exits) the moment EINIT self-stops.
# DPEI starts immediately so the trail streams to EAST while EINIT writes.
#
#   1. Create EI<slot>  (source:tables, EXTFILE, AS OF SCN)
#   2. Create DP<slot>  (trail e<slot> → d<slot>, starts immediately)
#   3. Wait for EI<slot> to self-stop → exit, slot freed
# ─────────────────────────────────────────────────────────────────────────────
run_extract_slot() {
    local table=$1 slot=$2 scn=$3
    # Each subprocess uses its own RESPONSE_FILE to prevent races between
    # parallel api_call invocations writing to the same file.
    RESPONSE_FILE="/tmp/gg_ext${slot}_$$.json"

    local einit dpei tw te
    einit=$(einit_name "$slot")
    dpei=$(dpei_name  "$slot")
    tw=$(trail_west   "$slot")
    te=$(trail_east   "$slot")
    get_ogg_port "WEST"; local wp=$ogg_port

    slot_ok "$slot" "$table" "EXTRACT -- $einit / $dpei  trails ${tw}->${te}  SCN=$scn"

    # 1. Create EINIT
    slot_ok "$slot" "$table" "Creating $einit..."
    api_call "POST" "https://localhost:$wp/services/v2/extracts/$einit" \
        "{
            \"credentials\": {\"domain\": \"OracleGoldenGate\", \"alias\": \"WEST\"},
            \"status\": \"running\",
            \"encryptionProfile\": \"LocalWallet\",
            \"source\": \"tables\",
            \"config\": [
                \"EXTRACT $einit\",
                \"USERIDALIAS WEST DOMAIN OracleGoldenGate\",
                \"EXTFILE $tw MEGABYTES $TRAIL_INIT_SIZE_MB PURGE\",
                \"TABLE ${INIT_SCHEMA}.${table}; SQLPREDICATE \\\"AS OF SCN $scn\\\";\"
            ]
        }" > /dev/null

    # 2. Create DPEI immediately (streams trail while EINIT writes)
    slot_ok "$slot" "$table" "Creating $dpei (${tw} -> ${te})..."
    api_call "POST" "https://localhost:$wp/services/v2/sources/$dpei" \
        "{
            \"name\": \"$dpei\",
            \"description\": \"Init dist path ${tw} -> ${te}\",
            \"source\": {\"uri\": \"trail://localhost/services/WEST/distsrvr/v2/sources?trail=${tw}\"},
            \"target\": {
                \"uri\": \"ws://${DOCKER_OGG_EAST_IP}:9014/services/v2/targets?trail=${te}\",
                \"authenticationMethod\": {\"domain\": \"Network\", \"alias\": \"oggnet\"},
                \"details\": {
                    \"trail\": {\"seqLength\": 9, \"sizeMB\": $TRAIL_INIT_SIZE_MB},
                    \"compression\": {\"enabled\": true}
                }
            },
            \"begin\": {\"sequence\": 0, \"offset\": 0},
            \"encryptionProfile\": \"LocalWallet\",
            \"status\": \"running\"
        }"

    local dpei_st dpei_st_up
    dpei_st=$(get_process_status "sources" "$dpei" "localhost" "$wp")
    dpei_st_up=$(echo "$dpei_st" | tr '[:lower:]' '[:upper:]')
    case "$dpei_st_up" in
        RUNNING)   slot_ok   "$slot" "$table" "$dpei running." ;;
        STOPPED)   slot_warn "$slot" "$table" "$dpei created but stopped — continuing." ;;
        NOT_FOUND) slot_err  "$slot" "$table" "$dpei creation FAILED."; exit 1 ;;
        ABENDED)   slot_err  "$slot" "$table" "$dpei ABENDED on create."; exit 1 ;;
        *)         slot_warn "$slot" "$table" "$dpei status: $dpei_st" ;;
    esac

    # 3. Wait for EINIT to self-stop — slot freed when this job exits
    slot_ok "$slot" "$table" "Waiting for $einit to self-stop..."
    local elapsed=0
    while [[ $elapsed -lt $POLL_TIMEOUT ]]; do
        local st ST
        st=$(get_process_status "extracts" "$einit" "localhost" "$wp")
        ST=$(echo "$st" | tr '[:lower:]' '[:upper:]')
        case "$ST" in
            STOPPED)   break ;;
            NOT_FOUND) slot_err "$slot" "$table" "$einit disappeared."; exit 1 ;;
            ABENDED)   slot_err "$slot" "$table" "$einit ABENDED."; exit 1 ;;
        esac
        sleep $POLL_INTERVAL; elapsed=$((elapsed + POLL_INTERVAL))
    done
    if [[ $elapsed -ge $POLL_TIMEOUT ]]; then
        slot_err "$slot" "$table" "Timeout waiting for $einit."; exit 1
    fi

    slot_ok "$slot" "$table" "$einit done — trail ${tw} written. Slot free."
    rm -f "$RESPONSE_FILE"
}


# ─────────────────────────────────────────────────────────────────────────────
# run_delivery_slot <table> <slot> [parent_table1 parent_table2 ...]
#
# DELIVERY PHASE — runs entirely in the background (no slot consumed).
# DPEI is already running when this starts.
# parent_tables: tables whose delivery must complete before RI<slot> starts.
#   Gate enforces FK ordering across levels.
# Writes $WAVE_SYNC_DIR/<table>.done on completion.
#
#   [gate]  Wait for parent-level done markers (if any)
#   4.      Wait for DP<slot> lag = 0
#   5.      Create RI<slot>, start it
#   6.      Wait for RI<slot> to stabilise at end of trail
#   7.      Stop RI<slot>
#   [done]  Write done marker
# ─────────────────────────────────────────────────────────────────────────────
run_delivery_slot() {
    local table=$1 slot=$2
    shift 2
    local wait_tables=("$@")   # parent tables (level N-1) whose done markers must exist
    RESPONSE_FILE="/tmp/gg_del${slot}_$$.json"

    local dpei rinit te
    dpei=$(dpei_name  "$slot")
    rinit=$(rinit_name "$slot")
    te=$(trail_east   "$slot")
    get_ogg_port "WEST"; local wp=$ogg_port
    get_ogg_port "EAST"; local ep=$ogg_port

    # Gate: wait for parent-level done markers
    if [[ ${#wait_tables[@]} -gt 0 ]]; then
        slot_ok "$slot" "$table" "DELIVERY -- waiting for parent tables: ${wait_tables[*]}"
        local gated=0
        while [[ $gated -lt $POLL_TIMEOUT ]]; do
            local all_ready=1
            local wt
            for wt in "${wait_tables[@]}"; do
                [[ ! -f "$WAVE_SYNC_DIR/${wt}.done" ]] && all_ready=0 && break
            done
            [[ $all_ready -eq 1 ]] && break
            sleep $POLL_INTERVAL; gated=$((gated + POLL_INTERVAL))
        done
        if [[ $gated -ge $POLL_TIMEOUT ]]; then
            slot_err "$slot" "$table" "Timeout waiting for parent deliveries."; exit 1
        fi
        slot_ok "$slot" "$table" "Parent tables confirmed — proceeding."
    else
        slot_ok "$slot" "$table" "DELIVERY -- $dpei lag wait -> $rinit"
    fi

    # 4. Wait for DPEI lag = 0
    local elapsed=0
    while [[ $elapsed -lt $POLL_TIMEOUT ]]; do
        local dp_st dp_st_up
        dp_st=$(get_process_status "sources" "$dpei" "localhost" "$wp")
        dp_st_up=$(echo "$dp_st" | tr '[:lower:]' '[:upper:]')
        case "$dp_st_up" in
            ABENDED)   slot_err "$slot" "$table" "$dpei ABENDED."; exit 1 ;;
            NOT_FOUND) slot_err "$slot" "$table" "$dpei disappeared."; exit 1 ;;
        esac
        local lag
        lag=$(get_distpath_lag "$dpei" "WEST" "localhost" "$wp")
        slot_ok "$slot" "$table" "$dpei lag=${lag}s"
        [[ "$lag" -eq 0 ]] && break
        sleep $POLL_INTERVAL; elapsed=$((elapsed + POLL_INTERVAL))
    done
    if [[ $elapsed -ge $POLL_TIMEOUT ]]; then
        slot_err "$slot" "$table" "Timeout waiting for $dpei lag=0."; exit 1
    fi
    slot_ok "$slot" "$table" "$dpei caught up — trail ${te} ready on EAST."

    # 5. Create RINIT
    slot_ok "$slot" "$table" "Creating $rinit on EAST (trail $te)..."
    api_call "POST" "https://localhost:$ep/services/v2/replicats/$rinit" \
        "{
            \"description\": \"Init replicat ${INIT_SCHEMA}.${table} via $te\",
            \"config\": [
                \"REPLICAT $rinit\",
                \"USERIDALIAS EAST DOMAIN OracleGoldenGate\",
                \"MAP ${INIT_SCHEMA}.${table}, TARGET ${INIT_SCHEMA}.${table};\"
            ],
            \"credentials\": {\"alias\": \"EAST\"},
            \"mode\": {\"parallel\": false, \"type\": \"nonintegrated\"},
            \"source\": {\"name\": \"$te\"},
            \"checkpoint\": {\"table\": \"$CHECKPOINT_TABLE\"},
            \"status\": \"running\"
        }" > /dev/null

    # 6. Wait for RINIT to stabilise at end of trail
    local max_di_seq
    max_di_seq=$(get_max_trail_seq "oggEAST" "$te")
    slot_ok "$slot" "$table" "$rinit started — waiting to reach trail seq $max_di_seq..."

    local prev_seq="-1"
    elapsed=0
    while [[ $elapsed -lt $POLL_TIMEOUT ]]; do
        local rstatus
        rstatus=$(get_process_status "replicats" "$rinit" "localhost" "$ep")
        if [[ "$(echo "$rstatus" | tr '[:lower:]' '[:upper:]')" == "ABENDED" ]]; then
            slot_err "$slot" "$table" "$rinit ABENDED."; exit 1
        fi
        local pos cur_seq cur_offset
        pos=$(get_replicat_read_pos "$rinit" "localhost" "$ep")
        read -r cur_seq cur_offset <<< "$pos"
        slot_ok "$slot" "$table" "$rinit seq=$cur_seq offset=$cur_offset (target=$max_di_seq)"
        if [[ "$cur_seq" -ge "$max_di_seq" && "$cur_seq" == "$prev_seq" ]]; then
            slot_ok "$slot" "$table" "$rinit stable — all rows applied."; break
        fi
        prev_seq=$cur_seq
        sleep $POLL_INTERVAL; elapsed=$((elapsed + POLL_INTERVAL))
    done
    if [[ $elapsed -ge $POLL_TIMEOUT ]]; then
        slot_err "$slot" "$table" "Timeout waiting for $rinit."; exit 1
    fi

    # 7. Stop RINIT — all rows applied, no more reads needed
    slot_ok "$slot" "$table" "Stopping $rinit..."
    curl -s -o /dev/null -k -X POST \
        -H "Content-Type: application/json" \
        -u "$OGG_USER:$GLOBAL_PASS" \
        -d '{"command":"STOP","isReported":false}' \
        "https://localhost:$ep/services/v2/replicats/$rinit/command"
    sleep 5

    # 8. Stop DPEI — trail fully forwarded (lag=0 confirmed above) and RINIT done
    slot_ok "$slot" "$table" "Stopping $dpei..."
    curl -s -o /dev/null -k -X PATCH \
        -H "Content-Type: application/json" \
        -u "$OGG_USER:$GLOBAL_PASS" \
        -d '{"status":"stopped"}' \
        "https://localhost:$wp/services/v2/sources/$dpei"
    sleep 3

    # Done marker — unblocks child-level deliveries
    echo "done" > "$WAVE_SYNC_DIR/${table}.done"
    slot_ok "$slot" "$table" "DONE. ${INIT_SCHEMA}.${table} fully loaded."
    rm -f "$RESPONSE_FILE"
}


# ─────────────────────────────────────────────────────────────────────────────
# run_all_tables
#
# Global rolling queue: start INIT_PARALLELISM extracts, and as each EINIT
# self-stops free its slot and start the next table's extract immediately.
# Each freed slot also launches its delivery (DPEI lag → RINIT) in background.
# Deliveries are gated by level done-markers to preserve FK ordering.
#
# Globals read : ALL_TABLES_ORDERED[], ALL_TABLE_WAVES[], SCN, INIT_PARALLELISM
# Globals written: slot_num
# ─────────────────────────────────────────────────────────────────────────────
run_all_tables() {
    local n=${#ALL_TABLES_ORDERED[@]}
    [[ $n -eq 0 ]] && return 0

    mkdir -p "$WAVE_SYNC_DIR"
    local t_idx=0

    # Active extract slots
    local ext_pids=() ext_slots=() ext_tables=() ext_waves=()
    # All delivery jobs (background, unlimited)
    local del_pids=() del_labels=()

    # ── Fill initial extract slots ────────────────────────────────────────────
    # Note: all loop variables inside run_all_tables use the _ prefix (e.g. _tbl,
    # _slot, _i) to avoid shadowing the outer-scope variables from the main script.
    # macOS ships bash 3.2 which does not support nameref or local -n; variable
    # shadowing in bash 3.x can silently corrupt values across loop iterations,
    # so the _ prefix ensures uniqueness.
    while [[ $t_idx -lt $n && ${#ext_pids[@]} -lt $INIT_PARALLELISM ]]; do
        local _tbl _wav _slot
        _tbl="${ALL_TABLES_ORDERED[$t_idx]}"
        _wav="${ALL_TABLE_WAVES[$t_idx]}"
        slot_num=$((slot_num + 1)); _slot=$slot_num
        t_idx=$((t_idx + 1))
        print_ok "Slot $_slot | Level $_wav | Extract: ${INIT_SCHEMA}.${_tbl}"
        run_extract_slot "$_tbl" "$_slot" "$SCN" &
        ext_pids+=("$!"); ext_slots+=("$_slot")
        ext_tables+=("$_tbl"); ext_waves+=("$_wav")
    done

    # ── Rolling queue: poll for extract completions ───────────────────────────
    while [[ ${#ext_pids[@]} -gt 0 ]]; do
        sleep 10

        local np=() ns=() nt=() nw=()
        local _i
        for _i in "${!ext_pids[@]}"; do
            local _pid _slot _tbl _wav
            _pid="${ext_pids[$_i]}"
            _slot="${ext_slots[$_i]}"
            _tbl="${ext_tables[$_i]}"
            _wav="${ext_waves[$_i]}"

            if kill -0 "$_pid" 2>/dev/null; then
                # Still running
                np+=("$_pid"); ns+=("$_slot"); nt+=("$_tbl"); nw+=("$_wav")
            else
                # Extract finished
                wait "$_pid"; local _ec=$?
                if [[ $_ec -ne 0 ]]; then
                    print_err "Extract for ${_tbl} (slot ${_slot}) FAILED (exit ${_ec})."
                    local _j
                    for _j in "${!np[@]}"; do kill "${np[$_j]}" 2>/dev/null || true; done
                    for _j in "${!del_pids[@]}"; do kill "${del_pids[$_j]}" 2>/dev/null || true; done
                    exit 1
                fi

                # Build parent-level list for delivery gate (level N waits for level N-1)
                local _parents=()
                if [[ $_wav -gt 0 ]]; then
                    local _pw=$((_wav - 1)) _j
                    for _j in "${!ALL_TABLE_WAVES[@]}"; do
                        if [[ "${ALL_TABLE_WAVES[$_j]}" -eq $_pw ]]; then
                            _parents+=("${ALL_TABLES_ORDERED[$_j]}")
                        fi
                    done
                fi

                # Launch delivery in background (no slot consumed)
                print_ok "Slot $_slot | Level $_wav | Extract done: ${_tbl} — delivery launching in background."
                if [[ ${#_parents[@]} -gt 0 ]]; then
                    run_delivery_slot "$_tbl" "$_slot" "${_parents[@]}" &
                else
                    run_delivery_slot "$_tbl" "$_slot" &
                fi
                del_pids+=("$!"); del_labels+=("${_tbl}(slot${_slot})")

                # Slot free — start next extract immediately
                if [[ $t_idx -lt $n ]]; then
                    local _ntbl _nwav _nslot
                    _ntbl="${ALL_TABLES_ORDERED[$t_idx]}"
                    _nwav="${ALL_TABLE_WAVES[$t_idx]}"
                    slot_num=$((slot_num + 1)); _nslot=$slot_num
                    t_idx=$((t_idx + 1))
                    print_ok "Slot $_nslot | Level $_nwav | Extract: ${INIT_SCHEMA}.${_ntbl}"
                    run_extract_slot "$_ntbl" "$_nslot" "$SCN" &
                    np+=("$!"); ns+=("$_nslot"); nt+=("$_ntbl"); nw+=("$_nwav")
                fi
            fi
        done

        # Replace active lists with survivors + any new extract just launched.
        # Arrays are rebuilt from scratch each poll cycle to avoid index drift.
        ext_pids=(); ext_slots=(); ext_tables=(); ext_waves=()
        if [[ ${#np[@]} -gt 0 ]]; then
            ext_pids=("${np[@]}"); ext_slots=("${ns[@]}")
            ext_tables=("${nt[@]}"); ext_waves=("${nw[@]}")
        fi
    done

    # ── All extracts done — wait for every background delivery ───────────────
    if [[ ${#del_pids[@]} -gt 0 ]]; then
        print_ok "All ${n} extract(s) complete. Waiting for ${#del_pids[@]} delivery job(s)..."
        local _i
        for _i in "${!del_pids[@]}"; do
            wait "${del_pids[$_i]}"; local _ec=$?
            if [[ $_ec -ne 0 ]]; then
                print_err "Delivery ${del_labels[$_i]} FAILED (exit ${_ec})."; exit 1
            fi
            print_ok "Delivery ${del_labels[$_i]} confirmed complete."
        done
    fi

    rm -rf "$WAVE_SYNC_DIR"
    print_ok "All ${n} table(s) from ${INIT_SCHEMA} loaded successfully."
}


# ─────────────────────────────────────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────────────────────────────────────
print_step "CLEANUP: Removing existing pipeline and parallel initial load processes"

echo "--- Change pipeline ---"
for replicat in "${replicat_properties[@]}"; do
    IFS=':' read -r region_name replicat_name ogg_ip replicat_file <<< "$replicat"
    get_ogg_port "$region_name"
    stop_and_delete "replicats" "$replicat_name" "$ogg_ip" "$ogg_port"
done
for dp in "${distpath_properties[@]}"; do
    IFS=':' read -r region_name dp_name extract_file ogg_ip ogg_ip_remote dp_filename <<< "$dp"
    get_ogg_port "$region_name"
    stop_and_delete "sources" "$dp_name" "$ogg_ip" "$ogg_port"
done
for extract in "${extract_properties[@]}"; do
    IFS=':' read -r region_name extract_name extract_file ogg_ip <<< "$extract"
    get_ogg_port "$region_name"
    stop_and_delete "extracts" "$extract_name" "$ogg_ip" "$ogg_port"
done

echo "--- Parallel initial load process cleanup ---"
cleanup_parallel_processes

sleep 3

echo "--- Trail file cleanup ---"
delete_trail_files "oggWEST" "ew" "e1" "e2" "e3" "e4" "e5" "e6" "e7" "e8" "e9"
delete_trail_files "oggEAST" "dw" "d1" "d2" "d3" "d4" "d5" "d6" "d7" "d8" "d9"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Disable FK constraints + truncate all schema tables on TARGET DB
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 3: Disable FK constraints + truncate ${INIT_SCHEMA} tables on TARGET (EAST) DB"

run_sql_as_sysdba "dbEAST" "FREEPDB1" \
    "
BEGIN
  FOR c IN (
    SELECT c.constraint_name, c.table_name
    FROM   all_constraints c
    WHERE  c.owner          = UPPER('${INIT_SCHEMA}')
    AND    c.constraint_type = 'R'
    AND    c.status          = 'ENABLED'
    ORDER  BY c.table_name
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE ${INIT_SCHEMA}.' || c.table_name
                     || ' DISABLE CONSTRAINT '        || c.constraint_name;
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE = -26990 THEN NULL; ELSE RAISE; END IF;
    END;
  END LOOP;
END;
/

DECLARE
  TYPE t_names   IS TABLE OF VARCHAR2(128);
  v_pending      t_names := t_names();
  v_deferred     t_names := t_names();
  v_pass         NUMBER  := 0;
BEGIN
  SELECT table_name BULK COLLECT INTO v_pending
  FROM   all_tables
  WHERE  owner      = UPPER('${INIT_SCHEMA}')
  AND    table_name NOT LIKE 'DT\$_%' ESCAPE '\'
  ORDER  BY table_name;

  WHILE v_pending.COUNT > 0 AND v_pass < 20 LOOP
    v_deferred := t_names();
    FOR i IN 1..v_pending.COUNT LOOP
      BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ${INIT_SCHEMA}.' || v_pending(i);
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -2266 THEN
            v_deferred.EXTEND; v_deferred(v_deferred.COUNT) := v_pending(i);
          ELSE RAISE; END IF;
      END;
    END LOOP;
    v_pending := v_deferred; v_pass := v_pass + 1;
  END LOOP;
END;
/
"
print_ok "FK constraints disabled and all ${INIT_SCHEMA} tables truncated on EAST DB."


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Create change Extract (EWEST)
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 5: Create change Extract (EWEST) on WEST GG"

for extract in "${extract_properties[@]}"; do
    IFS=':' read -r region_name extract_name extract_file ogg_ip <<< "$extract"
    get_ogg_port "$region_name"
    api_call "POST" "https://$ogg_ip:$ogg_port/services/v2/extracts/$extract_name" \
        '{
            "description": "Extract Demo",
            "config": [
                "EXTRACT '$extract_name'",
                "ENCRYPTTRAIL AES256",
                "EXTTRAIL '$extract_file'",
                "USERIDALIAS '$region_name' DOMAIN OracleGoldenGate",
                "TRANLOGOPTIONS EXCLUDETAG 00",
                "DDL INCLUDE MAPPED",
                "TABLE HR.*;"
            ],
            "source": "tranlogs",
            "credentials": {"alias": "'$region_name'"},
            "registration": "default",
            "begin": "now",
            "targets": [{"name": "'$extract_file'", "sizeMB": 1}],
            "critical": false,
            "managedProcessSettings": "'$region_name'-profile",
            "encryptionProfile": "LocalWallet",
            "status": "running"
        }'
    print_ok "$extract_name created and started."
done


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5b — Copy wallet + create change Dist Path (DPWE)
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 5b: Copy wallet + create change Dist Path (DPWE) on WEST GG"

docker cp oggWEST:/u02/Deployment/var/lib/wallet/cwallet.sso .
docker cp ./cwallet.sso oggEAST:/u02/Deployment/var/lib/wallet/cwallet.sso
docker exec -u 0 oggEAST chown 1001:root /u02/Deployment/var/lib/wallet/cwallet.sso
print_ok "Wallet copied to EAST."

for dp in "${distpath_properties[@]}"; do
    IFS=':' read -r region_name dp_name extract_file ogg_ip ogg_ip_remote dp_filename <<< "$dp"
    get_ogg_port "$region_name"
    api_call "POST" "https://$ogg_ip:$ogg_port/services/v2/sources/$dp_name" \
        '{
            "name": "'$dp_name'",
            "description": "DIST PATH '$dp_name'",
            "source": {"uri": "trail://'$ogg_ip'/services/'$region_name'/distsrvr/v2/sources?trail='$extract_file'"},
            "target": {
                "uri": "ws://'$ogg_ip_remote':9014/services/v2/targets?trail='$dp_filename'",
                "authenticationMethod": {"domain": "Network", "alias": "oggnet"},
                "details": {
                    "trail": {"seqLength": 9, "sizeMB": 1},
                    "compression": {"enabled": true}
                }
            },
            "begin": {"sequence": 0, "offset": 0},
            "encryptionProfile": "LocalWallet",
            "status": "running"
        }'
    print_ok "$dp_name created and started."
done


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Create change Replicat (RWEST) — stopped
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 6: Create change Replicat (RWEST) on EAST GG — stopped"

for replicat in "${replicat_properties[@]}"; do
    IFS=':' read -r region_name replicat_name ogg_ip replicat_file <<< "$replicat"
    get_ogg_port "$region_name"
    api_call "POST" "https://$ogg_ip:$ogg_port/services/v2/replicats/$replicat_name" \
        '{
            "description": "Replicat Demo",
            "config": [
                "REPLICAT '$replicat_name'",
                "USERIDALIAS '$region_name' DOMAIN OracleGoldenGate",
                "DDL INCLUDE MAPPED",
                "MAP hr.*, TARGET hr.*;"
            ],
            "credentials": {"alias": "'$region_name'"},
            "mode": {"parallel": true, "type": "nonintegrated"},
            "source": {"name": "'$replicat_file'"},
            "checkpoint": {"table": "oggadmin.checkpoints"},
            "managedProcessSettings": "'$region_name'-profile"
        }'
    print_ok "$replicat_name created (stopped — will start at Step 10)."
done


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — Capture SCN from source (WEST) DB
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 7: Capture SCN from source (WEST) DB"

SCN=$(docker exec -u oracle -i dbWEST bash -s <<BASHEOF
export ORACLE_PASSWORD="$GLOBAL_PASS"
\$ORACLE_HOME/bin/sqlplus -s "oggadmin/\$ORACLE_PASSWORD@//localhost:1521/freepdb1" <<'SQLEOF'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON TRIMSPOOL ON
SELECT NVL(
    (SELECT TO_CHAR(MIN(T.START_SCN))
     FROM   gv\$transaction T
     INNER JOIN gv\$session S ON S.SADDR = T.SES_ADDR
     WHERE  T.STATUS = 'ACTIVE'),
    (SELECT TO_CHAR(current_scn) FROM v\$database)
) FROM dual;
EXIT;
SQLEOF
BASHEOF
)
SCN=$(echo "$SCN" | tr -d '[:space:]')
if [[ -z "$SCN" || ! "$SCN" =~ ^[0-9]+$ ]]; then
    print_err "Could not retrieve a valid SCN. Got: '$SCN'"; exit 1
fi
print_ok "Captured SCN: $SCN"
echo "$(date): Parallel initial load SCN = $SCN" >> "$LOG_FILE"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — Discover tables + FK levels, run global rolling-queue load
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 8: Discover tables, compute FK load order, run parallel initial load"

# Discover tables
ALL_TABLES=()
while IFS= read -r tbl; do
    tbl=$(echo "$tbl" | tr -d '[:space:]')
    [[ -n "$tbl" ]] && ALL_TABLES+=("$tbl")
done < <(get_table_list)
if [[ ${#ALL_TABLES[@]} -eq 0 ]]; then
    print_err "No tables found in schema $INIT_SCHEMA — aborting."; exit 1
fi
print_ok "Found ${#ALL_TABLES[@]} table(s) in $INIT_SCHEMA."

# Compute FK dependency levels
print_ok "Querying FK dependency graph from source DB..."
LEVEL_ENTRIES=()
while IFS= read -r entry; do
    [[ -n "$entry" ]] && LEVEL_ENTRIES+=("$entry")
done < <(compute_load_levels "${ALL_TABLES[@]}" | sort -n -k1,1)

# Build ordered arrays — strip all whitespace from level and table name
# to guard against sqlplus carriage-return or trailing-space artifacts
ALL_TABLES_ORDERED=()
ALL_TABLE_WAVES=()
for entry in "${LEVEL_ENTRIES[@]}"; do
    local_lv=$(echo "${entry%% *}" | tr -d '[:space:]')
    local_tbl=$(echo "${entry#* }"  | tr -d '[:space:]')
    [[ -z "$local_lv" || -z "$local_tbl" ]] && continue
    ALL_TABLE_WAVES+=("$local_lv")
    ALL_TABLES_ORDERED+=("$local_tbl")
done

if [[ ${#ALL_TABLES_ORDERED[@]} -eq 0 ]]; then
    print_err "FK level computation returned no tables — aborting."; exit 1
fi

MAX_WAVE=0
for wav in "${ALL_TABLE_WAVES[@]}"; do
    [[ $wav -gt $MAX_WAVE ]] && MAX_WAVE=$wav
done

# Print load plan
echo ""
print_ok "FK-aware load plan: ${#ALL_TABLES_ORDERED[@]} table(s), $((MAX_WAVE+1)) level(s), $INIT_PARALLELISM concurrent extract slot(s):"
local_w=0
while [[ $local_w -le $MAX_WAVE ]]; do
    line="    Level $local_w:"
    local_j=0
    while [[ $local_j -lt ${#ALL_TABLES_ORDERED[@]} ]]; do
        if [[ "${ALL_TABLE_WAVES[$local_j]}" -eq $local_w ]]; then
            line="$line ${ALL_TABLES_ORDERED[$local_j]}"
        fi
        local_j=$((local_j + 1))
    done
    echo "$line"
    local_w=$((local_w + 1))
done
echo ""
echo "    Queue order: ${ALL_TABLES_ORDERED[*]}"
echo "    Extracts roll at $INIT_PARALLELISM concurrent.  Deliveries run in background."
echo "    Level-N deliveries gate on Level-(N-1) done markers before starting RINIT."
echo ""

# Run the global rolling queue
slot_num=0
run_all_tables

print_ok "All ${#ALL_TABLES_ORDERED[@]} table(s) from $INIT_SCHEMA loaded successfully."


# ─────────────────────────────────────────────────────────────────────────────
# STEP 9c — Re-enable FK constraints on target DB
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 9c: Re-enable FK constraints on TARGET (EAST) DB"

run_sql_as_sysdba "dbEAST" "FREEPDB1" \
    "
BEGIN
  FOR c IN (
    SELECT c.constraint_name, c.table_name
    FROM   all_constraints c
    WHERE  c.owner = UPPER('${INIT_SCHEMA}')
    AND    c.constraint_type = 'R'
    AND    c.status = 'DISABLED'
    ORDER BY c.table_name
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE ${INIT_SCHEMA}.' || c.table_name
                     || ' ENABLE CONSTRAINT ' || c.constraint_name;
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE = -26990 THEN NULL; ELSE RAISE; END IF;
    END;
  END LOOP;
END;
/
"
print_ok "FK constraints re-enabled on EAST DB."


# ─────────────────────────────────────────────────────────────────────────────
# STEP 10 — Start RWEST at captured SCN via commands/execute
#            RWEST has been stopped since Step 6; dw trail accumulated since Step 5b.
#            commands/execute positions the replicat AT the snapshot SCN so CDC
#            changes predating the initial load are skipped (avoids ORA-00001).
# ─────────────────────────────────────────────────────────────────────────────
print_step "STEP 10: Start change Replicat (RWEST) at SCN $SCN"

for replicat in "${replicat_properties[@]}"; do
    IFS=':' read -r region_name replicat_name ogg_ip replicat_file <<< "$replicat"
    get_ogg_port "$region_name"

    print_ok "Starting $replicat_name at SCN $SCN via commands/execute..."
    api_call "POST" "https://$ogg_ip:$ogg_port/services/v2/commands/execute" \
        "{
            \"\$schema\": \"ogg:command\",
            \"name\": \"start\",
            \"processType\": \"replicat\",
            \"processName\": \"$replicat_name\",
            \"at\": $SCN,
            \"filterDuplicates\": true
        }"

    local_w=0
    while [[ $local_w -lt 60 ]]; do
        sleep 3; local_w=$((local_w + 3))
        FINAL_STATUS=$(get_process_status "replicats" "$replicat_name" "$ogg_ip" "$ogg_port")
        FINAL_UP=$(echo "$FINAL_STATUS" | tr '[:lower:]' '[:upper:]')
        print_ok "$replicat_name status: $FINAL_STATUS"
        [[ "$FINAL_UP" == "RUNNING" ]] && break
        [[ "$FINAL_UP" == "ABENDED" ]] && print_err "$replicat_name ABENDED." && exit 1
    done
    if [[ "$FINAL_UP" != "RUNNING" ]]; then
        print_err "$replicat_name did not reach RUNNING within 60s."; exit 1
    fi
    print_ok "$replicat_name is RUNNING from SCN $SCN."
done


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
end_time=$(date +%s)
elapsed=$((end_time - start_time))

get_ogg_port "WEST"; west_port=$ogg_port
get_ogg_port "EAST"; east_port=$ogg_port

status_ewest=$(get_process_status "extracts"  "EWEST" "localhost" "$west_port")
status_dpwe=$(get_process_status  "sources"   "DPWE"  "localhost" "$west_port")
status_rwest=$(get_process_status "replicats" "RWEST" "localhost" "$east_port")

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  ✔  Parallel Initial Load completed in ${elapsed}s"
echo "════════════════════════════════════════════════════════════"
echo "  Tables loaded    : ${#ALL_TABLES_ORDERED[@]} (${ALL_TABLES_ORDERED[*]})"
echo "  FK levels        : $((MAX_WAVE+1))"
echo "  Parallelism used : $INIT_PARALLELISM concurrent extract slots"
echo "  Slots used total : $slot_num"
echo "  Init SCN         : $SCN"
echo ""
echo "  Change Extract   : EWEST  ($status_ewest -- trail ew)"
echo "  Change Dist Path : DPWE   ($status_dpwe -- ew -> dw)"
echo "  Change Replicat  : RWEST  ($status_rwest)"
echo "  Init processes   : $(einit_name 1)..$(einit_name $slot_num) / $(dpei_name 1)..$(dpei_name $slot_num) / $(rinit_name 1)..$(rinit_name $slot_num)  (all stopped)"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  GG WEST WebUI : https://localhost:9090"
echo "  GG EAST WebUI : https://localhost:8080"
echo ""
