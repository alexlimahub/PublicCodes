#!/bin/bash
# =============================================================================
#  init_cleanup.sh
#
#  Author:  Alex Lima - GoldenGate Product Manager
#  Purpose: Stop and delete all initial load processes and trail files
#           left behind by initial_load.sh or initial_load_parallel.sh.
#
#  Cleans up:
#    Serial init processes  : EINIT, DPEI, RINIT
#    Parallel init processes: EI01..EI99+, DP01..DP99+, RI01..RI99+ (any found via API scan)
#    Trail files            : all 2-char letter-pair trails on WEST (except ew)
#                             and on EAST (except dw) — covers serial (ei/di) and
#                             all parallel init trails (aa, ab, ac, ... up to 674 slots)
#
#  Does NOT touch the change pipeline:
#    EWEST, DPWE, RWEST and their trails (ew, dw) are left untouched.
#
#  Usage:
#    ./init_cleanup.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/goldengate_setup.log"
RESPONSE_FILE="$SCRIPT_DIR/response_cleanup.json"

source "$SCRIPT_DIR/.env"

GLOBAL_PASS=$OGG_ADMIN_PWD
OGG_USER="oggadmin"


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

get_ogg_port() {
    case $1 in
        "WEST") ogg_port="9090" ;;
        "EAST") ogg_port="8080" ;;
        *) echo "Invalid region: $1"; exit 1 ;;
    esac
}

api_call() {
    local method=$1 url=$2 data=${3:-}
    local curl_args=(-s -o "$RESPONSE_FILE" -w "%{http_code}" -k -X "$method" "$url" \
        -H "Content-Type: application/json" \
        -H "Accept: application/json" \
        -u "$OGG_USER:$GLOBAL_PASS")
    [[ -n "$data" ]] && curl_args+=(-d "$data")
    local http_status json_response
    http_status=$(curl "${curl_args[@]}")
    json_response=$(cat "$RESPONSE_FILE")
    if [[ "$http_status" -ne 200 && "$http_status" -ne 201 && "$http_status" -ne 202 ]]; then
        echo "Error: API call failed for $url (HTTP $http_status). Response:" | tee -a "$LOG_FILE"
        echo "$json_response" | tee -a "$LOG_FILE"
    fi
}

fetch_json() { curl -s -k -u "$OGG_USER:$GLOBAL_PASS" "$1"; }

get_process_status() {
    local kind=$1 name=$2 host=$3 port=$4
    local http_status
    http_status=$(curl -s -o "$RESPONSE_FILE" -w "%{http_code}" -k \
        -u "$OGG_USER:$GLOBAL_PASS" \
        "https://$host:$port/services/v2/${kind}/${name}")
    if [[ "$http_status" == "404" ]]; then
        echo "NOT_FOUND"
    else
        jq -r '.response.status // .status // "UNKNOWN"' "$RESPONSE_FILE" 2>/dev/null || echo "UNKNOWN"
    fi
}

# stop_and_delete <kind> <name> <host> <port>
stop_and_delete() {
    local kind=$1 name=$2 host=$3 port=$4
    local status
    status=$(get_process_status "$kind" "$name" "$host" "$port")

    if [[ "$status" == "NOT_FOUND" ]]; then
        print_warn "$name not found — skipping."
        return 0
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
            print_ok "$name: $s"
            [[ "$su" == "STOPPED" || "$su" == "NOT_FOUND" || "$su" == "ABENDED" ]] && break
        done
    fi

    echo "  Deleting $name..."
    api_call "DELETE" "https://$host:$port/services/v2/${kind}/${name}"

    # GoldenGate deletes asynchronously — poll until NOT_FOUND
    local waited=0
    while [[ $waited -lt 60 ]]; do
        [[ "$(get_process_status "$kind" "$name" "$host" "$port")" == "NOT_FOUND" ]] && break
        sleep 3; waited=$((waited + 3))
    done
    if [[ $waited -ge 60 ]]; then
        print_err "$name still exists after 60s."; exit 1
    fi
    print_ok "$name deleted."
}

# delete_trail_files <container> <trail_abbrev> [<trail_abbrev2> ...]
delete_trail_files() {
    local container=$1; shift
    local trails=("$@")
    for trail in "${trails[@]}"; do
        local count
        count=$(docker exec "$container" bash -c \
            "find /u02/Deployment/var/lib/data/ -maxdepth 1 -name '${trail}[0-9]*' -type f 2>/dev/null | wc -l")
        count=$(echo "$count" | tr -d '[:space:]')
        if [[ "$count" -eq 0 ]]; then
            print_warn "No trail files for '$trail' in $container — skipping."
            continue
        fi
        echo "  Deleting $count trail file(s) for '$trail' in $container..."
        docker exec "$container" bash -c \
            "find /u02/Deployment/var/lib/data/ -maxdepth 1 -name '${trail}[0-9]*' -type f -delete"
        print_ok "Trail '$trail' files deleted from $container."
    done
}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Stop and delete serial init processes (EINIT, DPEI, RINIT)
# ─────────────────────────────────────────────────────────────────────────────
print_step "Serial init processes: EINIT / DPEI / RINIT"

get_ogg_port "WEST"; wp=$ogg_port
get_ogg_port "EAST"; ep=$ogg_port

stop_and_delete "replicats" "RINIT" "localhost" "$ep"
stop_and_delete "sources"   "DPEI"  "localhost" "$wp"
stop_and_delete "extracts"  "EINIT" "localhost" "$wp"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Stop and delete parallel init processes (EI*, DP*, RI*)
#           Scans the GG API dynamically — handles any slot count
# ─────────────────────────────────────────────────────────────────────────────
print_step "Parallel init processes: RI* / DP* / EI* (dynamic scan)"

echo "  Scanning for RI* replicats on EAST..."
ri_list=$(fetch_json "https://localhost:$ep/services/v2/replicats" | \
    jq -r '.response.items[]?.name // empty' 2>/dev/null | grep -E '^RI[0-9]+$' || true)
for name in $ri_list; do
    stop_and_delete "replicats" "$name" "localhost" "$ep"
done
[[ -z "$ri_list" ]] && print_warn "No RI* processes found on EAST."

echo "  Scanning for DP* sources on WEST..."
dp_list=$(fetch_json "https://localhost:$wp/services/v2/sources" | \
    jq -r '.response.items[]?.name // empty' 2>/dev/null | grep -E '^DP[0-9]+$' || true)
for name in $dp_list; do
    stop_and_delete "sources" "$name" "localhost" "$wp"
done
[[ -z "$dp_list" ]] && print_warn "No DP* processes found on WEST."

echo "  Scanning for EI* extracts on WEST..."
ei_list=$(fetch_json "https://localhost:$wp/services/v2/extracts" | \
    jq -r '.response.items[]?.name // empty' 2>/dev/null | grep -E '^EI[0-9]+$' || true)
for name in $ei_list; do
    stop_and_delete "extracts" "$name" "localhost" "$wp"
done
[[ -z "$ei_list" ]] && print_warn "No EI* processes found on WEST."


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Delete init trail files
#           Serial   : ei (WEST), di (EAST)
#           Parallel : any 2-char letter-pair trail (aa, ab, ...) written by
#                      a previous run — up to 674 slots supported.
#           Preserved: ew (WEST change pipeline), dw (EAST change pipeline)
# ─────────────────────────────────────────────────────────────────────────────
print_step "Init trail file cleanup"

# Delete all 2-char lowercase trails on WEST except the change-pipeline trail 'ew'.
# This covers serial init trail 'ei' and all parallel init trails (aa, ab, ...).
echo "  WEST GG — deleting all 2-char init trail files (except ew)..."
docker exec oggWEST bash -c "
    cnt=\$(find /u02/Deployment/var/lib/data/ -maxdepth 1 -type f \
         -name '[a-z][a-z]*' ! -name 'ew*' 2>/dev/null | wc -l | tr -d '[:space:]')
    if [[ \"\$cnt\" -gt 0 ]]; then
        find /u02/Deployment/var/lib/data/ -maxdepth 1 -type f \
             -name '[a-z][a-z]*' ! -name 'ew*' -delete
        echo \"    Deleted \$cnt trail file(s).\"
    else
        echo '    No init trail files found — skipping.'
    fi
"

# Delete all 2-char lowercase trails on EAST except the change-pipeline trail 'dw'.
# This covers serial init trail 'di' and all parallel init trails (aa, ab, ...).
echo "  EAST GG — deleting all 2-char init trail files (except dw)..."
docker exec oggEAST bash -c "
    cnt=\$(find /u02/Deployment/var/lib/data/ -maxdepth 1 -type f \
         -name '[a-z][a-z]*' ! -name 'dw*' 2>/dev/null | wc -l | tr -d '[:space:]')
    if [[ \"\$cnt\" -gt 0 ]]; then
        find /u02/Deployment/var/lib/data/ -maxdepth 1 -type f \
             -name '[a-z][a-z]*' ! -name 'dw*' -delete
        echo \"    Deleted \$cnt trail file(s).\"
    else
        echo '    No init trail files found — skipping.'
    fi
"


# ─────────────────────────────────────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  ✔  Init cleanup complete"
echo "════════════════════════════════════════════════════════════"
echo "  Change pipeline (EWEST / DPWE / RWEST) left untouched."
echo ""
