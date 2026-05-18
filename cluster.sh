#!/usr/bin/env bash

# --- Configuration ---
MACHINES=("orion01" "orion02" "orion03" "orion04" "orion05" "orion06" "orion07" "orion08" "orion09" "orion10" "orion12" "mc01" "mc02" "mc03" "mc04" "mc05" "mc06" "mc07" "mc08" "mc09" "mc10")

# DFS Config
DFS_CONTROLLER_HOST="orion11"
DFS_CONTROLLER_PORT=39039
DFS_BASE_PORT=39040
DFS_BIN_DIR="$(pwd)/dfs/bin"
DFS_CONTROLLER_BINARY="controller"
DFS_SERVER_BINARY="server"
WAL_FILE="controller_metadata.wal"

# MapReduce Config
MR_MASTER_HOST="orion11"
MR_MASTER_PORT=39079
MR_BASE_PORT=39080
MR_BIN_DIR="$(pwd)/mapreduce/bin"
MR_MASTER_BINARY="master"
MR_WORKER_BINARY="worker"

# Student ID / Storage
STUDENT_ID="whsiao5"
BASE_STORAGE="/bigdata/students/$STUDENT_ID"
DFS_DIR="$BASE_STORAGE/mydfs"
MR_DIR="$BASE_STORAGE/mr"
# ---------------------

get_dfs_port() {
    local machine=$1
    for i in "${!MACHINES[@]}"; do
        if [[ "${MACHINES[$i]}" == "$machine" ]]; then
            echo $((DFS_BASE_PORT + i))
            return
        fi
    done
}

get_mr_port() {
    local machine=$1
    for i in "${!MACHINES[@]}"; do
        if [[ "${MACHINES[$i]}" == "$machine" ]]; then
            echo $((MR_BASE_PORT + i))
            return
        fi
    done
}

# --- DFS Commands ---
dfs_start() {
    case "$1" in
        controller)
            echo "🚀 Starting DFS Controller on $DFS_CONTROLLER_HOST:$DFS_CONTROLLER_PORT..."
            ssh -f "$DFS_CONTROLLER_HOST" "nohup $DFS_BIN_DIR/$DFS_CONTROLLER_BINARY $DFS_CONTROLLER_PORT > $(pwd)/dfs_controller.log 2>&1 &"
            ;;
        server)
            local machine=$2
            if [[ -z "$machine" ]]; then
                echo "Error: Missing node name. Usage: ./cluster.sh dfs start server <node>"
                return 1
            fi
            local port=$(get_dfs_port "$machine")
            echo "  -> Starting DFS Server on $machine:$port ..."
            ssh -f "$machine" "nohup $DFS_BIN_DIR/$DFS_SERVER_BINARY $port $DFS_CONTROLLER_HOST:$DFS_CONTROLLER_PORT > $(pwd)/dfs_server_$machine.log 2>&1 &"
            ;;
        all)
            dfs_start controller
            sleep 1
            for MACHINE in "${MACHINES[@]}"; do
                dfs_start server "$MACHINE"
            done
            ;;
    esac
}

dfs_stop() {
    case "$1" in
        controller)
            echo "🛑 Stopping DFS Controller..."
            ssh "$DFS_CONTROLLER_HOST" "pkill -u $USER -x $DFS_CONTROLLER_BINARY"
            ;;
        server)
            local machine=$2
            echo "🛑 Stopping DFS Server on $machine..."
            ssh "$machine" "pkill -u $USER -x $DFS_SERVER_BINARY"
            ;;
        all)
            dfs_stop controller
            for MACHINE in "${MACHINES[@]}"; do
                dfs_stop server "$MACHINE"
            done
            ;;
    esac
}

dfs_clean() {
    echo "🧹 Cleaning DFS storage..."
    for MACHINE in "${MACHINES[@]}"; do
        echo "  -> Cleaning DFS on $MACHINE..."
        ssh "$MACHINE" "rm -rf $DFS_DIR/*"
    done
    echo "  -> Cleaning WAL on $DFS_CONTROLLER_HOST..."
    ssh "$DFS_CONTROLLER_HOST" "rm -f $BASE_STORAGE/$WAL_FILE"
    echo "✅ DFS Cleanup complete."
}

# --- MapReduce Commands ---
mr_start() {
    case "$1" in
        master)
            echo "🚀 Starting MR Master on $MR_MASTER_HOST:$MR_MASTER_PORT..."
            ssh -f "$MR_MASTER_HOST" "nohup $MR_BIN_DIR/$MR_MASTER_BINARY $MR_MASTER_PORT $DFS_CONTROLLER_HOST:$DFS_CONTROLLER_PORT > $(pwd)/mr_master.log 2>&1 &"
            ;;
        worker)
            local machine=$2
            local port=$(get_mr_port "$machine")
            echo "  -> Starting MR Worker on $machine:$port ..."
            ssh -f "$machine" "nohup $MR_BIN_DIR/$MR_WORKER_BINARY $port $MR_MASTER_HOST:$MR_MASTER_PORT $DFS_CONTROLLER_HOST:$DFS_CONTROLLER_PORT $DFS_BIN_DIR/client > $(pwd)/mr_worker_$machine.log 2>&1 &"
            ;;
        all)
            mr_start master
            sleep 1
            for MACHINE in "${MACHINES[@]}"; do
                mr_start worker "$MACHINE"
            done
            ;;
    esac
}

mr_stop() {
    case "$1" in
        master)
            echo "🛑 Stopping MR Master..."
            ssh "$MR_MASTER_HOST" "pkill -u $USER -x $MR_MASTER_BINARY || true"
            ;;
        worker)
            local machine=$2
            if [[ -z "$machine" ]]; then
                echo "Error: Missing node name. Usage: ./cluster.sh mr stop worker <node>"
                return 1
            fi
            echo "🛑 Stopping MR Worker on $machine..."
            ssh "$machine" "pkill -u $USER -x $MR_WORKER_BINARY || true"
            ;;
        all)
            mr_stop master
            for MACHINE in "${MACHINES[@]}"; do
                mr_stop worker "$MACHINE"
            done
            ;;
    esac
}

mr_clean() {
    for MACHINE in "${MACHINES[@]}"; do
        echo "Cleaning MapReduce directories on $MACHINE..."
        ssh "$MACHINE" "rm -rf $MR_DIR/*"
    done
    echo "✅ MapReduce Cleanup complete."
}

# --- Shared Commands ---
status() {
    echo "🔍 System Status:"
    
    # DFS
    local c_status=$(ssh "$DFS_CONTROLLER_HOST" "pgrep -u $USER -x $DFS_CONTROLLER_BINARY | wc -l")
    [[ $c_status -gt 0 ]] && echo "  [DFS CTRL ] [RUNNING] $DFS_CONTROLLER_HOST:$DFS_CONTROLLER_PORT" || echo "  [DFS CTRL ] [DOWN   ] $DFS_CONTROLLER_HOST:$DFS_CONTROLLER_PORT"
    
    # MR Master
    local m_status=$(ssh "$MR_MASTER_HOST" "pgrep -u $USER -x $MR_MASTER_BINARY | wc -l")
    [[ $m_status -gt 0 ]] && echo "  [MR MASTER] [RUNNING] $MR_MASTER_HOST:$MR_MASTER_PORT" || echo "  [MR MASTER] [DOWN   ] $MR_MASTER_HOST:$MR_MASTER_PORT"

    echo "--- Workers ---"
    for MACHINE in "${MACHINES[@]}"; do
        local s_port=$(get_dfs_port "$MACHINE")
        local w_port=$(get_mr_port "$MACHINE")
        local s_status=$(ssh "$MACHINE" "pgrep -u $USER -x $DFS_SERVER_BINARY | wc -l")
        local w_status=$(ssh "$MACHINE" "pgrep -u $USER -x $MR_WORKER_BINARY | wc -l")
        
        local line="  [$MACHINE] "
        [[ $s_status -gt 0 ]] && line+="DFS:OK ($s_port)   " || line+="DFS:DOWN ($s_port) "
        [[ $w_status -gt 0 ]] && line+="MR:OK ($w_port)" || line+="MR:DOWN ($w_port)"
        echo "$line"
    done
}

# --- Main Logic ---
case "$1" in
    dfs)
        shift
        case "$1" in
            start) dfs_start "${2:-all}" "$3" ;;
            stop)  dfs_stop "${2:-all}" "$3" ;;
            clean) dfs_clean ;;
            *) echo "Usage: $0 dfs {start|stop|clean} [{controller|server <node>|all}]" ;;
        esac
        ;;
    mr)
        shift
        case "$1" in
            start) mr_start "${2:-all}" "$3" ;;
            stop)  mr_stop "${2:-all}" "$3" ;;
            clean) mr_clean ;;
            *) echo "Usage: $0 mr {start|stop|clean} [{master|worker <node>|all}]" ;;
        esac
        ;;
    status) status ;;
    *)
        echo "Usage: $0 {dfs|mr} {start|stop|clean} ..."
        echo "       $0 status"
        exit 1
        ;;
esac

