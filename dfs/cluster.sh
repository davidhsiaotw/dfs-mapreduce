#!/usr/bin/env bash

# --- Configuration ---
MACHINES=("orion01" "orion02" "orion03" "orion04" "orion05" "orion06" "orion07" "orion08" "orion09" "orion10" "orion12" "mc01" "mc02" "mc03" "mc04" "mc05" "mc06" "mc07" "mc08" "mc09" "mc10")
BASE_PORT=39040
SERVER_BINARY="server"
CONTROLLER_BINARY="controller"
CONTROLLER_HOST="orion11"
CONTROLLER_PORT=39039
# ---------------------

get_port() {
    local machine=$1
    for i in "${!MACHINES[@]}"; do
        if [[ "${MACHINES[$i]}" == "$machine" ]]; then
            echo $((BASE_PORT + i + 1))
            return
        fi
    done
}

start_controller() {
    echo "🚀 Starting Controller on $CONTROLLER_HOST:$CONTROLLER_PORT..."
    ssh -f "$CONTROLLER_HOST" "nohup $(pwd)/$CONTROLLER_BINARY $CONTROLLER_PORT > $(pwd)/controller.log 2>&1 &"
}

start_server() {
    local machine=$1
    local port=$(get_port "$machine")
    if [ -z "$port" ]; then
        echo "❌ Unknown machine: $machine"
        return
    fi
    echo "  -> Starting Server on $machine:$port ..."
    ssh -f "$machine" "nohup $(pwd)/$SERVER_BINARY $port $CONTROLLER_HOST:$CONTROLLER_PORT > $(pwd)/server_$machine.log 2>&1 &"
}

case "$1" in
    start)
        case "$2" in
            controller)
                start_controller
                ;;
            server)
                if [ -z "$3" ]; then
                    echo "Usage: $0 start server <machine>"
                else
                    start_server "$3"
                fi
                ;;
            all|"")
                start_controller
                sleep 1
                echo "🚀 Starting All Servers..."
                for MACHINE in "${MACHINES[@]}"; do
                    start_server "$MACHINE"
                done
                ;;
            *)
                echo "Usage: $0 start {controller|server <machine>|all}"
                ;;
        esac
        ;;
    stop)
        case "$2" in
            controller)
                echo "🛑 Stopping controller on $CONTROLLER_HOST..."
                ssh "$CONTROLLER_HOST" "pkill -u \$USER -x $CONTROLLER_BINARY"
                ;;
            server)
                if [ -z "$3" ]; then
                    echo "Usage: $0 stop server <machine>"
                else
                    echo "🛑 Stopping server on $3..."
                    ssh "$3" "pkill -u \$USER -x $SERVER_BINARY"
                fi
                ;;
            all|"")
                echo "🛑 Stopping all components..."
                ssh "$CONTROLLER_HOST" "pkill -u \$USER -x $CONTROLLER_BINARY"
                for MACHINE in "${MACHINES[@]}"; do
                    ssh "$MACHINE" "pkill -u \$USER -x $SERVER_BINARY"
                done
                ;;
            *)
                echo "Usage: $0 stop {controller|server <machine>|all}"
                ;;
        esac
        ;;
    status)
        echo "🔍 Checking status..."
        C_COUNT=$(ssh "$CONTROLLER_HOST" "pgrep -u \$USER -x $CONTROLLER_BINARY | wc -l")
        if [ "$C_COUNT" -gt 0 ]; then
            echo "  [RUNNING] Controller on $CONTROLLER_HOST:$CONTROLLER_PORT"
        else
            echo "  [DOWN   ] Controller on $CONTROLLER_HOST:$CONTROLLER_PORT"
        fi

        for MACHINE in "${MACHINES[@]}"; do
            PORT=$(get_port "$MACHINE")
            COUNT=$(ssh "$MACHINE" "pgrep -u \$USER -x $SERVER_BINARY | wc -l")
            if [ "$COUNT" -gt 0 ]; then
                echo "  [RUNNING] Server on $MACHINE:$PORT"
            else
                echo "  [DOWN   ] Server on $MACHINE:$PORT"
            fi
        done
        ;;
    clean)
        echo "🧹 Cleaning storage directories..."
        for MACHINE in "${MACHINES[@]}"; do
            echo "  -> Cleaning on $MACHINE..."
            ssh "$MACHINE" "rm -rf /bigdata/students/whsiao5/$MACHINE"
        done
        echo "✅ All storage directories removed."
        ;;
    *)
        echo "Usage: $0 {start|stop|status|clean}"
        echo "       start controller"
        echo "       start server <machine>"
        echo "       start all"
        echo "       stop controller"
        echo "       stop server <machine>"
        echo "       stop all"
        exit 1
esac
