#!/usr/bin/env bash
set -e

# Lấy đường dẫn thư mục project (nơi chứa run_airflow.sh)
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

# venv nằm trong project
VENV_PATH="$PROJECT_ROOT/venv"
export AIRFLOW_HOME="$PROJECT_ROOT/airflow_home"

echo "[*] PROJECT_ROOT = $PROJECT_ROOT"
echo "[*] VENV_PATH    = $VENV_PATH"
echo "[*] AIRFLOW_HOME = $AIRFLOW_HOME"
echo

# --- 1. Kích hoạt venv ---
if [ ! -d "$VENV_PATH" ]; then
  echo "(!) Không tìm thấy virtualenv ở $VENV_PATH"
  exit 1
fi

# shellcheck source=/dev/null
source "$VENV_PATH/bin/activate"

# --- 2. Khởi tạo / migrate Airflow DB nếu chưa có ---
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
  echo "[*] Chưa thấy database Airflow, đang migrate lần đầu..."
  mkdir -p "$AIRFLOW_HOME"
  airflow db migrate
else
  echo "[*] Đã có database Airflow, bỏ qua bước migrate."
fi

echo
echo "[*] Khởi động Airflow webserver (port 8080) & scheduler..."

airflow webserver -p 8080 > "$AIRFLOW_HOME/webserver.log" 2>&1 &
WEB_PID=$!
echo "  - Webserver PID  = $WEB_PID (log: $AIRFLOW_HOME/webserver.log)"

airflow scheduler > "$AIRFLOW_HOME/scheduler.log" 2>&1 &
SCH_PID=$!
echo "  - Scheduler PID  = $SCH_PID (log: $AIRFLOW_HOME/scheduler.log)"

echo
echo "✅ Airflow đang chạy."
echo "   Mở: http://localhost:8080"
echo "   Username mặc định: admin"
echo "   Password: xem trong file:"
echo "      $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
echo
echo "   Hoặc grep trong log webserver:"
echo "      grep -i \"password\" \"$AIRFLOW_HOME/webserver.log\" | tail"
echo
echo "   Dừng Airflow: pkill -f 'airflow webserver'; pkill -f 'airflow scheduler'"
