#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

log() {
  printf '[cluster-setup] %s\n' "$*"
}

fail() {
  printf '[cluster-setup] ERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    fail "Run this script as root (sudo)."
  fi
}

normalize_base_path() {
  local raw="${1:-}"
  if [[ -z "${raw}" || "${raw}" == "/" ]]; then
    printf '/cluster'
    return
  fi
  if [[ "${raw}" != /* ]]; then
    raw="/${raw}"
  fi
  raw="${raw%/}"
  printf '%s' "${raw}"
}

escape_regex() {
  printf '%s' "$1" | sed 's/[.[\*^$()+?{|]/\\&/g'
}

usage() {
  cat <<'USAGE'
Usage:
  sudo ./deploy/setup_harmonizer_cluster_vps.sh [options]

Options:
  --domain DOMAIN              Domain that already serves your site (default: harmonizer.cc)
  --base-path PATH             URL prefix for cluster app (default: /cluster)
  --app-user USER              Linux user that owns app files (default: harmonizer)
  --app-group GROUP            Linux group for app files (default: same as app-user)
  --app-dir DIR                Deployment directory (default: /home/harmonizer/apps/cluster)
  --repo-url URL               Git repository URL to pull from
  --repo-branch BRANCH         Git branch to deploy (default: light)
  --service-name NAME          systemd service name (default: cluster-app)
  --app-port PORT              Local app port behind nginx (default: 18080)
  --app-host HOST              Bind host for app service (default: 127.0.0.1)
  --nginx-site-conf PATH       Explicit nginx site file to patch include into
  --enable-ufw 0|1             Enable UFW at end of setup (default: 1)
  --install-fail2ban 0|1       Install and enable fail2ban (default: 1)
  --help                       Show this help
USAGE
}

DOMAIN="harmonizer.cc"
BASE_PATH="/cluster"
APP_USER="harmonizer"
APP_GROUP=""
APP_DIR=""
REPO_URL="https://github.com/asajid2-cell/E-Waste-Reclamation-via-Beowulf-Cluster.git"
REPO_BRANCH="light"
SERVICE_NAME="cluster-app"
APP_PORT="18080"
APP_HOST="127.0.0.1"
NGINX_SITE_CONF=""
ENABLE_UFW="1"
INSTALL_FAIL2BAN="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --domain)
      DOMAIN="${2:?missing value}"
      shift 2
      ;;
    --base-path)
      BASE_PATH="${2:?missing value}"
      shift 2
      ;;
    --app-user)
      APP_USER="${2:?missing value}"
      shift 2
      ;;
    --app-group)
      APP_GROUP="${2:?missing value}"
      shift 2
      ;;
    --app-dir)
      APP_DIR="${2:?missing value}"
      shift 2
      ;;
    --repo-url)
      REPO_URL="${2:?missing value}"
      shift 2
      ;;
    --repo-branch)
      REPO_BRANCH="${2:?missing value}"
      shift 2
      ;;
    --service-name)
      SERVICE_NAME="${2:?missing value}"
      shift 2
      ;;
    --app-port)
      APP_PORT="${2:?missing value}"
      shift 2
      ;;
    --app-host)
      APP_HOST="${2:?missing value}"
      shift 2
      ;;
    --nginx-site-conf)
      NGINX_SITE_CONF="${2:?missing value}"
      shift 2
      ;;
    --enable-ufw)
      ENABLE_UFW="${2:?missing value}"
      shift 2
      ;;
    --install-fail2ban)
      INSTALL_FAIL2BAN="${2:?missing value}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fail "Unknown argument: $1"
      ;;
  esac
done

BASE_PATH="$(normalize_base_path "${BASE_PATH}")"
if [[ -z "${APP_GROUP}" ]]; then
  APP_GROUP="${APP_USER}"
fi
if [[ -z "${APP_DIR}" ]]; then
  APP_DIR="/home/${APP_USER}/apps/cluster"
fi

require_root

if ! command -v apt-get >/dev/null 2>&1; then
  fail "This setup script currently supports Debian/Ubuntu (apt-get)."
fi

log "Installing base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends ca-certificates curl git gnupg nginx ufw openssl
if [[ "${INSTALL_FAIL2BAN}" == "1" ]]; then
  apt-get install -y --no-install-recommends fail2ban
fi

log "Ensuring Node.js >= 20"
need_node_install="1"
if command -v node >/dev/null 2>&1; then
  node_major="$(node -v | sed -E 's/^v([0-9]+).*/\1/')"
  if [[ "${node_major}" -ge 20 ]]; then
    need_node_install="0"
  fi
fi
if [[ "${need_node_install}" == "1" ]]; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y nodejs build-essential
fi

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  log "Creating app user ${APP_USER}"
  useradd --create-home --shell /bin/bash "${APP_USER}"
fi

if ! getent group "${APP_GROUP}" >/dev/null 2>&1; then
  log "Creating app group ${APP_GROUP}"
  groupadd "${APP_GROUP}"
  usermod -a -G "${APP_GROUP}" "${APP_USER}"
fi

run_as_app() {
  runuser -u "${APP_USER}" -- bash -lc "$*"
}

log "Syncing repository into ${APP_DIR}"
install -d -o "${APP_USER}" -g "${APP_GROUP}" "$(dirname "${APP_DIR}")"
if [[ -d "${APP_DIR}/.git" ]]; then
  run_as_app "cd '${APP_DIR}' && git fetch origin '${REPO_BRANCH}' && git checkout '${REPO_BRANCH}' && git reset --hard 'origin/${REPO_BRANCH}'"
else
  run_as_app "git clone --branch '${REPO_BRANCH}' '${REPO_URL}' '${APP_DIR}'"
fi

ENV_FILE="${APP_DIR}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ -f "${APP_DIR}/.env.example" ]]; then
    cp "${APP_DIR}/.env.example" "${ENV_FILE}"
  else
    touch "${ENV_FILE}"
  fi
fi

get_env_value() {
  local key="$1"
  awk -F= -v target="${key}" '$1 == target {print substr($0, index($0, "=") + 1)}' "${ENV_FILE}" | tail -n 1
}

set_env_key() {
  local key="$1"
  local value="$2"
  local tmp
  tmp="$(mktemp)"
  awk -F= -v target="${key}" -v replacement="${value}" '
    BEGIN { done = 0 }
    $1 == target {
      print target "=" replacement
      done = 1
      next
    }
    { print $0 }
    END {
      if (!done) {
        print target "=" replacement
      }
    }
  ' "${ENV_FILE}" > "${tmp}"
  mv "${tmp}" "${ENV_FILE}"
}

is_missing_secret() {
  local value="${1:-}"
  [[ -z "${value}" || "${value}" == replace_with_* || "${value}" == "change-me" ]]
}

client_api_key="$(get_env_value CLIENT_API_KEY || true)"
worker_invite_secret="$(get_env_value WORKER_INVITE_SECRET || true)"

if is_missing_secret "${client_api_key}"; then
  client_api_key="$(openssl rand -hex 32)"
fi
if is_missing_secret "${worker_invite_secret}"; then
  worker_invite_secret="$(openssl rand -hex 48)"
fi

set_env_key "NODE_ENV" "production"
set_env_key "PORT" "${APP_PORT}"
set_env_key "HOST" "${APP_HOST}"
set_env_key "TRUST_PROXY" "1"
set_env_key "BASE_PATH" "${BASE_PATH}"
set_env_key "CLIENT_API_KEY" "${client_api_key}"
set_env_key "WORKER_INVITE_SECRET" "${worker_invite_secret}"

chown "${APP_USER}:${APP_GROUP}" "${ENV_FILE}"
chmod 600 "${ENV_FILE}"

log "Installing production dependencies"
run_as_app "cd '${APP_DIR}' && npm ci --omit=dev"

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
log "Writing systemd service ${SERVICE_NAME}"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Cluster Worker Orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=/usr/bin/node server/index.js
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
LimitNOFILE=65536
UMask=027

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

if [[ -z "${NGINX_SITE_CONF}" ]]; then
  domain_regex="$(escape_regex "${DOMAIN}")"
  mapfile -t nginx_candidates < <(grep -RlsE "server_name[[:space:]].*${domain_regex}" /etc/nginx/sites-enabled /etc/nginx/sites-available 2>/dev/null || true)
  if [[ "${#nginx_candidates[@]}" -eq 0 ]]; then
    fail "Could not find nginx site config with server_name ${DOMAIN}. Pass --nginx-site-conf."
  fi
  NGINX_SITE_CONF="${nginx_candidates[0]}"
fi

if [[ ! -f "${NGINX_SITE_CONF}" ]]; then
  fail "Nginx site config not found: ${NGINX_SITE_CONF}"
fi

NGINX_SNIPPET="/etc/nginx/snippets/${SERVICE_NAME}.conf"
log "Writing nginx location snippet ${NGINX_SNIPPET}"
install -d /etc/nginx/snippets
cat > "${NGINX_SNIPPET}" <<EOF
# Managed by ${SERVICE_NAME} setup script
location = ${BASE_PATH} {
    return 302 ${BASE_PATH}/client;
}

location ^~ ${BASE_PATH}/ {
    proxy_pass http://127.0.0.1:${APP_PORT};
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_set_header Upgrade \$http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;
}
EOF

include_line="include ${NGINX_SNIPPET};"
if ! grep -Fq "${include_line}" "${NGINX_SITE_CONF}"; then
  log "Injecting nginx include into ${NGINX_SITE_CONF}"
  cp "${NGINX_SITE_CONF}" "${NGINX_SITE_CONF}.bak.$(date +%Y%m%d%H%M%S)"
  domain_regex="$(escape_regex "${DOMAIN}")"
  sed -i "/server_name[[:space:]].*${domain_regex}/a\\    ${include_line}" "${NGINX_SITE_CONF}"
fi

log "Testing and reloading nginx"
nginx -t
systemctl reload nginx

if [[ "${INSTALL_FAIL2BAN}" == "1" ]]; then
  log "Enabling fail2ban"
  systemctl enable --now fail2ban
fi

if command -v ufw >/dev/null 2>&1; then
  log "Configuring firewall rules (ssh, 80, 443)"
  ufw allow OpenSSH >/dev/null || true
  ufw allow 80/tcp >/dev/null || true
  ufw allow 443/tcp >/dev/null || true
  if [[ "${ENABLE_UFW}" == "1" ]]; then
    ufw --force enable >/dev/null || true
  fi
fi

log "Performing local health check"
curl -fsS "http://127.0.0.1:${APP_PORT}${BASE_PATH}/health" >/dev/null

log "Setup complete."
log "Cluster client URL: https://${DOMAIN}${BASE_PATH}/client"
log "Worker entry URL:  https://${DOMAIN}${BASE_PATH}/worker?invite=..."
log "Service status: systemctl status ${SERVICE_NAME} --no-pager"
log "Client API token saved in: ${ENV_FILE}"
