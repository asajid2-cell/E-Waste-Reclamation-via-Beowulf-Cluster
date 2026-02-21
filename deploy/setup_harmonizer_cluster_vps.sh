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

is_missing_secret() {
  local value="${1:-}"
  [[ -z "${value}" || "${value}" == replace_with_* || "${value}" == "change-me" ]]
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
  --source-mode MODE           auto|local|remote (default: auto)
  --source-dir DIR             Local repo directory to deploy from (default: script parent dir)
  --pull-local-source 0|1      Pull latest git changes for local source repo (default: 1)
  --repo-url URL               Git repository URL when source-mode=remote
  --repo-branch BRANCH         Git branch to deploy when source-mode=remote (default: light)
  --proxy-mode MODE            auto|host-nginx|docker-proxy (default: auto)
  --docker-proxy-container N   Optional docker proxy container name override
  --rotate-client-key 0|1      Rotate CLIENT_API_KEY on each deploy (default: 1)
  --worker-invite-phrase-words N  Phrase alias word count for workers, 3..8 (default: 8)
  --service-name NAME          systemd service name (default: cluster-app)
  --app-port PORT              Local app port behind nginx (default: 18080)
  --app-host HOST              Bind host for app service (default: 127.0.0.1)
  --nginx-site-conf PATH       Explicit nginx site file to patch include into
  --venv-path PATH             Python virtualenv path (default: APP_DIR/.venv)
  --skip-venv 0|1              Skip creating python virtualenv (default: 0)
  --enable-ufw 0|1             Enable UFW at end of setup (default: 1)
  --install-fail2ban 0|1       Install and enable fail2ban (default: 1)
  --help                       Show this help
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SOURCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

DOMAIN="harmonizer.cc"
BASE_PATH="/cluster"
APP_USER="harmonizer"
APP_GROUP=""
APP_DIR=""
SOURCE_MODE="auto"
SOURCE_DIR="${DEFAULT_SOURCE_DIR}"
PULL_LOCAL_SOURCE="1"
REPO_URL="https://github.com/asajid2-cell/E-Waste-Reclamation-via-Beowulf-Cluster.git"
REPO_BRANCH="light"
PROXY_MODE="auto"
DOCKER_PROXY_CONTAINER=""
DOCKER_PROXY_TYPE=""
ROTATE_CLIENT_KEY="1"
WORKER_INVITE_PHRASE_WORDS="8"
SERVICE_NAME="cluster-app"
APP_PORT="18080"
APP_HOST="127.0.0.1"
NGINX_SITE_CONF=""
VENV_PATH=""
SKIP_VENV="0"
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
    --source-mode)
      SOURCE_MODE="${2:?missing value}"
      shift 2
      ;;
    --source-dir)
      SOURCE_DIR="${2:?missing value}"
      shift 2
      ;;
    --pull-local-source)
      PULL_LOCAL_SOURCE="${2:?missing value}"
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
    --proxy-mode)
      PROXY_MODE="${2:?missing value}"
      shift 2
      ;;
    --docker-proxy-container)
      DOCKER_PROXY_CONTAINER="${2:?missing value}"
      shift 2
      ;;
    --rotate-client-key)
      ROTATE_CLIENT_KEY="${2:?missing value}"
      shift 2
      ;;
    --worker-invite-phrase-words)
      WORKER_INVITE_PHRASE_WORDS="${2:?missing value}"
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
    --venv-path)
      VENV_PATH="${2:?missing value}"
      shift 2
      ;;
    --skip-venv)
      SKIP_VENV="${2:?missing value}"
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
if [[ -z "${VENV_PATH}" ]]; then
  VENV_PATH="${APP_DIR}/.venv"
fi

require_root

if [[ ! "${SOURCE_MODE}" =~ ^(auto|local|remote)$ ]]; then
  fail "--source-mode must be one of: auto, local, remote"
fi

if [[ ! "${PULL_LOCAL_SOURCE}" =~ ^(0|1)$ ]]; then
  fail "--pull-local-source must be 0 or 1"
fi

if [[ ! "${PROXY_MODE}" =~ ^(auto|host-nginx|docker-proxy)$ ]]; then
  fail "--proxy-mode must be one of: auto, host-nginx, docker-proxy"
fi

if [[ ! "${ROTATE_CLIENT_KEY}" =~ ^(0|1)$ ]]; then
  fail "--rotate-client-key must be 0 or 1"
fi

if ! [[ "${WORKER_INVITE_PHRASE_WORDS}" =~ ^[0-9]+$ ]]; then
  fail "--worker-invite-phrase-words must be an integer between 3 and 8"
fi
if (( WORKER_INVITE_PHRASE_WORDS < 3 || WORKER_INVITE_PHRASE_WORDS > 8 )); then
  fail "--worker-invite-phrase-words must be between 3 and 8"
fi

if [[ "${SOURCE_MODE}" == "auto" ]]; then
  if [[ -d "${SOURCE_DIR}/.git" && -f "${SOURCE_DIR}/package.json" ]]; then
    SOURCE_MODE="local"
  else
    SOURCE_MODE="remote"
  fi
fi

if [[ "${SOURCE_MODE}" == "local" ]]; then
  SOURCE_DIR="$(cd "${SOURCE_DIR}" && pwd)"
  if [[ ! -f "${SOURCE_DIR}/package.json" ]]; then
    fail "Local source dir does not look like this project: ${SOURCE_DIR}"
  fi
fi

if ! command -v apt-get >/dev/null 2>&1; then
  fail "This setup script currently supports Debian/Ubuntu (apt-get)."
fi

COMPLETED_STEPS=()
PENDING_STEPS=()
NOTES=()

mark_done() {
  COMPLETED_STEPS+=("$1")
}

mark_pending() {
  PENDING_STEPS+=("$1")
}

mark_note() {
  NOTES+=("$1")
}

run_as_app() {
  runuser -u "${APP_USER}" -- bash -lc "$*"
}

cleanup_legacy_nginx_artifacts() {
  local cleaned="0"

  # Backup files in sites-enabled are loaded by nginx and often cause duplicate server/location conflicts.
  while IFS= read -r bak_file; do
    rm -f "${bak_file}"
    cleaned="1"
  done < <(find /etc/nginx/sites-enabled -maxdepth 1 -type f -name '*.bak*' 2>/dev/null || true)

  # Remove legacy cluster snippet includes introduced by older setup versions.
  while IFS= read -r conf_file; do
    sed -i '\|include /etc/nginx/snippets/cluster-app.conf;|d' "${conf_file}"
    cleaned="1"
  done < <(grep -RIl "include /etc/nginx/snippets/cluster-app.conf;" /etc/nginx/sites-enabled /etc/nginx/sites-available /etc/nginx/conf.d 2>/dev/null || true)

  # Remove transient cluster-route snippet references from manual recovery attempts.
  while IFS= read -r conf_file; do
    sed -i '\|include /etc/nginx/snippets/cluster-route.conf;|d' "${conf_file}"
    cleaned="1"
  done < <(grep -RIl "include /etc/nginx/snippets/cluster-route.conf;" /etc/nginx/sites-enabled /etc/nginx/sites-available /etc/nginx/conf.d 2>/dev/null || true)

  if [[ -f /etc/nginx/snippets/cluster-app.conf ]]; then
    rm -f /etc/nginx/snippets/cluster-app.conf
    cleaned="1"
  fi
  if [[ -f /etc/nginx/snippets/cluster-route.conf ]]; then
    rm -f /etc/nginx/snippets/cluster-route.conf
    cleaned="1"
  fi

  if [[ "${cleaned}" == "1" ]]; then
    mark_note "Cleaned legacy nginx artifacts (.bak files and stale cluster snippet includes)."
  fi
}

dedupe_paths() {
  local seen=""
  local item
  for item in "$@"; do
    [[ -z "${item}" ]] && continue
    if [[ "|${seen}|" == *"|${item}|"* ]]; then
      continue
    fi
    printf '%s\n' "${item}"
    seen="${seen}|${item}"
  done
}

validate_signing_keypair() {
  local private_b64="$1"
  local public_b64="$2"

  if [[ -z "${private_b64}" || -z "${public_b64}" ]]; then
    return 1
  fi
  # Validate in Node using the exact formats the app requires (pkcs8/spki DER).
  PRIV_B64="${private_b64}" PUB_B64="${public_b64}" node - <<'NODE'
const { createPrivateKey, createPublicKey, createSign, createVerify, randomUUID } = require("node:crypto");

const privB64 = process.env.PRIV_B64 || "";
const pubB64 = process.env.PUB_B64 || "";

try {
  const privateKey = createPrivateKey({
    key: Buffer.from(privB64, "base64"),
    type: "pkcs8",
    format: "der",
  });
  const publicKey = createPublicKey({
    key: Buffer.from(pubB64, "base64"),
    type: "spki",
    format: "der",
  });

  const payload = `cluster-setup-key-check-${randomUUID()}`;
  const signer = createSign("RSA-SHA256");
  signer.update(payload);
  signer.end();
  const sig = signer.sign(privateKey);

  const verifier = createVerify("RSA-SHA256");
  verifier.update(payload);
  verifier.end();
  process.exit(verifier.verify(publicKey, sig) ? 0 : 1);
} catch (_error) {
  process.exit(1);
}
NODE
}

generate_signing_keypair_node() {
  node -e 'const {generateKeyPairSync}=require("crypto"); const k=generateKeyPairSync("rsa",{modulusLength:2048,publicKeyEncoding:{type:"spki",format:"der"},privateKeyEncoding:{type:"pkcs8",format:"der"}}); console.log("JOB_SIGNING_PRIVATE_KEY_B64="+k.privateKey.toString("base64")); console.log("JOB_SIGNING_PUBLIC_KEY_B64="+k.publicKey.toString("base64"));'
}

collect_host_nginx_site_confs() {
  local domain_regex
  domain_regex="$(escape_regex "${DOMAIN}")"
  local nginx_candidates=()
  mapfile -t nginx_candidates < <(
    grep -RlsE "server_name[[:space:]].*${domain_regex}" \
      /etc/nginx/sites-enabled \
      /etc/nginx/sites-available \
      /etc/nginx/conf.d \
      2>/dev/null || true
  )
  if [[ "${#nginx_candidates[@]}" -eq 0 ]]; then
    return 1
  fi

  # Filter backup artifacts and canonicalize paths.
  local filtered=()
  local candidate
  for candidate in "${nginx_candidates[@]}"; do
    case "${candidate}" in
      *.bak*|*.disabled.*)
        continue
        ;;
    esac
    if [[ -f "${candidate}" ]]; then
      filtered+=("$(realpath "${candidate}")")
    fi
  done

  if [[ "${#filtered[@]}" -eq 0 ]]; then
    return 1
  fi

  local unique=()
  local seen=""
  for candidate in "${filtered[@]}"; do
    if [[ "|${seen}|" != *"|${candidate}|"* ]]; then
      unique+=("${candidate}")
      seen="${seen}|${candidate}"
    fi
  done

  if [[ "${#unique[@]}" -eq 0 ]]; then
    return 1
  fi

  # Stable priority:
  # 1) active enabled configs
  # 2) conf.d configs
  # 3) remaining sites-available configs
  local prioritized=()
  for candidate in "${unique[@]}"; do
    if [[ "${candidate}" == /etc/nginx/sites-enabled/* ]]; then
      prioritized+=("${candidate}")
    fi
  done
  for candidate in "${unique[@]}"; do
    if [[ "${candidate}" == /etc/nginx/conf.d/* ]]; then
      prioritized+=("${candidate}")
    fi
  done
  for candidate in "${unique[@]}"; do
    if [[ "${candidate}" == /etc/nginx/sites-available/* ]]; then
      prioritized+=("${candidate}")
    fi
  done

  # Deduplicate after priority merge.
  local final=()
  seen=""
  for candidate in "${prioritized[@]}"; do
    if [[ "|${seen}|" != *"|${candidate}|"* ]]; then
      final+=("${candidate}")
      seen="${seen}|${candidate}"
    fi
  done

  if [[ "${#final[@]}" -eq 0 ]]; then
    return 1
  fi

  printf '%s\n' "${final[@]}"
}

detect_host_nginx_site_conf() {
  local first
  first="$(collect_host_nginx_site_confs | head -n1 || true)"
  if [[ -z "${first}" ]]; then
    return 1
  fi
  printf '%s' "${first}"
}

collect_active_tls_nginx_domain_confs() {
  DOMAIN="${DOMAIN}" python3 - <<'PY'
import os
import pathlib
import re
import subprocess
import sys

domain = os.environ["DOMAIN"]
try:
    proc = subprocess.run(["nginx", "-T"], capture_output=True, text=True, check=False)
except FileNotFoundError:
    sys.exit(1)

text = (proc.stdout or "") + "\n" + (proc.stderr or "")
if not text.strip():
    sys.exit(1)

marker_rx = re.compile(r"^# configuration file (.+):$")
server_open_rx = re.compile(r"^\s*server\s*\{")
server_name_rx = re.compile(r"^\s*server_name\b")
listen_443_rx = re.compile(r"^\s*listen\s+[^;]*443")
domain_rx = re.compile(r"\b" + re.escape(domain) + r"\b")

files = {}
order = []
current_file = None
for line in text.splitlines():
    m = marker_rx.match(line)
    if m:
        current_file = m.group(1).strip()
        if current_file not in files:
            files[current_file] = []
            order.append(current_file)
        continue
    if current_file is not None:
        files[current_file].append(line)

matches = []
for file_path in order:
    lines = files.get(file_path, [])
    i = 0
    found = False
    while i < len(lines):
        if server_open_rx.search(lines[i]):
            depth = lines[i].count("{") - lines[i].count("}")
            j = i + 1
            has_domain = False
            has_tls = False
            while j < len(lines):
                ln = lines[j]
                if server_name_rx.search(ln) and domain_rx.search(ln):
                    has_domain = True
                if listen_443_rx.search(ln):
                    has_tls = True
                depth += ln.count("{")
                depth -= ln.count("}")
                if depth == 0:
                    break
                j += 1
            if has_domain and has_tls:
                found = True
                break
            i = j
        i += 1
    if found:
        p = pathlib.Path(file_path)
        matches.append(str(p.resolve()) if p.exists() else file_path)

seen = set()
for item in matches:
    if item in seen:
        continue
    seen.add(item)
    print(item)
PY
}

ensure_nginx_include_in_domain_servers() {
  local nginx_conf_path="$1"
  local include_line="$2"
  DOMAIN="${DOMAIN}" BASE_PATH="${BASE_PATH}" INCLUDE_LINE="${include_line}" NGINX_CONF_PATH="${nginx_conf_path}" python3 - <<'PY'
import os
import pathlib
import re
import sys

path = pathlib.Path(os.environ["NGINX_CONF_PATH"])
domain = os.environ["DOMAIN"]
base_path = os.environ["BASE_PATH"]
include_stmt = os.environ["INCLUDE_LINE"].strip()

if not path.exists():
    print(f"Nginx conf not found: {path}", file=sys.stderr)
    sys.exit(2)

lines = path.read_text(encoding="utf-8").splitlines()
original_lines = list(lines)
domain_rx = re.compile(re.escape(domain))
server_open_rx = re.compile(r"^\s*server\s*\{")
server_name_rx = re.compile(r"^\s*server_name\b")
location_exact_rx = re.compile(rf"^\s*location\s*=\s*{re.escape(base_path)}\s*\{{")
location_prefix_rx = re.compile(rf"^\s*location\s+\^~\s+{re.escape(base_path)}/\s*\{{")
location_any_cluster_rx = re.compile(rf"^\s*location\s+(?:=|\^~|~\*|~)?\s*{re.escape(base_path)}(?:/)?\s*\{{")

blocks = []
i = 0
while i < len(lines):
    if server_open_rx.search(lines[i]):
        depth = lines[i].count("{") - lines[i].count("}")
        j = i + 1
        while j < len(lines):
            depth += lines[j].count("{")
            depth -= lines[j].count("}")
            if depth == 0:
                blocks.append((i, j))
                break
            j += 1
        i = j
    i += 1

changed = False
matched = 0

def strip_cluster_locations(block_lines):
    local_changed = False
    out = []
    i = 0
    while i < len(block_lines):
        line = block_lines[i]
        if location_exact_rx.search(line) or location_prefix_rx.search(line) or location_any_cluster_rx.search(line):
            local_changed = True
            depth = line.count("{") - line.count("}")
            i += 1
            while i < len(block_lines) and depth > 0:
                depth += block_lines[i].count("{")
                depth -= block_lines[i].count("}")
                i += 1
            continue
        out.append(line)
        i += 1
    return out, local_changed

for start, end in reversed(blocks):
    block = lines[start:end + 1]
    domain_hit = any(server_name_rx.search(ln) and domain_rx.search(ln) for ln in block)
    if not domain_hit:
        continue
    matched += 1

    stripped_block, removed_locations = strip_cluster_locations(block)
    if removed_locations:
        changed = True

    new_block = [ln for ln in stripped_block if include_stmt not in ln]
    if len(new_block) != len(block):
        changed = True

    insert_after = None
    for idx, ln in enumerate(new_block):
        if server_name_rx.search(ln) and domain_rx.search(ln):
            insert_after = idx
    if insert_after is None:
        insert_after = 0

    indent = "    "
    if 0 <= insert_after < len(new_block):
        m = re.match(r"^(\s*)", new_block[insert_after])
        indent = m.group(1) if m else indent
    include_line = f"{indent}{include_stmt}"
    new_block.insert(insert_after + 1, include_line)
    changed = True

    lines[start:end + 1] = new_block

if matched == 0:
    print(f"No server block with server_name containing '{domain}' found in {path}", file=sys.stderr)
    sys.exit(3)

if changed:
    backup = pathlib.Path("/tmp") / f"{path.name}.cluster-setup.{os.getpid()}.bak"
    backup.write_text("\n".join(original_lines) + "\n", encoding="utf-8")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

patch_active_tls_nginx_domain_files() {
  local include_line="$1"
  DOMAIN="${DOMAIN}" BASE_PATH="${BASE_PATH}" INCLUDE_LINE="${include_line}" python3 - <<'PY'
import os
import pathlib
import re

domain = os.environ["DOMAIN"]
base_path = os.environ["BASE_PATH"]
include_stmt = os.environ["INCLUDE_LINE"].strip()
roots = [pathlib.Path("/etc/nginx/sites-enabled"), pathlib.Path("/etc/nginx/conf.d")]

paths = []
for root in roots:
    if root.exists():
        for p in root.glob("*"):
            if p.is_file():
                paths.append(p)

srv_open = re.compile(r"^\s*server\s*\{")
srv_name = re.compile(r"^\s*server_name\b")
listen_443 = re.compile(r"^\s*listen\s+[^;]*443")
loc_cluster = re.compile(rf"^\s*location\s+(?:=|\^~|~\*|~)?\s*{re.escape(base_path)}(?:/)?\s*\{{")
inc_line_exact = re.compile(r"^\s*" + re.escape(include_stmt) + r"\s*$")
inc_line_cluster_app = re.compile(r"^\s*include\s+/etc/nginx/snippets/cluster-app\.conf;\s*$")

def strip_cluster_locations(block):
    out, i, changed = [], 0, False
    while i < len(block):
        line = block[i]
        if loc_cluster.search(line):
            changed = True
            depth = line.count("{") - line.count("}")
            i += 1
            while i < len(block) and depth > 0:
                depth += block[i].count("{") - block[i].count("}")
                i += 1
            continue
        out.append(line)
        i += 1
    return out, changed

patched = []
for path in paths:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    lines = txt.splitlines()
    orig = lines[:]
    blocks = []
    i = 0
    while i < len(lines):
        if srv_open.search(lines[i]):
            depth = lines[i].count("{") - lines[i].count("}")
            j = i + 1
            while j < len(lines):
                depth += lines[j].count("{") - lines[j].count("}")
                if depth == 0:
                    blocks.append((i, j))
                    break
                j += 1
            i = j
        i += 1

    changed = False
    for s, e in reversed(blocks):
        block = lines[s:e + 1]
        block_text = "\n".join(block)
        if domain not in block_text:
            continue
        if not any(listen_443.search(x) for x in block):
            continue
        if not any(srv_name.search(x) for x in block):
            continue

        b2, c1 = strip_cluster_locations(block)
        b3 = [x for x in b2 if not inc_line_exact.search(x) and not inc_line_cluster_app.search(x)]
        if c1 or len(b3) != len(block):
            changed = True

        insert_after = None
        for idx, ln in enumerate(b3):
            if srv_name.search(ln):
                insert_after = idx
        if insert_after is None:
            insert_after = 0

        indent = "    "
        m = re.match(r"^(\s*)", b3[insert_after]) if 0 <= insert_after < len(b3) else None
        if m:
            indent = m.group(1)
        b3.insert(insert_after + 1, f"{indent}{include_stmt}")
        changed = True

        lines[s:e + 1] = b3

    if changed and lines != orig:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        patched.append(str(path))

for p in patched:
    print(p)
PY
}

infer_proxy_type_from_image() {
  local image_lc
  image_lc="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  if [[ "${image_lc}" == *caddy* ]]; then
    printf '%s' "caddy"
    return 0
  fi
  if [[ "${image_lc}" == *nginx* || "${image_lc}" == *openresty* ]]; then
    printf '%s' "nginx"
    return 0
  fi
  if [[ "${image_lc}" == *traefik* ]]; then
    printf '%s' "traefik"
    return 0
  fi
  return 1
}

infer_proxy_type_from_container_mounts() {
  local container_name="$1"
  local destinations
  destinations="$(docker inspect --format '{{range .Mounts}}{{println .Destination}}{{end}}' "${container_name}" 2>/dev/null || true)"
  if printf '%s' "${destinations}" | grep -q '/etc/caddy'; then
    printf '%s' "caddy"
    return 0
  fi
  if printf '%s' "${destinations}" | grep -q '/etc/nginx'; then
    printf '%s' "nginx"
    return 0
  fi
  if printf '%s' "${destinations}" | grep -q '/etc/traefik'; then
    printf '%s' "traefik"
    return 0
  fi
  return 1
}

detect_docker_proxy_container() {
  if ! command -v docker >/dev/null 2>&1; then
    return 1
  fi
  if ! docker info >/dev/null 2>&1; then
    return 1
  fi

  if [[ -n "${DOCKER_PROXY_CONTAINER}" ]]; then
    if ! docker ps --format '{{.Names}}' | grep -Fxq "${DOCKER_PROXY_CONTAINER}"; then
      fail "Specified --docker-proxy-container '${DOCKER_PROXY_CONTAINER}' is not running."
    fi
    local image
    image="$(docker inspect --format '{{.Config.Image}}' "${DOCKER_PROXY_CONTAINER}")"
    DOCKER_PROXY_TYPE="$(infer_proxy_type_from_image "${image}" || true)"
    if [[ -z "${DOCKER_PROXY_TYPE}" ]]; then
      DOCKER_PROXY_TYPE="$(infer_proxy_type_from_container_mounts "${DOCKER_PROXY_CONTAINER}" || true)"
    fi
    if [[ -z "${DOCKER_PROXY_TYPE}" ]]; then
      fail "Unsupported docker proxy image: ${image}"
    fi
    return 0
  fi

  local lines=()
  local line
  local fallback_name=""
  local fallback_image=""
  mapfile -t lines < <(docker ps --format '{{.Names}}|{{.Image}}|{{.Ports}}')

  for line in "${lines[@]}"; do
    IFS='|' read -r name image ports <<<"${line}"
    if [[ "${ports}" != *":80->"* && "${ports}" != *":443->"* ]]; then
      continue
    fi
    local proxy_type
    proxy_type="$(infer_proxy_type_from_image "${image}" || true)"
    if [[ -z "${proxy_type}" ]]; then
      proxy_type="$(infer_proxy_type_from_container_mounts "${name}" || true)"
    fi
    if [[ -z "${proxy_type}" ]]; then
      if [[ -z "${fallback_name}" ]]; then
        fallback_name="${name}"
        fallback_image="${image}"
      fi
      continue
    fi
    DOCKER_PROXY_CONTAINER="${name}"
    DOCKER_PROXY_TYPE="${proxy_type}"
    return 0
  done

  if [[ -n "${fallback_name}" ]]; then
    DOCKER_PROXY_CONTAINER="${fallback_name}"
    DOCKER_PROXY_TYPE="unknown"
    mark_note "Found potential proxy container ${fallback_name} (${fallback_image}) but proxy type could not be inferred."
    return 0
  fi

  return 1
}

detect_tls_listener_owner() {
  local listeners
  listeners="$(ss -H -ltnp 'sport = :443' 2>/dev/null || true)"
  if [[ -z "${listeners}" ]]; then
    return 1
  fi
  if printf '%s' "${listeners}" | grep -Eqi 'docker-proxy|containerd|caddy'; then
    printf '%s' "docker"
    return 0
  fi
  if printf '%s' "${listeners}" | grep -Eqi 'nginx'; then
    printf '%s' "nginx"
    return 0
  fi
  printf '%s' "unknown"
}

get_container_network_gateway() {
  local container_name="$1"
  local network_name
  network_name="$(docker inspect --format '{{range $k,$v := .NetworkSettings.Networks}}{{printf "%s\n" $k}}{{break}}{{end}}' "${container_name}" 2>/dev/null || true)"
  if [[ -z "${network_name}" ]]; then
    return 1
  fi
  local gateway
  gateway="$(docker network inspect "${network_name}" --format '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || true)"
  if [[ -n "${gateway}" ]]; then
    printf '%s' "${gateway}"
    return 0
  fi
  return 1
}

locate_caddyfile_for_container() {
  local container_name="$1"
  local line source dest
  while IFS= read -r line; do
    source="${line%%|*}"
    dest="${line##*|}"
    if [[ "${dest}" == "/etc/caddy/Caddyfile" && -f "${source}" ]]; then
      printf '%s' "${source}"
      return 0
    fi
    if [[ "${dest}" == "/etc/caddy" && -f "${source}/Caddyfile" ]]; then
      printf '%s' "${source}/Caddyfile"
      return 0
    fi
  done < <(docker inspect --format '{{range .Mounts}}{{println .Source "|" .Destination}}{{end}}' "${container_name}")
  return 1
}

locate_nginx_conf_for_container() {
  local container_name="$1"
  local domain_regex
  domain_regex="$(escape_regex "${DOMAIN}")"
  local line source dest
  while IFS= read -r line; do
    source="${line%%|*}"
    dest="${line##*|}"
    if [[ "${dest}" != /etc/nginx* ]]; then
      continue
    fi
    if [[ -f "${source}" ]]; then
      if grep -qE "server_name[[:space:]].*${domain_regex}" "${source}"; then
        printf '%s' "${source}"
        return 0
      fi
      continue
    fi
    if [[ -d "${source}" ]]; then
      local candidate
      candidate="$(grep -RlsE "server_name[[:space:]].*${domain_regex}" "${source}" 2>/dev/null | head -n1 || true)"
      if [[ -n "${candidate}" ]]; then
        printf '%s' "${candidate}"
        return 0
      fi
    fi
  done < <(docker inspect --format '{{range .Mounts}}{{println .Source "|" .Destination}}{{end}}' "${container_name}")
  return 1
}

patch_caddy_for_cluster() {
  local caddyfile_path="$1"
  local upstream="$2"
  DOMAIN="${DOMAIN}" BASE_PATH="${BASE_PATH}" UPSTREAM="${upstream}" CADDYFILE_PATH="${caddyfile_path}" python3 - <<'PY'
import os
import pathlib
import re
import sys

path = pathlib.Path(os.environ["CADDYFILE_PATH"])
domain = os.environ["DOMAIN"]
base_path = os.environ["BASE_PATH"]
upstream = os.environ["UPSTREAM"]

if not path.exists():
    print(f"Caddyfile not found: {path}", file=sys.stderr)
    sys.exit(2)

lines = path.read_text(encoding="utf-8").splitlines()
if any(f"path {base_path} {base_path}/*" in ln for ln in lines):
    sys.exit(0)

start = None
for i, line in enumerate(lines):
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        continue
    if "{" in stripped and domain in stripped:
        start = i
        break

if start is None:
    print(f"Domain block not found for {domain} in {path}", file=sys.stderr)
    sys.exit(3)

indent = re.match(r"^(\s*)", lines[start]).group(1) + "    "
insert = [
    f"{indent}# cluster route {base_path}",
    f"{indent}@cluster_route path {base_path} {base_path}/*",
    f"{indent}handle @cluster_route {{",
    f"{indent}    reverse_proxy {upstream}",
    f"{indent}}}",
]

out = lines[:start + 1] + insert + lines[start + 1:]
backup = pathlib.Path("/tmp") / f"{path.name}.cluster-setup.{os.getpid()}.bak"
backup.write_text("\n".join(lines) + "\n", encoding="utf-8")
path.write_text("\n".join(out) + "\n", encoding="utf-8")
PY
}

patch_nginx_for_cluster() {
  local nginx_conf_path="$1"
  local upstream="$2"
  DOMAIN="${DOMAIN}" BASE_PATH="${BASE_PATH}" UPSTREAM="${upstream}" NGINX_CONF_PATH="${nginx_conf_path}" python3 - <<'PY'
import os
import pathlib
import re
import sys

path = pathlib.Path(os.environ["NGINX_CONF_PATH"])
domain = os.environ["DOMAIN"]
base_path = os.environ["BASE_PATH"]
upstream = os.environ["UPSTREAM"]

if not path.exists():
    print(f"Nginx conf not found: {path}", file=sys.stderr)
    sys.exit(2)

lines = path.read_text(encoding="utf-8").splitlines()
server_start = None
server_end = None
i = 0
while i < len(lines):
    if re.search(r"^\s*server\s*\{", lines[i]):
        depth = lines[i].count("{") - lines[i].count("}")
        domain_hit = False
        j = i + 1
        while j < len(lines):
            if re.search(r"^\s*server_name\b", lines[j]) and domain in lines[j]:
                domain_hit = True
            depth += lines[j].count("{")
            depth -= lines[j].count("}")
            if depth == 0:
                if domain_hit:
                    server_start = i
                    server_end = j
                    break
                i = j
                break
            j += 1
        if server_start is not None:
            break
    i += 1

if server_start is None or server_end is None:
    print(f"server_name for {domain} not found in {path}", file=sys.stderr)
    sys.exit(3)

block_text = "\n".join(lines[server_start:server_end + 1])
if f"location ^~ {base_path}/" in block_text:
    sys.exit(0)

closing_indent = re.match(r"^(\s*)", lines[server_end]).group(1)
indent = closing_indent + "    "
insert = [
    f"{indent}# cluster route {base_path}",
    f"{indent}location = {base_path} {{",
    f"{indent}    return 302 {base_path}/client;",
    f"{indent}}}",
    f"{indent}location ^~ {base_path}/ {{",
    f"{indent}    proxy_pass http://{upstream};",
    f"{indent}    proxy_http_version 1.1;",
    f"{indent}    proxy_set_header Host $host;",
    f"{indent}    proxy_set_header X-Real-IP $remote_addr;",
    f"{indent}    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;",
    f"{indent}    proxy_set_header X-Forwarded-Proto $scheme;",
    f"{indent}    proxy_set_header Upgrade $http_upgrade;",
    f"{indent}    proxy_set_header Connection \"upgrade\";",
    f"{indent}    proxy_read_timeout 300s;",
    f"{indent}    proxy_send_timeout 300s;",
    f"{indent}}}",
]

out = lines[:server_end] + insert + [lines[server_end]] + lines[server_end + 1:]
backup = pathlib.Path("/tmp") / f"{path.name}.cluster-setup.{os.getpid()}.bak"
backup.write_text("\n".join(lines) + "\n", encoding="utf-8")
path.write_text("\n".join(out) + "\n", encoding="utf-8")
PY
}

configure_docker_proxy_route() {
  if ! detect_docker_proxy_container; then
    fail "Could not find docker proxy container automatically. Pass --docker-proxy-container."
  fi

  local gateway_ip
  gateway_ip="$(get_container_network_gateway "${DOCKER_PROXY_CONTAINER}" || true)"
  if [[ -z "${gateway_ip}" ]]; then
    gateway_ip="$(ip -4 addr show docker0 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -n1 || true)"
  fi
  if [[ -z "${gateway_ip}" ]]; then
    gateway_ip="127.0.0.1"
    mark_note "Docker gateway not detected; falling back to 127.0.0.1 upstream for proxy container."
  fi
  local upstream="${gateway_ip}:${APP_PORT}"
  mark_done "Using docker proxy container ${DOCKER_PROXY_CONTAINER} (${DOCKER_PROXY_TYPE}) -> upstream ${upstream}."

  if [[ "${DOCKER_PROXY_TYPE}" == "caddy" ]]; then
    local caddyfile_path
    caddyfile_path="$(locate_caddyfile_for_container "${DOCKER_PROXY_CONTAINER}" || true)"
    if [[ -z "${caddyfile_path}" ]]; then
      fail "Could not locate Caddyfile for container ${DOCKER_PROXY_CONTAINER}."
    fi
    patch_caddy_for_cluster "${caddyfile_path}" "${upstream}"
    if ! docker exec "${DOCKER_PROXY_CONTAINER}" caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1; then
      docker restart "${DOCKER_PROXY_CONTAINER}" >/dev/null
    fi
    mark_done "Patched caddy config at ${caddyfile_path} and reloaded ${DOCKER_PROXY_CONTAINER}."
    return
  fi

  if [[ "${DOCKER_PROXY_TYPE}" == "nginx" ]]; then
    local nginx_conf_path
    nginx_conf_path="$(locate_nginx_conf_for_container "${DOCKER_PROXY_CONTAINER}" || true)"
    if [[ -z "${nginx_conf_path}" ]]; then
      fail "Could not locate nginx config for ${DOMAIN} inside ${DOCKER_PROXY_CONTAINER} mounts."
    fi
    patch_nginx_for_cluster "${nginx_conf_path}" "${upstream}"
    docker exec "${DOCKER_PROXY_CONTAINER}" nginx -t >/dev/null
    docker exec "${DOCKER_PROXY_CONTAINER}" nginx -s reload >/dev/null
    mark_done "Patched nginx config at ${nginx_conf_path} and reloaded ${DOCKER_PROXY_CONTAINER}."
    return
  fi

  fail "Unsupported docker proxy type '${DOCKER_PROXY_TYPE}'."
}

log "Installing base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  git \
  gnupg \
  nginx \
  ufw \
  openssl \
  rsync \
  python3 \
  python3-venv \
  python3-pip \
  docker.io \
  docker-compose-plugin
mark_done "Installed system packages (nginx, ufw, openssl, rsync, python3, docker, docker compose)."

systemctl enable --now docker >/dev/null 2>&1 || true
if docker info >/dev/null 2>&1; then
  mark_done "Docker daemon is available."
else
  mark_pending "Docker daemon is not reachable right now."
fi

if [[ "${INSTALL_FAIL2BAN}" == "1" ]]; then
  apt-get install -y --no-install-recommends fail2ban
  mark_done "Installed fail2ban."
else
  mark_pending "fail2ban install was skipped (--install-fail2ban 0)."
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
  mark_done "Installed Node.js 22.x via NodeSource."
else
  mark_done "Detected Node.js $(node -v) (>=20)."
fi

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  log "Creating app user ${APP_USER}"
  useradd --create-home --shell /bin/bash "${APP_USER}"
  mark_done "Created app user '${APP_USER}'."
else
  mark_done "App user '${APP_USER}' already exists."
fi

if ! getent group "${APP_GROUP}" >/dev/null 2>&1; then
  log "Creating app group ${APP_GROUP}"
  groupadd "${APP_GROUP}"
  mark_done "Created app group '${APP_GROUP}'."
else
  mark_done "App group '${APP_GROUP}' already exists."
fi
usermod -a -G "${APP_GROUP}" "${APP_USER}"

install -d -o "${APP_USER}" -g "${APP_GROUP}" "$(dirname "${APP_DIR}")"
install -d -o "${APP_USER}" -g "${APP_GROUP}" "${APP_DIR}"

if [[ "${SOURCE_MODE}" == "local" ]]; then
  if [[ "${PULL_LOCAL_SOURCE}" == "1" && -d "${SOURCE_DIR}/.git" ]]; then
    log "Pulling latest changes in local source repo ${SOURCE_DIR}"
    if run_as_app "cd '${SOURCE_DIR}' && git pull --ff-only"; then
      mark_done "Pulled latest local source updates in ${SOURCE_DIR}."
    elif git -C "${SOURCE_DIR}" pull --ff-only >/dev/null 2>&1; then
      mark_done "Pulled latest local source updates in ${SOURCE_DIR} (root fallback)."
    else
      mark_pending "Could not pull local source automatically in ${SOURCE_DIR}; continuing with current files."
    fi
  fi

  log "Syncing local source ${SOURCE_DIR} into ${APP_DIR}"
  source_real="$(realpath "${SOURCE_DIR}")"
  app_real="$(realpath "${APP_DIR}")"

  if [[ "${source_real}" == "${app_real}" ]]; then
    mark_done "Using local source directory in place: ${APP_DIR}"
  else
    rsync -a --delete \
      --exclude '.git' \
      --exclude '.github' \
      --exclude 'node_modules' \
      --exclude '.env' \
      --exclude '*.log' \
      "${SOURCE_DIR}/" "${APP_DIR}/"
    chown -R "${APP_USER}:${APP_GROUP}" "${APP_DIR}"
    mark_done "Synced project files from local clone to ${APP_DIR}."
  fi
else
  log "Syncing repository into ${APP_DIR} (remote mode)"
  if [[ -d "${APP_DIR}/.git" ]]; then
    run_as_app "cd '${APP_DIR}' && git fetch origin '${REPO_BRANCH}' && git checkout '${REPO_BRANCH}' && git reset --hard 'origin/${REPO_BRANCH}'"
    mark_done "Updated existing repo in ${APP_DIR} to origin/${REPO_BRANCH}."
  else
    run_as_app "rm -rf '${APP_DIR}' && git clone --branch '${REPO_BRANCH}' '${REPO_URL}' '${APP_DIR}'"
    mark_done "Cloned ${REPO_URL} (${REPO_BRANCH}) into ${APP_DIR}."
  fi
fi

ENV_FILE="${APP_DIR}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ -f "${APP_DIR}/.env.example" ]]; then
    cp "${APP_DIR}/.env.example" "${ENV_FILE}"
    mark_done "Created ${ENV_FILE} from .env.example."
  else
    touch "${ENV_FILE}"
    mark_done "Created empty ${ENV_FILE}."
  fi
else
  mark_done "Using existing ${ENV_FILE}."
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

client_api_key="$(get_env_value CLIENT_API_KEY || true)"
worker_invite_secret="$(get_env_value WORKER_INVITE_SECRET || true)"
job_signing_private_key_b64="$(get_env_value JOB_SIGNING_PRIVATE_KEY_B64 || true)"
job_signing_public_key_b64="$(get_env_value JOB_SIGNING_PUBLIC_KEY_B64 || true)"
client_api_key_mnemonic=""

if [[ "${ROTATE_CLIENT_KEY}" == "1" ]]; then
  client_api_key="$(openssl rand -hex 32)"
  mark_done "Rotated CLIENT_API_KEY for this deployment."
elif is_missing_secret "${client_api_key}"; then
  client_api_key="$(openssl rand -hex 32)"
fi
if is_missing_secret "${worker_invite_secret}"; then
  worker_invite_secret="$(openssl rand -hex 48)"
fi
read_generated_keypair() {
  local output
  output="$(generate_signing_keypair_node)"
  job_signing_private_key_b64="$(printf '%s\n' "${output}" | awk -F= '/^JOB_SIGNING_PRIVATE_KEY_B64=/{print substr($0, index($0,"=")+1)}' | tail -n1)"
  job_signing_public_key_b64="$(printf '%s\n' "${output}" | awk -F= '/^JOB_SIGNING_PUBLIC_KEY_B64=/{print substr($0, index($0,"=")+1)}' | tail -n1)"
}

if is_missing_secret "${job_signing_private_key_b64}" || is_missing_secret "${job_signing_public_key_b64}"; then
  log "Generating RSA signing keypair for signed jobs (Node crypto)"
  read_generated_keypair
elif ! validate_signing_keypair "${job_signing_private_key_b64}" "${job_signing_public_key_b64}"; then
  log "Existing signing keypair is invalid/mismatched. Regenerating."
  read_generated_keypair
  mark_note "Signing keypair was regenerated because existing values failed validation."
fi

set_env_key "NODE_ENV" "production"
set_env_key "PORT" "${APP_PORT}"
set_env_key "HOST" "${APP_HOST}"
set_env_key "TRUST_PROXY" "1"
set_env_key "BASE_PATH" "${BASE_PATH}"
set_env_key "CLIENT_API_KEY" "${client_api_key}"
set_env_key "WORKER_INVITE_SECRET" "${worker_invite_secret}"
set_env_key "WORKER_INVITE_REQUIRE_LATEST" "1"
set_env_key "WORKER_INVITE_PHRASE_WORDS" "${WORKER_INVITE_PHRASE_WORDS}"
set_env_key "JOB_SIGNING_PRIVATE_KEY_B64" "${job_signing_private_key_b64}"
set_env_key "JOB_SIGNING_PUBLIC_KEY_B64" "${job_signing_public_key_b64}"

chown "${APP_USER}:${APP_GROUP}" "${ENV_FILE}"
chmod 600 "${ENV_FILE}"
mark_done "Configured app environment keys and secure permissions on ${ENV_FILE}."
mark_note "Mnemonic helper: run 'cd ${APP_DIR} && node cli/cluster-cli.js token-mnemonic --env-file ${ENV_FILE}' to get an easier client auth phrase."

if [[ "${SKIP_VENV}" == "0" ]]; then
  log "Creating/updating python virtualenv at ${VENV_PATH}"
  run_as_app "python3 -m venv '${VENV_PATH}'"
  run_as_app "source '${VENV_PATH}/bin/activate' && python -m pip install --upgrade pip setuptools wheel"
  if [[ -f "${APP_DIR}/requirements.txt" ]]; then
    run_as_app "source '${VENV_PATH}/bin/activate' && cd '${APP_DIR}' && pip install -r requirements.txt"
    mark_done "Created python venv and installed requirements.txt."
  else
    mark_done "Created python venv and upgraded pip/setuptools/wheel."
    mark_pending "No requirements.txt found; no project-specific Python packages installed."
  fi
else
  mark_pending "Python virtualenv creation skipped (--skip-venv 1)."
fi

log "Installing production node dependencies"
run_as_app "cd '${APP_DIR}' && npm ci --omit=dev"
mark_done "Installed npm production dependencies."

client_api_key_mnemonic="$(run_as_app "cd '${APP_DIR}' && CLIENT_KEY='${client_api_key}' node -e \"const {tokenToMnemonic}=require('./common/client-key'); const r=tokenToMnemonic(process.env.CLIENT_KEY||''); if(!r.ok){process.exit(1);} console.log(r.mnemonic);\"" 2>/dev/null || true)"
if [[ -n "${client_api_key_mnemonic}" ]]; then
  mark_done "Generated mnemonic phrase for CLIENT_API_KEY."
else
  mark_pending "Could not generate mnemonic phrase for CLIENT_API_KEY automatically."
fi

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
mark_done "Wrote systemd unit ${SERVICE_FILE}."

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" >/dev/null 2>&1 || true
systemctl restart "${SERVICE_NAME}"
mark_done "Enabled and restarted systemd service ${SERVICE_NAME}."
mark_note "Service ${SERVICE_NAME} was explicitly restarted to apply code and .env changes."

effective_proxy_mode="${PROXY_MODE}"
detected_host_nginx_conf="${NGINX_SITE_CONF}"
detected_host_nginx_confs=()
if [[ -z "${detected_host_nginx_conf}" ]]; then
  mapfile -t detected_host_nginx_confs < <(collect_host_nginx_site_confs || true)
  detected_host_nginx_conf="${detected_host_nginx_confs[0]:-}"
else
  detected_host_nginx_conf="$(realpath "${detected_host_nginx_conf}" 2>/dev/null || printf '%s' "${detected_host_nginx_conf}")"
  detected_host_nginx_confs=("${detected_host_nginx_conf}")
fi

# Prefer configs that nginx currently loads for this domain on TLS, not just guessed files.
active_tls_nginx_domain_confs=()
mapfile -t active_tls_nginx_domain_confs < <(collect_active_tls_nginx_domain_confs || true)
if [[ "${#active_tls_nginx_domain_confs[@]}" -gt 0 ]]; then
  mapfile -t detected_host_nginx_confs < <(dedupe_paths "${active_tls_nginx_domain_confs[@]}" "${detected_host_nginx_confs[@]}")
  detected_host_nginx_conf="${detected_host_nginx_confs[0]:-${detected_host_nginx_conf}}"
  mark_note "Active TLS nginx files for ${DOMAIN}: ${active_tls_nginx_domain_confs[*]}"
fi

tls_listener_owner="$(detect_tls_listener_owner || true)"
if [[ -n "${tls_listener_owner}" ]]; then
  mark_note "Detected local :443 listener owner: ${tls_listener_owner}"
fi

if [[ "${effective_proxy_mode}" == "auto" ]]; then
  if [[ "${tls_listener_owner}" == "docker" ]]; then
    if detect_docker_proxy_container && [[ "${DOCKER_PROXY_TYPE}" != "unknown" ]]; then
      effective_proxy_mode="docker-proxy"
    elif [[ -n "${detected_host_nginx_conf}" ]]; then
      effective_proxy_mode="host-nginx"
      mark_pending "443 appears docker-managed but docker proxy container was not confidently detected; falling back to host nginx setup."
    else
      effective_proxy_mode="host-nginx"
      mark_pending "Could not confidently detect docker proxy container; falling back to host nginx setup."
    fi
  elif [[ "${tls_listener_owner}" == "nginx" ]]; then
    if [[ -n "${detected_host_nginx_conf}" ]]; then
      effective_proxy_mode="host-nginx"
    elif detect_docker_proxy_container && [[ "${DOCKER_PROXY_TYPE}" != "unknown" ]]; then
      effective_proxy_mode="docker-proxy"
      mark_note "Nginx did not have a clear domain config; using detected docker proxy container."
    else
      effective_proxy_mode="host-nginx"
      mark_pending "Could not confidently detect docker proxy container; falling back to host nginx setup."
    fi
  else
    if [[ -n "${detected_host_nginx_conf}" ]]; then
      effective_proxy_mode="host-nginx"
    elif detect_docker_proxy_container && [[ "${DOCKER_PROXY_TYPE}" != "unknown" ]]; then
      effective_proxy_mode="docker-proxy"
    else
      effective_proxy_mode="host-nginx"
      mark_pending "Could not confidently detect docker proxy container; falling back to host nginx setup."
    fi
  fi
fi
mark_done "Proxy mode selected: ${effective_proxy_mode}."

# Normalize old config state before writing/updating route rules.
cleanup_legacy_nginx_artifacts

if [[ "${effective_proxy_mode}" == "host-nginx" ]]; then
  created_fallback_nginx_conf="0"
  NGINX_SITE_CONF="${detected_host_nginx_conf}"
  if [[ -z "${NGINX_SITE_CONF}" ]]; then
    NGINX_SITE_CONF="/etc/nginx/sites-available/${SERVICE_NAME}-${DOMAIN}.conf"
    cat > "${NGINX_SITE_CONF}" <<EOF
server {
    listen 80;
    server_name ${DOMAIN};

    location = ${BASE_PATH} {
        return 302 ${BASE_PATH}/client;
    }

    location ^~ ${BASE_PATH}/ {
        proxy_pass http://${APP_HOST}:${APP_PORT};
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
}
EOF
    ln -sf "${NGINX_SITE_CONF}" "/etc/nginx/sites-enabled/${SERVICE_NAME}-${DOMAIN}.conf"
    mark_done "Created fallback nginx site ${NGINX_SITE_CONF}."
    mark_pending "Fallback nginx site is HTTP-only unless your existing TLS setup terminates elsewhere."
    created_fallback_nginx_conf="1"
    detected_host_nginx_confs=("${NGINX_SITE_CONF}")
  elif [[ ! -f "${NGINX_SITE_CONF}" ]]; then
    fail "Nginx site config not found: ${NGINX_SITE_CONF}"
  fi

  if [[ "${created_fallback_nginx_conf}" == "0" ]]; then
    NGINX_SNIPPET="/etc/nginx/snippets/${SERVICE_NAME}.conf"
    log "Writing nginx location snippet ${NGINX_SNIPPET}"
    install -d /etc/nginx/snippets
    cat > "${NGINX_SNIPPET}" <<EOF
# Managed by ${SERVICE_NAME} setup script
location = ${BASE_PATH} {
    return 302 ${BASE_PATH}/client;
}

location ^~ ${BASE_PATH}/ {
    proxy_pass http://${APP_HOST}:${APP_PORT};
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
    mark_done "Wrote nginx snippet ${NGINX_SNIPPET}."

    include_line="include ${NGINX_SNIPPET};"
    mapfile -t tls_patched_files < <(patch_active_tls_nginx_domain_files "${include_line}" || true)
    if [[ "${#tls_patched_files[@]}" -gt 0 ]]; then
      mark_done "Patched active TLS nginx files for ${DOMAIN} (${#tls_patched_files[@]} file(s))."
      mark_note "Patched nginx files: ${tls_patched_files[*]}"
    else
      if [[ "${#detected_host_nginx_confs[@]}" -eq 0 ]]; then
        detected_host_nginx_confs=("${NGINX_SITE_CONF}")
      fi

      local_conf_count=0
      for conf_path in "${detected_host_nginx_confs[@]}"; do
        [[ -z "${conf_path}" ]] && continue
        if [[ ! -f "${conf_path}" ]]; then
          mark_pending "Detected nginx conf missing on disk: ${conf_path}"
          continue
        fi
        if grep -Fq "include /etc/nginx/snippets/${SERVICE_NAME}.conf;" "${conf_path}"; then
          sed -i "\|include /etc/nginx/snippets/${SERVICE_NAME}.conf;|d" "${conf_path}"
        fi
        log "Ensuring nginx include is present in domain server blocks in ${conf_path}"
        if ensure_nginx_include_in_domain_servers "${conf_path}" "${include_line}"; then
          local_conf_count=$((local_conf_count + 1))
        else
          mark_pending "Could not patch nginx config automatically: ${conf_path}"
        fi
      done
      if (( local_conf_count > 0 )); then
        mark_done "Ensured nginx include is configured for ${DOMAIN} server blocks in ${local_conf_count} nginx file(s)."
        mark_note "Patched nginx files: ${detected_host_nginx_confs[*]}"
      else
        mark_pending "Did not patch any nginx files for ${DOMAIN}; verify server_name blocks exist in active config."
      fi
    fi
  fi

  log "Testing and reloading nginx"
  nginx -t
  systemctl reload nginx
  mark_done "nginx config test and reload succeeded."
else
  configure_docker_proxy_route
fi

# Final forced restart to guarantee latest .env key material is loaded.
log "Performing final restart of ${SERVICE_NAME} to apply runtime config changes"
systemctl restart "${SERVICE_NAME}"
mark_done "Performed final restart of ${SERVICE_NAME} after proxy/config updates."

EDGE_HEALTH_HTTPS_URL="https://${DOMAIN}${BASE_PATH}/health"
edge_https_status="$(curl -ksS -o /dev/null -w '%{http_code}' --resolve "${DOMAIN}:443:127.0.0.1" "${EDGE_HEALTH_HTTPS_URL}" || true)"
if [[ "${edge_https_status}" == "200" ]]; then
  mark_done "Local edge HTTPS route check passed (${EDGE_HEALTH_HTTPS_URL})."
else
  edge_http_status="$(curl -sS -o /dev/null -w '%{http_code}' -H "Host: ${DOMAIN}" "http://127.0.0.1${BASE_PATH}/health" || true)"
  mark_pending "Local edge route check was not clean (https status=${edge_https_status:-error}, http status=${edge_http_status:-error}); verify active proxy path for ${DOMAIN}${BASE_PATH}."
fi

if [[ "${INSTALL_FAIL2BAN}" == "1" ]]; then
  systemctl enable --now fail2ban
  mark_done "Enabled fail2ban."
fi

if command -v ufw >/dev/null 2>&1; then
  log "Configuring firewall rules (ssh, 80, 443)"
  ufw allow OpenSSH >/dev/null || true
  ufw allow 80/tcp >/dev/null || true
  ufw allow 443/tcp >/dev/null || true
  if [[ "${ENABLE_UFW}" == "1" ]]; then
    ufw --force enable >/dev/null || true
    mark_done "Applied firewall rules and ensured UFW is enabled."
  else
    mark_pending "UFW enable skipped (--enable-ufw 0); rules were added but firewall may be inactive."
  fi
fi

HEALTH_URL="http://${APP_HOST}:${APP_PORT}${BASE_PATH}/health"
log "Performing local health check: ${HEALTH_URL}"
if curl -fsS "${HEALTH_URL}" >/dev/null; then
  mark_done "Local health check passed (${HEALTH_URL})."
else
  mark_pending "Local health check failed (${HEALTH_URL})."
fi

AUTH_CHECK_URL="http://${APP_HOST}:${APP_PORT}${BASE_PATH}/api/auth/check"
auth_check_ok="0"
for _attempt in 1 2 3 4 5; do
  if curl -fsS -H "Authorization: Bearer ${client_api_key}" "${AUTH_CHECK_URL}" >/dev/null; then
    auth_check_ok="1"
    break
  fi
  sleep 1
done
if [[ "${auth_check_ok}" == "1" ]]; then
  mark_done "Local auth check passed with current CLIENT_API_KEY (${AUTH_CHECK_URL})."
else
  mark_pending "Local auth check failed with current CLIENT_API_KEY (${AUTH_CHECK_URL}); verify service restart/env loading."
fi

if systemctl is-active --quiet "${SERVICE_NAME}"; then
  mark_done "Service ${SERVICE_NAME} is active."
else
  mark_pending "Service ${SERVICE_NAME} is not active."
  service_error_snippet="$(journalctl -u "${SERVICE_NAME}" -n 20 --no-pager 2>/dev/null | tail -n 6 | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' || true)"
  if [[ -n "${service_error_snippet}" ]]; then
    mark_note "Recent ${SERVICE_NAME} logs: ${service_error_snippet}"
  fi
fi

if systemctl is-active --quiet nginx; then
  mark_done "nginx service is active."
else
  mark_pending "nginx service is not active."
fi

if getent ahostsv4 "${DOMAIN}" >/dev/null 2>&1; then
  mark_done "Domain ${DOMAIN} resolves via DNS."
else
  mark_pending "Domain ${DOMAIN} does not currently resolve on this VPS; verify DNS A/AAAA records."
fi

mark_pending "Run an external test from a phone/another network: https://${DOMAIN}${BASE_PATH}/health"
mark_pending "Generate a worker invite from /client and verify join + job completion end-to-end."

REPORT_FILE="${APP_DIR}/deploy-status.txt"
{
  echo "Cluster Deployment Status Report"
  echo "Generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "Domain: ${DOMAIN}"
  echo "Base Path: ${BASE_PATH}"
  echo "App Dir: ${APP_DIR}"
  echo "Service: ${SERVICE_NAME}"
  echo "Source Mode: ${SOURCE_MODE}"
  echo "Pull Local Source: ${PULL_LOCAL_SOURCE}"
  echo "Proxy Mode: ${effective_proxy_mode}"
  echo "Rotate Client Key: ${ROTATE_CLIENT_KEY}"
  echo "Worker Invite Phrase Words: ${WORKER_INVITE_PHRASE_WORDS}"
  if [[ -n "${DOCKER_PROXY_CONTAINER}" ]]; then
    echo "Docker Proxy Container: ${DOCKER_PROXY_CONTAINER}"
    echo "Docker Proxy Type: ${DOCKER_PROXY_TYPE}"
  fi
  echo
  echo "Completed:"
  for item in "${COMPLETED_STEPS[@]}"; do
    echo "  - ${item}"
  done
  echo
  echo "Pending / Manual Follow-up:"
  for item in "${PENDING_STEPS[@]}"; do
    echo "  - ${item}"
  done
  if [[ "${#NOTES[@]}" -gt 0 ]]; then
    echo
    echo "Notes:"
    for item in "${NOTES[@]}"; do
      echo "  - ${item}"
    done
  fi
  echo
  echo "URLs:"
  echo "  - Client: https://${DOMAIN}${BASE_PATH}/client"
  echo "  - Worker entry: https://${DOMAIN}${BASE_PATH}/worker?invite=..."
  echo "  - Health (public): https://${DOMAIN}${BASE_PATH}/health"
  echo "  - Health (local): ${HEALTH_URL}"
} | tee "${REPORT_FILE}"

chown "${APP_USER}:${APP_GROUP}" "${REPORT_FILE}" || true

log "Setup complete."
log "Status report saved to ${REPORT_FILE}"
log "Client API token is stored in ${ENV_FILE}"
log "CLIENT_API_KEY (new/current): ${client_api_key}"
if [[ -n "${client_api_key_mnemonic}" ]]; then
  log "CLIENT_API_KEY mnemonic: ${client_api_key_mnemonic}"
fi
