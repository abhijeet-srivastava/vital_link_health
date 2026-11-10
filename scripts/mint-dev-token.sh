#!/usr/bin/env bash
# Mints an HS256 bearer token for query-api's dev issuer (never for production: real tokens come from OIDC).
#   scripts/mint-dev-token.sh <sub> [COACH|CLINICIAN|SERVICE] [scopes] [ttl-seconds]
#   scripts/mint-dev-token.sh 2                                   # coach 2, coach scopes, 1 hour
#   scripts/mint-dev-token.sh dr-7 CLINICIAN thresholds:write
# The secret comes from VITALLINK_DEV_JWT_SECRET, else from .env (the value compose gives query-api).
set -euo pipefail
cd "$(dirname "$0")/.."

sub=${1:?usage: $0 <sub> [COACH|CLINICIAN|SERVICE] [scopes] [ttl-seconds]}
actor=${2:-COACH}
scopes=${3:-readings:read alerts:read alerts:write}
ttl=${4:-3600}

secret=${VITALLINK_DEV_JWT_SECRET:-}
if [ -z "$secret" ] && [ -f .env ]; then
  secret=$(grep -E '^VITALLINK_DEV_JWT_SECRET=' .env | tail -1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//')
fi
[ -n "$secret" ] || { echo "VITALLINK_DEV_JWT_SECRET is not set (env or .env)" >&2; exit 1; }

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

now=$(date +%s)
header='{"alg":"HS256","typ":"JWT"}'
payload=$(printf '{"iss":"vitallink-dev","aud":"query-api","sub":"%s","vl_actor":"%s","scope":"%s","iat":%d,"exp":%d}' \
  "$sub" "$actor" "$scopes" "$now" "$((now + ttl))")
unsigned="$(printf '%s' "$header" | b64url).$(printf '%s' "$payload" | b64url)"
signature=$(printf '%s' "$unsigned" | openssl dgst -sha256 -hmac "$secret" -binary | b64url)
printf '%s.%s\n' "$unsigned" "$signature"
