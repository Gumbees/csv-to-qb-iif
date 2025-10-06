#!/bin/sh
set -eu
curl -fsS http://localhost:3000/healthz >/dev/null