#!/usr/bin/env bash

# Build all platforms that are known to be in use for OTPGo.
set -euo pipefail

cd "$(cd "$(dirname "$0")" && pwd)"

if ! command -v go >/dev/null 2>&1; then
    echo "error: go is not installed or not on PATH" >&2
    exit 1
fi

OUT_DIR="build"
mkdir -p "$OUT_DIR"

LDFLAGS="-s -w"

build() {
    local goos="$1" goarch="$2" out="$3"
    echo "==> Building $out ($goos/$goarch)"
    GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 \
        go build -trimpath -ldflags="$LDFLAGS" -o "$OUT_DIR/$out" .
}

build darwin  amd64 otpgo_darwin
build darwin  arm64 otpgo_darwin_arm
build linux   amd64 otpgo_linux
build linux   arm64 otpgo_linux_arm
build windows amd64 otpgo.exe

chmod -R +x "$OUT_DIR"

echo
echo "Build complete. Binaries are in $(pwd)/$OUT_DIR:"
ls -la "$OUT_DIR"

if [ -t 0 ]; then
    read -rp $'\nPress Enter to close...'
fi
