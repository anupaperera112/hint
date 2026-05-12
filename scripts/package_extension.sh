#!/usr/bin/env bash
set -euo pipefail

# Package the current repository into a tar.gz suitable for distribution.
# If in a git repo, use `git archive` to include only tracked files; otherwise
# fall back to `tar` excluding common build artifacts.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUT="hint-extension-$(date +%Y%m%d-%H%M%S).tar.gz"

if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1 && git rev-parse --verify HEAD >/dev/null 2>&1; then
  git archive --format=tar --prefix=hint/ HEAD | gzip > "$OUT"
  echo "Created $OUT (from git tracked files)"
else
  tar --exclude='./build' --exclude='./vcpkg' --exclude='./.git' --exclude='./.vscode' -czf "$OUT" .
  echo "Created $OUT (from filesystem, excluding build/vcpkg/.git)"
fi

echo "Packaging complete: $OUT"
