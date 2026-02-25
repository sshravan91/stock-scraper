#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"
REQUIREMENTS_FILE="${ROOT_DIR}/requirements.txt"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Error: ${PYTHON_BIN} not found on PATH."
  exit 1
fi

if [[ ! -f "${REQUIREMENTS_FILE}" ]]; then
  echo "Error: requirements file not found at ${REQUIREMENTS_FILE}"
  exit 1
fi

echo "==> Creating virtual environment at ${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"

echo "==> Activating virtual environment"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "==> Upgrading pip/setuptools/wheel"
python -m pip install --upgrade pip setuptools wheel

echo "==> Installing Python requirements"
python -m pip install -r "${REQUIREMENTS_FILE}"

echo "==> Installing Playwright Chromium browser binary"
python -m playwright install chromium

echo
echo "Setup complete."
echo "Activate with: source ${VENV_DIR}/bin/activate"
echo "Run parser with: python advisor-parser-plus.py"
