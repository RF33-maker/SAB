#!/bin/bash
set -e

echo "==> Installing Python dependencies..."
pip install -r requirements.txt --quiet

echo "==> Post-merge setup complete."
