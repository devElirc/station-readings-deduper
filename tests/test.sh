#!/bin/bash

mkdir -p /logs/verifier
echo 0 > /logs/verifier/reward.txt

if [ "$PWD" = "/" ]; then
    echo "Error: No working directory set. Please set a WORKDIR in your Dockerfile."
    exit 1
fi

apt-get update
apt-get install -y curl strace
curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh
source $HOME/.local/bin/env

# Don't change anything below this line except for adding additional Python dependencies, e.g., pandas
uvx \
    -p 3.13 \
    -w pytest==8.4.1 \
    -w pytest-json-ctrf==0.3.5 \
    pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA

if [ $? -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi
