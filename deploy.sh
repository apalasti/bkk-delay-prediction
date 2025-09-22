#!/bin/bash
set -euo pipefail

# Need to be filled in .env file:
# BKK_API_KEY="..."
# ARM_SUBSCRIPTION_ID="..."
# ARM_CLIENT_ID="..."        # appId
# ARM_CLIENT_SECRET="..."    # password
# ARM_TENANT_ID="..."        # tenant

if [ -f .env ]; then
  # Export variables from .env, ignoring comments and empty lines
  export $(grep -v '^#' .env | grep -v '^\s*$' | xargs)
fi
export TF_VAR_BKK_API_KEY="${BKK_API_KEY}"

terraform -chdir=infra init && terraform -chdir=infra apply 