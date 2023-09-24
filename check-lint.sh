#!/bin/bash

set -e
set -o pipefail

black --line-length 120 --check .
isort --profile black --line-length=120 -skip-gitignore --atomic --combine-as -c -v .  
autoflake --remove-all-unused-imports --check --recursive .
pylint_runner --django-settings-module=optimus_store.settings
