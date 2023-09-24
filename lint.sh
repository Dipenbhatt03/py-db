#!/bin/bash

dir=${1:-.}
autoflake --remove-all-unused-imports --in-place --recursive . "$dir"
isort --profile black --line-length=120 -skip-gitignore --atomic --combine-as "$dir"
black --line-length 120 "$dir"
