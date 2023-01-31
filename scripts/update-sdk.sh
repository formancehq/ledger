#!/usr/bin/env bash

dir=$(dirname "$0")
source "${dir}"/common.sh

task openapi:sdk:build
task -p openapi:sdk:generate:all
