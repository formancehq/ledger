#!/usr/bin/env bash

dir=$(dirname "$0")
source "${dir}"/common.sh

goimports -w $@
