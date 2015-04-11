#!/usr/bin/env bash

function do_all {
for d in ./dev/*; do echo $d; $d/bin/udon $1; done
}

do_all $1
