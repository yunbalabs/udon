#!/bin/bash
for d in ./dev/*; do alias u${d#./dev/dev}="$d/bin/udon"; done
for d in ./dev/*; do alias u${d#./dev/dev}a="$d/bin/udon-admin"; done

