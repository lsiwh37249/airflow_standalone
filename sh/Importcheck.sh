#!/bin/bash

DONE_PATH_FILE=$1

echo "check"

echo "$DONE_PATH_FILE"

if [ -e "$DONE_PATH_FILE" ]; then
    figlet "Let's move on"
    exit 0
else
    echo "I'll be back!! => $DONE_PATH_FILE"
    exit 1
fi
