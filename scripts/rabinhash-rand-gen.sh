#!/usr/bin/env bash

COOL_CORE_PATH="${COOL_CORE_JAR_PATH:-./cool-core/target/cool-core-0.1-SNAPSHOT.jar}"

# parse user input as loop times
if [[ $1 =~ ^[0-9]+$ ]]; then
  loop=$1
else
  echo "Usage: $0 <loop times>"
  exit 1
fi

for ((i = 1; i <= loop; i++)); do
  input=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13)
  output=$(java -cp $COOL_CORE_PATH com.rabinhash.RabinHashCLI "$input")
  printf "%s %s\n" "$input" "$output"
done
