#!/bin/bash

set -e -x
apt update -qq
apt-get -qq install pkg-config libssl-dev protobuf-compiler
apt-get -qq install git
rustup component add rustfmt
cargo build --verbose
cargo test --verbose
cargo fmt --all
