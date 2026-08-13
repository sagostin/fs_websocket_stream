#!/bin/bash
# Scripted build & install of mod_ws_bridge (Debian/Ubuntu).
# Requires FreeSWITCH development headers (libfreeswitch-dev).
set -e

apt-get -y install libfreeswitch-dev libssl-dev zlib1g-dev libspeexdsp-dev libevent-dev

FS_PKGCONFIG=/usr/local/freeswitch/lib/pkgconfig
if [ -d "$FS_PKGCONFIG" ]; then
    export PKG_CONFIG_PATH=$FS_PKGCONFIG
fi

mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
make install
