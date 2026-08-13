# FreeSWITCH build with mod_ws_bridge baked in.
# Build context must be the repository root:
#   docker build -f examples/freeswitch/Dockerfile -t fs-ws-bridge .
FROM debian:bookworm

COPY examples/freeswitch/modules.conf /modules.conf

RUN apt-get update && \
DEBIAN_FRONTEND=noninteractive apt-get -y install git build-essential pkg-config uuid-dev zlib1g-dev libjpeg-dev libsqlite3-dev libcurl4-openssl-dev libpcre3-dev libspeexdsp-dev libldns-dev libedit-dev libtiff5-dev yasm libopus-dev libsndfile1-dev unzip libavformat-dev libswscale-dev libswresample-dev liblua5.2-dev liblua5.2-0 cmake libpq-dev unixodbc-dev autoconf automake ntpdate libxml2-dev libpq-dev libpq5 libspeex-dev libevent-dev &&\
\
cd /usr/src/ && \
git clone https://github.com/signalwire/libks.git && \
cd libks && \
cmake . && \
make && make install && \
\
cd /usr/src/ && \
git clone https://github.com/signalwire/signalwire-c.git && \
cd signalwire-c && \
cmake . && \
make && make install && \
\
cd /usr/src/ && \
git clone https://github.com/freeswitch/sofia-sip.git && \
cd sofia-sip && \
./bootstrap.sh && \
./configure && \
make && make install && \
\
cd /usr/src/ && \
git clone https://github.com/freeswitch/spandsp.git && \
cd spandsp && \
./bootstrap.sh && \
./configure && \
make && make install && \
\
ldconfig && \
\
cd /usr/src/ && \
git clone https://github.com/signalwire/freeswitch.git -bv1.10 freeswitch && \
cd freeswitch && \
git config pull.rebase true && \
cp /modules.conf /usr/src/freeswitch/modules.conf && \
\
./bootstrap.sh -j && \
./configure && \
make && \
make install

# Build mod_ws_bridge against the freshly installed FreeSWITCH.
COPY module /usr/src/mod_ws_bridge
RUN export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig && \
    cd /usr/src/mod_ws_bridge && \
    mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && make install

# Enable the module.
RUN sed -i 's#</modules>#    <load module="mod_ws_bridge"/>\n  </modules>#' \
    /usr/local/freeswitch/conf/autoload_configs/modules.conf.xml

# Dialplan: dial 9999 -> stream full-duplex audio to the bridge.
COPY examples/freeswitch/dialplan-9999.xml /usr/local/freeswitch/conf/dialplan/default/01_ws_bridge.xml

CMD ["/usr/local/freeswitch/bin/freeswitch", "-nonat"]
