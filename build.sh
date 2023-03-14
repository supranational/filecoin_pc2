#!/bin/bash
# Copyright Supranational LLC

set -e
set -x

CUDA="/usr/local/cuda"
NVCC="$CUDA/bin/nvcc"
PC1="../filecoin_pc1"

SPDK="../spdk-v22.09"

CPPFLAGS="-std=c++17 -fno-omit-frame-pointer -g -O2 -Wall -Wextra -Wno-unused-parameter \
          -Wno-missing-field-initializers -fno-strict-aliasing \
          -march=native -Wformat -Wformat-security \
          -D_GNU_SOURCE -fPIC -fstack-protector \
          -fno-common -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2 \
          -DSPDK_GIT_COMMIT=4be6d3043 -pthread"

LDFLAGS="-Wl,-z,relro,-z,now -Wl,-z,noexecstack -fuse-ld=bfd\
         -L$SPDK/build/lib \
         -Wl,--whole-archive -Wl,--no-as-needed \
         -lspdk_bdev_malloc \
         -lspdk_bdev_null \
         -lspdk_bdev_nvme \
         -lspdk_bdev_passthru \
         -lspdk_bdev_lvol \
         -lspdk_bdev_raid \
         -lspdk_bdev_error \
         -lspdk_bdev_gpt \
         -lspdk_bdev_split \
         -lspdk_bdev_delay \
         -lspdk_bdev_zone_block \
         -lspdk_blobfs_bdev \
         -lspdk_blobfs \
         -lspdk_blob_bdev \
         -lspdk_lvol \
         -lspdk_blob \
         -lspdk_nvme \
         -lspdk_bdev_ftl \
         -lspdk_ftl \
         -lspdk_bdev_aio \
         -lspdk_bdev_virtio \
         -lspdk_virtio \
         -lspdk_vfio_user \
         -lspdk_accel_ioat \
         -lspdk_ioat \
         -lspdk_scheduler_dynamic \
         -lspdk_env_dpdk \
         -lspdk_scheduler_dpdk_governor \
         -lspdk_scheduler_gscheduler \
         -lspdk_sock_posix \
         -lspdk_event \
         -lspdk_event_bdev \
         -lspdk_bdev \
         -lspdk_notify \
         -lspdk_dma \
         -lspdk_event_accel \
         -lspdk_accel \
         -lspdk_event_vmd \
         -lspdk_vmd \
         -lspdk_event_sock \
         -lspdk_init \
         -lspdk_thread \
         -lspdk_trace \
         -lspdk_sock \
         -lspdk_rpc \
         -lspdk_jsonrpc \
         -lspdk_json \
         -lspdk_util \
         -lspdk_log \
         -Wl,--no-whole-archive $SPDK/build/lib/libspdk_env_dpdk.a \
         -Wl,--whole-archive $SPDK/dpdk/build/lib/librte_bus_pci.a \
         $SPDK/dpdk/build/lib/librte_cryptodev.a \
         $SPDK/dpdk/build/lib/librte_dmadev.a \
         $SPDK/dpdk/build/lib/librte_eal.a \
         $SPDK/dpdk/build/lib/librte_ethdev.a \
         $SPDK/dpdk/build/lib/librte_hash.a \
         $SPDK/dpdk/build/lib/librte_kvargs.a \
         $SPDK/dpdk/build/lib/librte_mbuf.a \
         $SPDK/dpdk/build/lib/librte_mempool.a \
         $SPDK/dpdk/build/lib/librte_mempool_ring.a \
         $SPDK/dpdk/build/lib/librte_net.a \
         $SPDK/dpdk/build/lib/librte_pci.a \
         $SPDK/dpdk/build/lib/librte_power.a \
         $SPDK/dpdk/build/lib/librte_rcu.a \
         $SPDK/dpdk/build/lib/librte_ring.a \
         $SPDK/dpdk/build/lib/librte_telemetry.a \
         $SPDK/dpdk/build/lib/librte_vhost.a \
         -Wl,--no-whole-archive \
         -lnuma -ldl \
         -L$SPDK/isa-l/.libs -lisal \
         -lpthread -lrt -luuid -lssl -lcrypto -lm -laio"

INCLUDE="-I$PC1/src -I$SPDK/include -I$SPDK/isa-l/.. -I$SPDK/dpdk/build/include"

rm -fr obj
mkdir -p obj

if [ ! -d $PC1 ]; then
    cd $PC1
    git clone https://github.com/supranational/filecoin_pc1.git
    cd -
fi
if [ ! -d "sppark" ]; then
    git clone https://github.com/supranational/sppark.git
fi
if [ ! -d "blst" ]; then
    git clone https://github.com/supranational/blst.git
    cd blst
    ./build.sh
    cd ..
fi

c++ $CPPFLAGS $INCLUDE -o obj/debug_helpers.o -c $PC1/src/util/debug_helpers.cpp &
c++ $CPPFLAGS $INCLUDE -o obj/column_reader.o -c src/column_reader.cpp &
$NVCC -g -std=c++17 $INCLUDE -DNO_SPDK -D__ADX__ -Xcompiler -Wno-subobject-linkage -Xcompiler -O2 \
      -Isppark -Iblst/src -arch=sm_75 -dc cuda/pc2.cu -o obj/pc2.o &
$NVCC -g -std=c++17 $INCLUDE -DNO_SPDK -D__ADX__ -Xcompiler -Wno-subobject-linkage -Xcompiler -O2 \
      -Isppark -Iblst/src -arch=sm_75 -dlink cuda/pc2.cu -o obj/link.o &

wait

c++ -I$PC1/src -Isppark -Iblst/src \
    -g -o pc2 src/main.cpp obj/debug_helpers.o obj/column_reader.o \
    obj/pc2.o obj/link.o \
    $LDFLAGS -Lblst -lblst -L$CUDA/lib64 -lcudart

