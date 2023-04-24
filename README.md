# Filecoin Optimized Sealing

**This repository is under active development and is not expected to work in a general setting. It should be considered view only.**

This respository optimizes the filecoin sealing process for throughput per dollar. It is a companion to https://github.com/supranational/filecoin_pc1 and will read sealed PC1 column data from NVMe devices under SPDK to generate tree-r and tree-c files that are compatible with the existing sealing software. The current software is configured to seal 64 sectors in parallel.

Like filecoin_pc1 this is an initial prototype release.

What's coming:
- Integrate filecoin_pc1, filecoin_pc2, c1 into a single repository
- Support for pipelined and simultaneous jobs
- More seamless support for sealing a variety of parallel sector counts
- Simplified configuration

# Performance

Using the reference platform outlined below this software generates PC2 tree-r and tree-c files for 64 sectors in approximately 70 minutes.

# Reference Platform

We will specify a reference configuration with the final release of the software. Our current testing configuration consists of:
- Threadripper PRO 5975WX
- ASUS WRX80E SAGE Motherboard
- 256GB Memory
- 15 Samsung 7.68GB U.2 Drives
- 4 Supermicro AOC-SLG4-4E4T NVMe HBA 
- 2 NVIDIA GeForce RTX 3080

# Building

### Enable Huge Pages (1GB):

Though huge pages aren't strictly necessary for PC2, the software is configured to use them since it is expected to run alongside PC1.

```
sudo vi /etc/default/grub
GRUB_CMDLINE_LINUX_DEFAULT="default_hugepagesz=1G hugepagesz=1G hugepages=128"
GRUB_CMDLINE_LINUX="default_hugepagesz=1G hugepagesz=1G hugepages=128"
sudo update-grub
sudo reboot
```

You can confirm huge pages are enabled with:
```
grep Huge /proc/meminfo

# Look for:
HugePages_Total:     128
HugePages_Free:      128
```

Additionally you may need to enable huge pages after boot using:
```
sudo sysctl -w vm.nr_hugepages=128
```

### Build [SPDK](https://spdk.io/doc/getting_started.html) in the Parent Directory:

```
git clone --branch v22.09 https://github.com/spdk/spdk --recursive spdk-v22.09
cd spdk-v22.09/
sudo scripts/pkgdep.sh
./configure --with-virtio --with-vhost
make
sudo env NRHUGE=128 ./scripts/setup.sh
```

SPDK is used for directly controlling the NVMe drives.

### Update NVMe Controller Addresses in [src/column_reader.cpp](src/column_reader.cpp):

SPDK can be used to identify attached NVMe devices and their addresses with the following command:
```
sudo ./scripts/setup.sh status
```

For more extensive information about attached devices:
```
sudo ./build/examples/identify
```

This will show the NVMe disks (controllers) along with their addresses, which will resemble `0000:2c:00.0`. The address list in [src/column_reader.cpp](src/column_reader.cpp) must be updated to reflect the addresses that will be used.

In addition, if you have a different number of drives than the reference configuration, then you must update `NUM_CONTROLLERS` in [/filecoin_pc1/src/sealing/constants.hpp](/filecoin_pc1/src/sealing/constants.hpp)

**The drive configuration for PC2 must match PC1 for correct functionality.**

### Build the PC2 Binary

```
./build.sh
```

# Running

PC1 should be run prior to running PC2 to populate the DRG data on the NVMe drives. See https://github.com/supranational/filecoin_pc1 for related instructions.

Once this is completed, run:
```
sudo rm results/* # Delete any existing output files
sudo ./pc2
```

This will create a "results" directly that stores the PC2 output files. 
