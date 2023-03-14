// Copyright Supranational LLC

#include <stdio.h>
#include <stdint.h>
#include <set>
#include <string>
#include <assert.h>

#include <ff/bls12-381.hpp>
#include "pc2.hpp"
#include <util/util.hpp>

int main(int argc, char** argv) {
  int node_reader_core = 8;
  int hasher_core = 9;
  int write_core = 10;
  set_core_affinity(hasher_core);

  // Total number of streams across all GPUs
  size_t stream_count = 64;
  //size_t stream_count = 8;

  // Batch size in nodes. Each node includes all parallel sectors
  size_t batch_size = 64;

  // Nodes to read per partition
  size_t nodes_to_read = (1<<30) / PARTITIONS;
  //size_t nodes_to_read = 64 * 8 * 8 * 8 * 8;
  
  // Allocate storage for 2x the streams to support tree-c and tree-r
  column_reader_t* column_reader = new column_reader_t(batch_size, stream_count * 2,
                                                       node_reader_core);

  pc2_hash(*column_reader, nodes_to_read, batch_size, stream_count, write_core);

  return 0;
}
