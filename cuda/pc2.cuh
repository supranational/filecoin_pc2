// Copyright Supranational LLC
#ifndef __PC2_CUH__
#define __PC2_CUH__

#include <nvme/ring_t.hpp>
#include "../src/pc2.hpp"

// #define DISABLE_FILE_READS

// Class to compute the offset of serialized nodes in a tree.
class tree_address_t {
  size_t leaf_count;
  size_t arity;
  size_t node_size;
  std::vector<size_t> layer_offsets;
public:
  tree_address_t(size_t _leaf_count, size_t _arity, size_t _node_size, size_t layer_skips)
    : leaf_count(_leaf_count), arity(_arity), node_size(_node_size) {
    assert (leaf_count % arity == 0);
    size_t layer = 0;
    size_t offset = 0;
    size_t node_count = leaf_count;
    for (size_t i = 0; i < layer_skips; i++) {
      node_count /= arity;
    }
    while (node_count > 1) {
      layer_offsets.push_back(offset);
      layer++;
      offset += node_count * node_size;
      node_count /= arity;
    }
    layer_offsets.push_back(offset);
  }

  size_t address(node_id_t& node) {
    size_t base = layer_offsets[node.layer()];
    return base + (size_t)node.node() * node_size;
  }

  // Total tree size
  size_t data_size() {
    return layer_offsets.back() + node_size;
  }

  void print() {
    size_t layer = 0;
    for (auto i : layer_offsets) {
      printf("layer %2ld, offset 0x%08lx %ld\n", layer, i, i);
      layer++;
    }
  }
};

enum class ResourceState {
  IDLE,
  DATA_READ,
  DATA_WAIT,
  HASH_COLUMN,
  HASH_COLUMN_WRITE,
  HASH_COLUMN_LEAVES,
  HASH_LEAF,
  HASH_WAIT,
  DONE
};

typedef host_ptr_t<fr_t> host_buffer_t;

struct gpu_resource_t {
  size_t id;
  
  // GPU id
  const gpu_t& gpu;
  
  // GPU stream
  stream_t stream;

  // Storage for column input data
  size_t batch_elements;
  // Host side column (layer) data
  uint8_t* column_data;
  // Device side column (layer) data
  dev_ptr_t<fr_t> column_data_d;
  // Valid count from page reader
  atomic<uint64_t> valid;
  // Expected valid count for all pages
  size_t valid_count;
  // Node IO tracking storage for SPDK
  void* node_ios;

  // Hashed node buffers
  buffers_t<gpu_buffer_t> buffers;
      
  // Aux buffer
  dev_ptr_t<fr_t> aux_d;

  // Schedulers for tree-c and tree-r. They will follow identical paths
  // but this is a clean way to track input/output buffers through the tree.
  scheduler_t<gpu_buffer_t> scheduler_c;
  scheduler_t<gpu_buffer_t> scheduler_r;

  // Current work item
  work_item_t<gpu_buffer_t> work_c;
  work_item_t<gpu_buffer_t> work_r;
  // Flag set by Cuda when a hashing job is complete
  atomic<bool> async_done;
  
  ResourceState state;

  // Last hash is in progress
  bool last;

  gpu_resource_t(size_t _id,
                 const gpu_t& _gpu,
                 size_t _nodes_to_read,
                 size_t _batch_size,
                 void* _node_ios)
    : id(_id),
      gpu(_gpu),
      stream(gpu.id()),
      batch_elements(PARALLEL_SECTORS * LAYER_COUNT * _batch_size),
      column_data_d(batch_elements),
      node_ios(_node_ios),
      buffers(_batch_size * PARALLEL_SECTORS),
      // Size aux to hold the larger of the tree and column hash data
      aux_d(max(// tree aux size
                _batch_size * PARALLEL_SECTORS * COL_ARITY_DT,
                // column aux size
                _batch_size * PARALLEL_SECTORS / TREE_ARITY * TREE_ARITY_DT)),
      scheduler_c(_nodes_to_read / _batch_size, TREE_ARITY, buffers),
      scheduler_r(_nodes_to_read / _batch_size, TREE_ARITY, buffers),
      async_done(true),
      state(ResourceState::IDLE),
      last(false)
  {}
  void reset() {
    state = ResourceState::IDLE;
    last = false;
    async_done = true;
    scheduler_c.reset();
    scheduler_r.reset();
  }
};

struct buf_to_disk_t {
  fr_t* data;
  fr_t* dst[PARALLEL_SECTORS];
  fr_t* src[PARALLEL_SECTORS];
  size_t size;
};

class pc2_t {
private:
  column_reader_t& reader;
  size_t nodes_to_read;
  size_t batch_size;
  tree_address_t tree_c_address;
  tree_address_t tree_r_address;
  size_t stream_count;
  size_t nodes_per_stream;
  
  int tree_c_fds[PARALLEL_SECTORS][PARTITIONS];
  uint8_t* tree_c_files[PARALLEL_SECTORS][PARTITIONS];

  int tree_r_fds[PARALLEL_SECTORS][PARTITIONS];
  uint8_t* tree_r_files[PARALLEL_SECTORS][PARTITIONS];
  
  // Storage to transfer results from GPU to CPU for tree-c and tree-r
  host_ptr_t<fr_t> gpu_results_c;
  host_ptr_t<fr_t> gpu_results_r;

  // Final offset for GPU data in tree-c and tree-rfiles
  size_t final_gpu_offset_c;
  size_t final_gpu_offset_r;

  // Used to compute the actual node id for the various streams
  vector<size_t> layer_offsets_c;
  vector<size_t> layer_offsets_r;

  // GPU resources
  vector<PoseidonCuda<COL_ARITY_DT>*> poseidon_columns;
  vector<PoseidonCuda<TREE_ARITY_DT>*> poseidon_trees;
  vector<gpu_resource_t*> resources;

  // Buffer to store pages loaded from drives
  uint8_t* page_buffer;

  // Buffer pool for data coming back from GPU
  // The number of buffers should be large enough to hide disk IO delays.
  const size_t num_host_bufs = 1<<16;
  host_ptr_t<fr_t> host_buf_storage;
  vector<buf_to_disk_t> host_bufs;
  mt_fifo_t<buf_to_disk_t> host_buf_pool;
  mutex host_buf_mtx;
  mt_fifo_t<buf_to_disk_t> host_buf_to_disk;

  int write_core;
  
  void hash_gpu(size_t partition);
  void hash_cpu(size_t partition, fr_t* input,
                uint8_t* tree_files[PARALLEL_SECTORS][PARTITIONS],
                size_t file_offset);
  
public:
  pc2_t(column_reader_t& _reader, size_t _nodes_to_read, size_t _batch_size,
        size_t _stream_count, int _write_core);
  ~pc2_t();
  
  void hash();
};

#endif
