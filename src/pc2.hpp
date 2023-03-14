// Copyright Supranational LLC
#ifndef __PC2_HPP__
#define __PC2_HPP__

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif
#include <sealing/constants.hpp>
#include <sealing/data_structures.hpp>

// TODO: where to get these contants?
const int COL_ARITY          = LAYER_COUNT;
const int COL_ARITY_DT       = COL_ARITY + 1;
const int TREE_ARITY_LG      = 3;
const int TREE_ARITY         = 1 << TREE_ARITY_LG;
const int TREE_ARITY_DT      = TREE_ARITY + 1;
const int PARTITIONS         = 8;
const int TREE_R_LAYER_SKIPS = 2;

class streaming_node_reader_t;

class column_reader_t {
  size_t batch_size;
  size_t num_batches;
  uint8_t* page_data;
  streaming_node_reader_t* node_reader;
  
public:
  column_reader_t(size_t batch_size, size_t num_batches, int core_num);
  ~column_reader_t();

  uint8_t* get_buffer(size_t& bytes);
  uint8_t* get_buffer_id(size_t id);

  void* alloc_node_ios();
  void free_node_ios(void* node_ios);
  
  // Starting at 'node', read all columns of 'num_nodes' nodes
  uint8_t* read_columns(uint64_t node, size_t buffer_id,
                        atomic<uint64_t>* valid, size_t* valid_count,
                        void* node_ios);
};

void pc2_hash(column_reader_t& _reader, size_t _nodes_to_read, size_t _batch_size,
              size_t _stream_count, int _write_core);

#endif
