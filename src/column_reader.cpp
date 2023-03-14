// Copyright Supranational LLC
#include <stdio.h>
#include <unistd.h>
#include <string>
#include <set>
#include <mutex>
#include <thread>
#include <spdk/stdinc.h>
#include "spdk/nvme.h"
#include "pc2.hpp"
#include <nvme/nvme.hpp>
#include <util/stats.hpp>
#include <util/util.hpp>

std::mutex print_mtx;

#include <sealing/node_rw_t.hpp>
#include <util/debug_helpers.hpp>

int g_spdk_error = 0;

typedef batch_t<node_io_t, 1> node_io_batch_t;

// Encapsulate the SPDK node reader 
class streaming_node_reader_t {
  nvme_controllers_t* controllers;
  // Fixed size FIFOs for requests to the parent reader
  mt_fifo_t<node_io_batch_t> node_read_fifo;
  node_rw_t<node_io_batch_t>* node_reader;
  atomic<bool> terminator;
  std::thread reader_thread;
  
public:
  streaming_node_reader_t(set<string>& allowed_nvme, int core_num)
    : terminator(false)
  {
    // Initialize SPDK
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "nvme";
    int rc = spdk_env_init(&opts);
    if (rc < 0) {
      fprintf(stderr, "Unable to initialize SPDK env\n");
      abort();
    }

    assert (allowed_nvme.size() == NUM_CONTROLLERS);

    controllers = new nvme_controllers_t(allowed_nvme);
    controllers->init(1); // qpairs
    controllers->sort();

    printf("Layer disks\n");
    for (size_t i = 0; i < controllers->size(); i++) {
      printf("  %s\n", (*controllers)[i].get_name().c_str());
    }
    if (controllers->size() != NUM_CONTROLLERS) {
      printf("ERROR: only %ld controllers found, expected %ld\n",
             controllers->size(), NUM_CONTROLLERS);
      exit(1);
    }

    // Streaming reads
    SPDK_ASSERT(node_read_fifo.create("node_read_fifo", nvme_controller_t::queue_size));
    node_reader = new node_rw_t<node_io_batch_t>(terminator, *controllers, node_read_fifo, 0, 0);
    reader_thread = std::thread([&, core_num]() {
      printf("Setting affinity for node reader to core %d\n", core_num);
      set_core_affinity(core_num);
      assert(node_reader->process() == 0);
    });    
  }
  ~streaming_node_reader_t() {
    terminator = true;
    reader_thread.join();
    delete node_reader;
    delete controllers;
  }
  int load_layers(page_t* pages, node_id_t start_node, size_t node_count,
                  atomic<uint64_t>* valid, size_t* valid_count,
                  node_io_batch_t* node_ios) {
    size_t total_pages = LAYER_COUNT * node_count / NODES_PER_PAGE;
    size_t node_read_fifo_free;
    while (true) {
      node_read_fifo_free = node_read_fifo.free_count();
      if (node_read_fifo_free >= total_pages) {
        break;
      }
    }

    // Valid counter
    valid->store(0);

    node_id_t node_to_read = start_node;
    assert (node_to_read.layer() == 0);

    size_t idx = 0;
    uint32_t cur_layer = start_node.layer();
    for (size_t i = 0; i < LAYER_COUNT; i++) {
      for (size_t j = 0; j < node_count; j += NODES_PER_PAGE) {
        node_io_t& io = node_ios[idx].batch[0];
        io.type = node_io_t::type_e::READ;
        io.node = node_to_read;
        io.valid = valid;
        io.tracker.buf = (uint8_t*)&pages[idx];
          
        SPDK_ERROR(node_read_fifo.enqueue(&node_ios[idx]));

        node_to_read += NODES_PER_PAGE;
        idx++;
      }
      // Increment the layer
      cur_layer++;
      node_to_read = node_id_t(cur_layer, start_node.node());
    }
    *valid_count = total_pages;
    return 0;
  }
};

column_reader_t::column_reader_t(size_t _batch_size, size_t _num_batches, int core_num)
  : batch_size(_batch_size), num_batches(_num_batches)
{
  // Construct a set of NVMEs to use for sealing
  // TODO: duplicated from pc1
  set<string> allowed_nvme;
  allowed_nvme.insert("0000:29:00.0");
  allowed_nvme.insert("0000:2a:00.0");
  allowed_nvme.insert("0000:2b:00.0");
  allowed_nvme.insert("0000:2c:00.0");
  allowed_nvme.insert("0000:62:00.0");
  allowed_nvme.insert("0000:63:00.0");
  allowed_nvme.insert("0000:64:00.0");
  allowed_nvme.insert("0000:65:00.0");
  allowed_nvme.insert("0000:01:00.0");
  allowed_nvme.insert("0000:02:00.0");
  allowed_nvme.insert("0000:03:00.0");
  allowed_nvme.insert("0000:04:00.0");
  allowed_nvme.insert("0000:41:00.0");
  allowed_nvme.insert("0000:43:00.0");
  allowed_nvme.insert("0000:44:00.0");
  
  assert (allowed_nvme.size() == NUM_CONTROLLERS);

  node_reader = new streaming_node_reader_t(allowed_nvme, core_num);
  
  assert (batch_size % NODES_PER_PAGE == 0);
  // Pages in a single batch
  size_t num_pages = batch_size / NODES_PER_PAGE * LAYER_COUNT;

  page_data = (uint8_t*)spdk_dma_zmalloc(sizeof(page_t) * num_pages * num_batches, PAGE_SIZE, NULL);
  assert (page_data != nullptr);
}

column_reader_t::~column_reader_t() {
  delete node_reader;
  spdk_free(page_data);
}

uint8_t* column_reader_t::get_buffer(size_t& bytes) {
  // Pages in a single batch
  size_t num_pages = batch_size / NODES_PER_PAGE * LAYER_COUNT;
  bytes = sizeof(page_t) * num_pages * num_batches;
  return page_data;
}

uint8_t* column_reader_t::get_buffer_id(size_t id) {
  assert (id <  num_batches);
  size_t num_pages = batch_size / NODES_PER_PAGE * LAYER_COUNT;
  return &page_data[sizeof(page_t) * num_pages * id];
}

void* column_reader_t::alloc_node_ios() {
  size_t total_pages = LAYER_COUNT * batch_size / NODES_PER_PAGE;
  return malloc(sizeof(node_io_batch_t) * total_pages);
}

void column_reader_t::free_node_ios(void* node_ios) {
  free(node_ios);
}

// Starting at 'node', read all columns of 'num_nodes' nodes
uint8_t* column_reader_t::read_columns(uint64_t node, size_t buffer_id,
                                       atomic<uint64_t>* valid, size_t* valid_count,
                                       void* node_ios) {
  assert (node % NODES_PER_PAGE == 0);
  uint8_t* buf = get_buffer_id(buffer_id);
  node_reader->load_layers((page_t*)buf, node, batch_size,
                           valid, valid_count, (node_io_batch_t*)node_ios);
  return buf;
}
