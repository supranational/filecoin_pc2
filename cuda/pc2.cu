// Copyright Supranational LLC

#include "poseidon.cu"
#include <util/debug_helpers.hpp>
#include "host_ptr_t.hpp"

#ifndef __CUDA_ARCH__

#include <filesystem>
#include <chrono>
#include "../src/planner.cpp"
#include "pc2.cuh"
#include "cuda_lambda_t.hpp"
#include <util/util.hpp>

pc2_t::pc2_t(column_reader_t& _reader, size_t _nodes_to_read, size_t _batch_size,
             size_t _stream_count, int _write_core) :
  reader(_reader),
  nodes_to_read(_nodes_to_read),
  batch_size(_batch_size),
  tree_c_address(NODE_COUNT / PARTITIONS, TREE_ARITY, NODE_SIZE, 0),
  tree_r_address(NODE_COUNT / PARTITIONS, TREE_ARITY, NODE_SIZE, TREE_R_LAYER_SKIPS + 1),
  stream_count(_stream_count),
  gpu_results_c(_batch_size * PARALLEL_SECTORS / TREE_ARITY * stream_count),
  gpu_results_r(_batch_size * PARALLEL_SECTORS / TREE_ARITY * stream_count),
  host_buf_storage(num_host_bufs * batch_size * PARALLEL_SECTORS),
  write_core(_write_core)
{
  assert (nodes_to_read % stream_count == 0);

  // Open all tree-c and tree-r files
  const char* tree_c_filename_template = "results/sc-02-data-tree-c-s-%03ld-%ld.dat";
  const char* tree_r_filename_template = "results/sc-02-data-tree-r-last-s-%03ld-%ld.dat";
  std::filesystem::create_directory("results");
  for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
    for (size_t j = 0; j < PARTITIONS; j++) {
      const size_t MAX = 256;
      char fname[MAX];

      // tree-c
      snprintf(fname, MAX, tree_c_filename_template, i, j);
      tree_c_fds[i][j] = open(fname, O_RDWR | O_CREAT, (mode_t)0664);
      assert (tree_c_fds[i][j] != -1);
      lseek(tree_c_fds[i][j], tree_c_address.data_size() - 1, SEEK_SET);
      assert (write(tree_c_fds[i][j], "", 1) != -1);
      tree_c_files[i][j] = (uint8_t*)mmap(NULL, tree_c_address.data_size(),
                                          PROT_WRITE, MAP_SHARED, tree_c_fds[i][j], 0);
      if (tree_c_files[i][j] == MAP_FAILED) {
        perror("mmap failed for tree_c file");
        exit(1);
      }
      assert(madvise(tree_c_files[i][j], tree_c_address.data_size(), MADV_RANDOM) == 0);

      // tree-r
      snprintf(fname, MAX, tree_r_filename_template, i, j);
      tree_r_fds[i][j] = open(fname, O_RDWR | O_CREAT, (mode_t)0664);
      assert (tree_r_fds[i][j] != -1);
      lseek(tree_r_fds[i][j], tree_r_address.data_size() - 1, SEEK_SET);
      assert (write(tree_r_fds[i][j], "", 1) != -1);
      tree_r_files[i][j] = (uint8_t*)mmap(NULL, tree_r_address.data_size(),
                                          PROT_WRITE, MAP_SHARED, tree_r_fds[i][j], 0);
      if (tree_r_files[i][j] == MAP_FAILED) {
        perror("mmap failed for tree_r file");
        exit(1);
      }
      assert(madvise(tree_r_files[i][j], tree_r_address.data_size(), MADV_RANDOM) == 0);
    }
  }
  
  // Compute the final offset in the file for GPU data
  tree_address_t final_tree(stream_count, TREE_ARITY, sizeof(fr_t), 0);
  final_gpu_offset_c = tree_c_address.data_size() - final_tree.data_size();
  final_gpu_offset_r = tree_r_address.data_size() - final_tree.data_size();

  // Compute an offset table used for multiple partitions
  size_t nodes_per_stream = nodes_to_read / stream_count;
  size_t layer_offset = nodes_per_stream;
  while (layer_offset >= TREE_ARITY) {
    layer_offsets_c.push_back(layer_offset);
    layer_offset /= TREE_ARITY;
  }

  layer_offset = nodes_per_stream;
  for (size_t i = 0; i < TREE_R_LAYER_SKIPS + 1; i++) {
    layer_offset /= TREE_ARITY;
  }
  while (layer_offset >= TREE_ARITY) {
    layer_offsets_r.push_back(layer_offset);
    layer_offset /= TREE_ARITY;
  }

  // Create GPU poseidon hashers and streams
  size_t resource_id = 0;
  for (size_t i = 0; i < ngpus(); i++) {
    auto& gpu = select_gpu(i);
    poseidon_columns.push_back(new PoseidonCuda<COL_ARITY_DT>(gpu));
    poseidon_trees.push_back(new PoseidonCuda<TREE_ARITY_DT>(gpu));
      
    for (size_t j = 0; j < stream_count / ngpus(); j++) {
      resources.push_back(new gpu_resource_t(resource_id, gpu, nodes_per_stream, batch_size,
                                             reader.alloc_node_ios()));
      resource_id++;
    }
  }

  // Register the page buffer with the CUDA driver
  size_t page_buffer_size;
  page_buffer = reader.get_buffer(page_buffer_size);
  cudaHostRegister(page_buffer, page_buffer_size, cudaHostRegisterDefault);

  // Set up host side buffers for returning data
  host_bufs.resize(num_host_bufs);
  host_buf_pool.create(num_host_bufs);
  host_buf_to_disk.create(num_host_bufs);
  for (size_t i = 0; i < num_host_bufs; i++) {
    host_bufs[i].data = &host_buf_storage[i * batch_size * PARALLEL_SECTORS];
    host_buf_pool.enqueue(&host_bufs[i]);
  }
}

pc2_t::~pc2_t() {
  for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
    for (size_t j = 0; j < PARTITIONS; j++) {
      munmap(tree_c_files[i][j], tree_c_address.data_size());
      close(tree_c_fds[i][j]);

      munmap(tree_r_files[i][j], tree_r_address.data_size());
      close(tree_r_fds[i][j]);
    }
  }
  while (resources.size() > 0) {
    gpu_resource_t* r = resources.back();
    select_gpu(r->gpu);
      
    delete r;
    resources.pop_back();
  }
  for (size_t i = 0; i < ngpus(); i++) {
    delete poseidon_columns[i];
    delete poseidon_trees[i];
  }
  cudaHostUnregister(page_buffer);
}

void pc2_t::hash() {
  auto start = chrono::high_resolution_clock::now();
  for (size_t partition = 0; partition < PARTITIONS; partition++) {
    auto pstart_gpu = chrono::high_resolution_clock::now();
    hash_gpu(partition);
    auto pstop_gpu = chrono::high_resolution_clock::now();
    hash_cpu(partition, &(gpu_results_c[0]), tree_c_files, final_gpu_offset_c);
    hash_cpu(partition, &(gpu_results_r[0]), tree_r_files, final_gpu_offset_r);
    auto pstop_cpu = chrono::high_resolution_clock::now();
    uint64_t secs_gpu = std::chrono::duration_cast<
      std::chrono::seconds>(pstop_gpu - pstart_gpu).count();
    uint64_t secs_cpu = std::chrono::duration_cast<
      std::chrono::seconds>(pstop_cpu - pstop_gpu).count();
    printf("Partition %ld took %ld seconds (gpu %ld, cpu %ld)\n",
           partition, secs_gpu + secs_cpu, secs_gpu, secs_cpu);
  }
  auto stop = chrono::high_resolution_clock::now();
  uint64_t secs = std::chrono::duration_cast<
    std::chrono::seconds>(stop - start).count();

  size_t total_page_reads = nodes_to_read * PARTITIONS / NODES_PER_PAGE * LAYER_COUNT;
  printf("pc2 took %ld seconds utilizing %0.1lf iOPS\n",
         secs, (double)total_page_reads / (double)secs);
}

void pc2_t::hash_gpu(size_t partition) {
  assert (stream_count % ngpus() == 0);

  nodes_per_stream = nodes_to_read / stream_count;

  thread_pool_t pool(1);

  for (size_t i = 0; i < resources.size(); i++) {
    resources[i]->reset();
  }
  
  // Start a thread to process writes to disk
  atomic<bool> terminate = false;
  atomic<bool> disk_writer_done = false;
  pool.spawn([this, &terminate, &disk_writer_done]() {
    // TODO: enable config of core
    set_core_affinity(write_core);

    const size_t batch_size = 32;
    buf_to_disk_t* to_disk_batch[batch_size];
    size_t count = 0;
    
    while(!terminate || host_buf_to_disk.size() > 0) {
      buf_to_disk_t* to_disk = host_buf_to_disk.dequeue();
      if (to_disk != nullptr) {
        for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
          memcpy(to_disk->dst[i], to_disk->src[i], to_disk->size);
        }
        host_buf_pool.enqueue(to_disk);
      }
    }
    disk_writer_done = true;
  });
  
  bool all_done = false;
  cuda_lambda_t cuda_notify(1);
  in_ptrs_d<TREE_ARITY> in_d;
  buf_to_disk_t* to_disk;
  buf_to_disk_t* to_disk_r;
  fr_t* fr;

  while (!all_done) {
    all_done = true;
    for (size_t resource_num = 0; resource_num < resources.size(); resource_num++) {
      gpu_resource_t& resource = *resources[resource_num];
      select_gpu(resource.gpu);
      int gpu_id = resource.gpu.id();
      fr_t* host_buf_c = (fr_t*)reader.get_buffer_id(resource.id);
      fr_t* host_buf_r = (fr_t*)reader.get_buffer_id(resources.size() + resource.id);

      if (resource.state != ResourceState::DONE) {
        all_done = false;
      }

      fr_t* out_c_d = nullptr;
      fr_t* out_r_d = nullptr;
      size_t layer_offset;
      node_id_t addr;
      size_t offset_c;
      size_t offset_r;
      uint64_t start_node;
      bool write_tree_r;

      // Device storage for the hash result
      if (resource.work_c.buf != nullptr) {
        out_c_d = &(*resource.work_c.buf)[0];
        out_r_d = &(*resource.work_r.buf)[0];
      }
      
      switch (resource.state) {
      case ResourceState::DONE:
        // Nothing
        break;
        
      case ResourceState::IDLE:
        // Initiate data read
        resource.last = !resource.scheduler_c.next([](work_item_t<gpu_buffer_t>& w) {},
                                                   &resource.work_c);
        resource.scheduler_r.next([](work_item_t<gpu_buffer_t>& w) {},
                                  &resource.work_r);
        if (resource.work_c.is_leaf) {
#ifdef DISABLE_FILE_READS
          resource.state = ResourceState::HASH_COLUMN;
          resource.column_data = reader.get_buffer_id(resource_num);
#else
          resource.state = ResourceState::DATA_READ;
#endif
        } else {
          resource.state = ResourceState::HASH_LEAF;
        }
        break;

      case ResourceState::DATA_READ:
        // Initiate the next data read
        start_node = ((uint64_t)resource.work_c.idx.node() * batch_size +
                      nodes_per_stream * resource.id +
                      partition * nodes_to_read);
        resource.column_data = reader.read_columns
          (start_node, resource.id, &resource.valid, &resource.valid_count, resource.node_ios);
        resource.state = ResourceState::DATA_WAIT;
        break;

      case ResourceState::DATA_WAIT:
        if (resource.valid.load() == resource.valid_count) {
          resource.state = ResourceState::HASH_COLUMN;
        }
        break;
      
      case ResourceState::HASH_COLUMN:
        to_disk = host_buf_pool.dequeue();
        if (to_disk == nullptr) {
          break;
        }
        
        resource.stream.HtoD(&resource.column_data_d[0], resource.column_data, resource.batch_elements);

        // Hash the columns
        poseidon_columns[gpu_id]->hash_batch_device
          (out_c_d, &resource.column_data_d[0], &resource.aux_d[0],
           batch_size * PARALLEL_SECTORS, PARALLEL_SECTORS,
           resource.stream, true, false, true, true);

        // Initiate copy of the hashed data from GPU
        fr = to_disk->data;
        resource.stream.DtoH(fr, out_c_d, batch_size * PARALLEL_SECTORS);

        // Initiate transfer of tree-c data to files
        layer_offset = layer_offsets_c[resource.work_c.idx.layer() - 1];
        addr = node_id_t(resource.work_c.idx.layer() - 1,
                         resource.work_c.idx.node() * batch_size + layer_offset * resource_num);
        offset_c = tree_c_address.address(addr);

        for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
          to_disk->dst[i] = (fr_t*)&tree_c_files[i][partition][offset_c];
          to_disk->src[i] = &to_disk->data[i * batch_size];
        }
        to_disk->size = batch_size * sizeof(fr_t);

        resources[resource_num]->async_done = false;
        cuda_notify.schedule(resource.stream, [this, resource_num, to_disk, offset_c]() {
          this->host_buf_to_disk.enqueue(to_disk);
          resources[resource_num]->async_done = true;
        });

        resource.state = ResourceState::HASH_COLUMN_LEAVES;
        break;
        
      case ResourceState::HASH_COLUMN_LEAVES:
        if (!resources[resource_num]->async_done) {
          break;
        }
        to_disk = host_buf_pool.dequeue();
        if (to_disk == nullptr) {
          break;
        }

        // Hash tree-c
        poseidon_trees[gpu_id]->hash_batch_device
          (out_c_d, out_c_d, &resource.aux_d[0],
           batch_size * PARALLEL_SECTORS / TREE_ARITY, 1,
           resource.stream, false, false, true, true);

        // Hash tree-r using layer 11
        poseidon_trees[gpu_id]->hash_batch_device
          (out_r_d,
           &resource.column_data_d[batch_size * PARALLEL_SECTORS * (LAYER_COUNT - 1)],
           &resource.aux_d[0],
           batch_size * PARALLEL_SECTORS / TREE_ARITY,
           PARALLEL_SECTORS,
           resource.stream, false, true, true, true);

        // Initiate copy of the hashed data from GPU, reusing the host side column buffer
        resource.stream.DtoH(&to_disk->data[0], out_c_d,
                             batch_size * PARALLEL_SECTORS / TREE_ARITY);
        
        // Initiate transfer of tree-c data to files
        layer_offset = layer_offsets_c[resource.work_c.idx.layer()];
        addr = node_id_t(resource.work_c.idx.layer(),
                         resource.work_c.idx.node() * batch_size / TREE_ARITY +
                         layer_offset * resource_num);
        offset_c = tree_c_address.address(addr);
        for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
          to_disk->dst[i] = (fr_t*)&tree_c_files[i][partition][offset_c];
          to_disk->src[i] = &to_disk->data[i * TREE_ARITY];
        }
        to_disk->size = TREE_ARITY * sizeof(fr_t);
        
        resources[resource_num]->async_done = false;
        cuda_notify.schedule(resource.stream, [this, resource_num, to_disk]() {
          this->host_buf_to_disk.enqueue(to_disk);
          resources[resource_num]->async_done = true;
        });

        resource.state = ResourceState::HASH_WAIT;
        break;

      case ResourceState::HASH_LEAF:
        if (host_buf_pool.size() < 2) {
          break;
        }
        to_disk = host_buf_pool.dequeue();
        assert (to_disk != nullptr);
        
        // Hash tree-c
        for (size_t i = 0; i < TREE_ARITY; i++) {
          in_d.ptrs[i] = &(*resource.work_c.inputs[i])[0];
        }
        poseidon_trees[gpu_id]->hash_batch_device_ptrs
          (out_c_d, in_d, &resource.aux_d[0],
           batch_size * PARALLEL_SECTORS / TREE_ARITY, 1,
           resource.stream, false, false, true, true);

        // Hash tree-r 
        for (size_t i = 0; i < TREE_ARITY; i++) {
          in_d.ptrs[i] = &(*resource.work_r.inputs[i])[0];
        }
        poseidon_trees[gpu_id]->hash_batch_device_ptrs
          (out_r_d, in_d, &resource.aux_d[0],
           batch_size * PARALLEL_SECTORS / TREE_ARITY, 1,
           resource.stream, false, false, true, true);

        // Initiate copy of the hashed data
        resource.stream.DtoH(&to_disk->data[0], out_c_d, batch_size * PARALLEL_SECTORS / TREE_ARITY);
        if (resource.last) {
          // Stash the final result in a known place
          fr_t* host_buf_c = (fr_t*)reader.get_buffer_id(resource_num);
          CUDA_OK(cudaMemcpyAsync(host_buf_c, &to_disk->data[0],
                                  batch_size * PARALLEL_SECTORS / TREE_ARITY * sizeof(fr_t),
                                  cudaMemcpyHostToHost, resource.stream));
        }

        // Compute offsets in the output files - tree-c
        layer_offset = layer_offsets_c[resource.work_c.idx.layer()];
        addr = node_id_t(resource.work_c.idx.layer(),
                         resource.work_c.idx.node() * batch_size / TREE_ARITY +
                         layer_offset * resource_num);
        offset_c = tree_c_address.address(addr);
        for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
          to_disk->dst[i] = (fr_t*)&tree_c_files[i][partition][offset_c];
          to_disk->src[i] = &to_disk->data[i * TREE_ARITY];
        }
        to_disk->size = TREE_ARITY * sizeof(fr_t);

        // tree-r
        write_tree_r = resource.work_r.idx.layer() > TREE_R_LAYER_SKIPS;
        if (write_tree_r) {
          to_disk_r = host_buf_pool.dequeue();
          assert (to_disk_r != nullptr);
          resource.stream.DtoH(&to_disk_r->data[0], out_r_d,
                               batch_size * PARALLEL_SECTORS / TREE_ARITY);
          if (resource.last) {
            // Stash the final result in a known place
            fr_t* host_buf_r = (fr_t*)reader.get_buffer_id(resources.size() + resource_num);
            CUDA_OK(cudaMemcpyAsync(host_buf_r, &to_disk_r->data[0],
                                    batch_size * PARALLEL_SECTORS / TREE_ARITY * sizeof(fr_t),
                                    cudaMemcpyHostToHost, resource.stream));
          }

          layer_offset = layer_offsets_r[resource.work_r.idx.layer() - TREE_R_LAYER_SKIPS - 1];
          addr = node_id_t(resource.work_r.idx.layer() - TREE_R_LAYER_SKIPS - 1,
                           resource.work_r.idx.node() * batch_size / TREE_ARITY +
                           layer_offset * resource_num);
          offset_r = tree_r_address.address(addr);
          for (size_t i = 0; i < PARALLEL_SECTORS; i++) {
            to_disk_r->dst[i] = (fr_t*)&tree_r_files[i][partition][offset_r];
            to_disk_r->src[i] = &to_disk_r->data[i * TREE_ARITY];
          }
          to_disk_r->size = TREE_ARITY * sizeof(fr_t);
        }
        
        // Initiate transfer of data to files
        resources[resource_num]->async_done = false;
        cuda_notify.schedule(resource.stream, [this, resource_num,
                                               to_disk, to_disk_r, write_tree_r]() {
          this->host_buf_to_disk.enqueue(to_disk);
          if (write_tree_r) {
            this->host_buf_to_disk.enqueue(to_disk_r);
          }
          resources[resource_num]->async_done = true;
        });

        resource.state = ResourceState::HASH_WAIT;
        break;
      
      case ResourceState::HASH_WAIT:
        if (resource.async_done.load() == true) {
          if (resource.last) {
            resource.state = ResourceState::DONE;
          } else {
            resource.state = ResourceState::IDLE;
          }
        }
        break;

      default:
        abort();
      }
    }
  }
  for (size_t resource_num = 0; resource_num < stream_count; resource_num++) {
    resources[resource_num]->stream.sync();
  }

  terminate = true;

  size_t stride = batch_size * PARALLEL_SECTORS / TREE_ARITY;
  for (size_t resource_num = 0; resource_num < stream_count; resource_num++) {
    fr_t* host_buf_c = (fr_t*)reader.get_buffer_id(resource_num);
    memcpy(&gpu_results_c[resource_num * stride],
           &host_buf_c[0], batch_size * PARALLEL_SECTORS / TREE_ARITY * sizeof(fr_t));
  }
  for (size_t resource_num = 0; resource_num < stream_count; resource_num++) {
    fr_t* host_buf_r = (fr_t*)reader.get_buffer_id(resources.size() + resource_num);
    memcpy(&gpu_results_r[resource_num * stride],
           &host_buf_r[0], batch_size * PARALLEL_SECTORS / TREE_ARITY * sizeof(fr_t));
  }

  // Really only need this at the last partition...
  while (!disk_writer_done) {}
}

void pc2_t::hash_cpu(size_t partition, fr_t* input,
                     uint8_t* tree_files[PARALLEL_SECTORS][PARTITIONS],
                     size_t file_offset) {
  const size_t nodes_to_hash = stream_count; // Number of GPU streams
  
  tree_address_t final_tree(nodes_to_hash, TREE_ARITY, sizeof(fr_t), 0);

  Poseidon hasher(TREE_ARITY);

  auto hash_func = [this, &hasher, &final_tree, input, partition, tree_files, file_offset]
    (work_item_t<host_buffer_t>& w) {
    node_id_t addr(w.idx.layer() - 1, w.idx.node());
    size_t offset = final_tree.address(addr) + file_offset;

    const size_t stride = TREE_ARITY * sizeof(fr_t);
    if (w.is_leaf) {
      for (size_t sector = 0; sector < PARALLEL_SECTORS; sector++) {
        fr_t* out = &(*w.buf)[sector];
        fr_t in[TREE_ARITY];
        
        for (size_t i = 0; i < TREE_ARITY; i++) {
          in[i] = input[w.idx.node() * TREE_ARITY * PARALLEL_SECTORS +
                        sector * TREE_ARITY + i];
        }
        hasher.Hash((uint8_t*)out, (uint8_t*)in);
        memcpy(&tree_files[sector][partition][offset],
               &out[0], sizeof(fr_t));
      }
    } else {
      for (size_t sector = 0; sector < PARALLEL_SECTORS; sector++) {
        fr_t* out = &(*w.buf)[sector];
        fr_t in[TREE_ARITY];
        for (size_t i = 0; i < TREE_ARITY; i++) {
          in[i] = (*w.inputs[i])[sector];
        }
        hasher.Hash((uint8_t*)out, (uint8_t*)in);
        
        memcpy(&tree_files[sector][partition][offset],
               &out[0], sizeof(fr_t));
      }
    }
  };
  
  buffers_t<host_buffer_t> buffers(PARALLEL_SECTORS);
  scheduler_t<host_buffer_t> scheduler(nodes_to_hash, TREE_ARITY, buffers);
  scheduler.run(hash_func);
}

void pc2_hash(column_reader_t& reader, size_t nodes_to_read, size_t batch_size,
              size_t stream_count, int write_core) {
  pc2_t pc2(reader, nodes_to_read, batch_size, stream_count, write_core);
  pc2.hash();
}

#endif
