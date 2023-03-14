// Copyright Supranational LLC

#include <map>
#include <fstream>
#include <iostream>
#include "poseidon.hpp"

Poseidon::Poseidon(const int arity) :
  arity_(arity),
  half_full_rounds_(4),
  t_(arity + 1),
  domain_tag_((1 << arity) - 1) {

  const std::map<int, int> partial_rounds_map = {
    {2, 55},
    {4, 56},
    {8, 57},
    {11, 57},
    {16, 59},
    {24, 59},
    {36, 60}
  };
  std::map<int, int>::const_iterator map_res = partial_rounds_map.find(arity);
  partial_rounds_ = map_res->second;

  // Read the constants file
  std::ifstream file(std::string("constants/constants_") +
                     std::to_string(arity), std::ios::binary | std::ios::ate);

  if (!file.is_open()) {
    std::cout << "Failed to open constants file" << std::endl;
    exit(1);
  }

  constants_size_ = file.tellg();
  constants_file_ = static_cast<fr_t*>(malloc(constants_size_));
  file.seekg(0);
  file.read(reinterpret_cast<char*>(constants_file_), constants_size_);
  file.close();

  // Assign constants pointers to location in buffer
  // round_constants_   = constants_file_;
  // mds_matrix_        = round_constants_ +
  //                      (t_ * half_full_rounds_ * 2) + 
  //                      partial_rounds_;
  // pre_sparse_matrix_ = mds_matrix_ + (t_ * t_);
  // sparse_matrices_   = pre_sparse_matrix_ + (t_ * t_);
  AssignPointers(constants_file_,
                 &round_constants_, &mds_matrix_,
                 &pre_sparse_matrix_, &sparse_matrices_);
}

Poseidon::~Poseidon() {
  free(constants_file_);
}

void Poseidon::AssignPointers(fr_t* constants_file, 
                              fr_t** round_constants, fr_t** mds_matrix,
                              fr_t** pre_sparse_matrix, fr_t** sparse_matrices) {
  *round_constants   = constants_file;
  *mds_matrix        = *round_constants +
                       (t_ * half_full_rounds_ * 2) + 
                       partial_rounds_;
  *pre_sparse_matrix = *mds_matrix + (t_ * t_);
  *sparse_matrices   = *pre_sparse_matrix + (t_ * t_);
}

void Poseidon::Hash(uint8_t* out, const uint8_t* in) {
  fr_t* elements = static_cast<fr_t*>(malloc(sizeof(fr_t) * t_));

  elements[0] = domain_tag_;

  for (int i = 0; i < t_ - 1; ++i) {
    elements[i + 1].to(in + (i * 32), 32, true);
  }

  for (int i = 0; i < t_; ++i) {
    elements[i] += round_constants_[i];
  }

  int rk_offset = t_;
  int current_round = 0;

  for (int i = 0; i < half_full_rounds_; ++i) {
    FullRound(elements, rk_offset, current_round);
  }

  for (int i = 0; i < partial_rounds_; ++i) {
    PartialRound(elements, rk_offset, current_round);
  }

  for (int i = 0; i < half_full_rounds_ - 1; ++i) {
    FullRound(elements, rk_offset, current_round);
  }

  LastFullRound(elements, mds_matrix_);

  elements[1].to_scalar(*((fr_t::pow_t*)out));

  free(elements);
}

void Poseidon::QuinticSBox(fr_t& element, const fr_t& round_constant) {
  element ^= 5;
  element += round_constant;
}

void Poseidon::MatrixMul(fr_t* elements, const fr_t* matrix) {
  fr_t* tmp = static_cast<fr_t*>(malloc(sizeof(fr_t) * t_));

  for (int i = 0; i < t_; ++i) {
    tmp[i] = elements[0] * matrix[i];

    for (int j = 1; j < t_; j++) {
      tmp[i] += elements[j] * matrix[j * t_ + i];
    }
  }

  for (int i = 0; i < t_; ++i) {
    elements[i] = tmp[i];
  }

  free(tmp);
}

void Poseidon::SparseMatrixMul(fr_t* elements, const fr_t* sparse_matrix) {
  fr_t element0 = elements[0];

  elements[0] *= sparse_matrix[0];
  for (int i = 1; i < t_; ++i) {
    elements[0] += elements[i] * sparse_matrix[i];
  }

  for (int i = 1; i < t_; ++i) {
    elements[i] += element0 * sparse_matrix[t_ + i - 1];
  }
}

void Poseidon::RoundMatrixMul(fr_t* elements, const int current_round) {
  if (current_round == 3) {
    MatrixMul(elements, pre_sparse_matrix_);
  }
  else if ((current_round > 3) &&
           (current_round < half_full_rounds_ + partial_rounds_)) {
    int index = current_round - half_full_rounds_;
    SparseMatrixMul(elements, sparse_matrices_ + (t_ * 2 - 1) * index);
  }
  else {
    MatrixMul(elements, mds_matrix_);
  }
}

void Poseidon::FullRound(fr_t* elements, int& rk_offset, int& current_round) {
  for (int i = 0; i < t_; ++i) {
    QuinticSBox(elements[i], round_constants_[rk_offset + i]);
  }
  rk_offset += t_;

  RoundMatrixMul(elements, current_round);
  current_round++;
}

void Poseidon::LastFullRound(fr_t* elements, const fr_t* mds_matrix) {
  for (int i = 0; i < t_; ++i) {
    elements[i] ^= 5;
  }

  MatrixMul(elements, mds_matrix);
}

void Poseidon::PartialRound(fr_t* elements, int& rk_offset,
                            int& current_round) {
  QuinticSBox(elements[0], round_constants_[rk_offset]);
  rk_offset += 1;

  RoundMatrixMul(elements, current_round);
  current_round++;
}
