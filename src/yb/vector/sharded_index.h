// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#pragma once

#include "yb/vector/coordinate_types.h"
#include "yb/vector/distance.h"
#include "yb/vector/vector_index_if.h"

namespace yb::vectorindex {

// Allows creating multiple instances of the vector index so we can saturate the capacity of the
// test system.
template<IndexableVectorType Vector, ValidDistanceResultType DistanceResult>
class ShardedVectorIndex : public VectorIndexIf<Vector, DistanceResult> {
 public:
  ShardedVectorIndex(const VectorIndexFactory<Vector, DistanceResult>& factory,
                     size_t num_shards)
      : indexes_(num_shards), round_robin_counter_(0) {
    for (auto& index : indexes_) {
      index = factory();
    }
  }

  // Reserve capacity across all shards (each shard gets an equal portion, rounded up).
  Status Reserve(size_t num_vectors) override {
    size_t capacity_per_shard = (num_vectors + indexes_.size() - 1) / indexes_.size(); // Round up
    for (auto& index : indexes_) {
      RETURN_NOT_OK(index->Reserve(capacity_per_shard));
    }
    return Status::OK();
  }

  // Insert a vector into the current shard using round-robin.
  Status Insert(VertexId vertex_id, const Vector& vector) override {
    size_t current_index = round_robin_counter_.fetch_add(1) % indexes_.size();
    return indexes_[current_index]->Insert(vertex_id, vector);
  }

  // Retrieve a vector from any shard.
  Result<Vector> GetVector(VertexId vertex_id) const override {
    for (const auto& index : indexes_) {
      auto v = VERIFY_RESULT(index->GetVector(vertex_id));
      if (!v.empty()) {
        return v;
      }
    }
    return Vector();  // Return an empty vector if not found.
  }

  // Search for the closest vectors across all shards.
  std::vector<VertexWithDistance<DistanceResult>> Search(
      const Vector& query_vector, size_t max_num_results) const override {
    std::vector<VertexWithDistance<DistanceResult>> all_results;
    for (const auto& index : indexes_) {
      auto results = index->Search(query_vector, max_num_results);
      all_results.insert(all_results.end(), results.begin(), results.end());
    }

    // Sort all_results by distance and keep the top max_num_results.
    std::sort(all_results.begin(), all_results.end(), [](const auto& a, const auto& b) {
      return a.distance < b.distance;
    });

    if (all_results.size() > max_num_results) {
      all_results.resize(max_num_results);
    }

    return all_results;
  }

  Status SaveToFile(const std::string& path) override {
    return STATUS(NotSupported, "Saving to file is not implemented for ShardedVectorIndex");
  }

  Status LoadFromFile(const std::string& path) override {
    return STATUS(NotSupported, "Loading from file is not implemented for ShardedVectorIndex");
  }

  DistanceResult Distance(const Vector& lhs, const Vector& rhs) const override {
    CHECK(!indexes_.empty());
    return indexes_[0]->Distance(lhs, rhs);
  }

 private:
  std::vector<VectorIndexIfPtr<Vector, DistanceResult>> indexes_;
  std::atomic<size_t> round_robin_counter_;  // Atomic counter for thread-safe round-robin insertion
};

}  // namespace yb::vectorindex
