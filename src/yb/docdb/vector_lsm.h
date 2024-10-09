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

#include "yb/common/hybrid_time.h"

#include "yb/dockv/key_bytes.h"

#include "yb/rocksdb/rocksdb_fwd.h"

#include "yb/rpc/rpc_fwd.h"

#include "yb/util/locks.h"

#include "yb/vector/vector_index_if.h"

namespace yb::docdb {

template<vectorindex::ValidDistanceResultType DistanceResult>
struct VectorLSMSearchEntry {
  DistanceResult distance;
  // base_table_key could be the encoded DocKey of the corresponding row in the base
  // (indexed) table, and the hybrid time of the vector insertion.
  KeyBuffer base_table_key;

  std::string ToString() const {
    return YB_STRUCT_TO_STRING(
        distance, (base_table_key, base_table_key.AsSlice().ToDebugHexString()));
  }
};

struct VectorLSMSearchOptions {
  size_t max_num_results;
};

template<vectorindex::IndexableVectorType Vector>
struct VectorLSMInsertEntry {
  vectorindex::VertexId vertex_id;
  KeyBuffer base_table_key;
  Vector vector;
};

template<vectorindex::IndexableVectorType Vector,
         vectorindex::ValidDistanceResultType DistanceResult>
struct VectorLSMOptions;

template<vectorindex::IndexableVectorType Vector,
         vectorindex::ValidDistanceResultType DistanceResult>
class VectorLSMInsertRegistry;

template<vectorindex::IndexableVectorType Vector,
         vectorindex::ValidDistanceResultType DistanceResult>
struct VectorLSMTypes {
  using Chunk = vectorindex::VectorIndexIf<Vector, DistanceResult>;
  using ChunkPtr = vectorindex::VectorIndexIfPtr<Vector, DistanceResult>;
  using ChunkFactory = vectorindex::VectorIndexFactory<Vector, DistanceResult>;
  using SearchResults = std::vector<VectorLSMSearchEntry<DistanceResult>>;
  using InsertEntry = VectorLSMInsertEntry<Vector>;
  using InsertEntries = std::vector<InsertEntry>;
  using Options = VectorLSMOptions<Vector, DistanceResult>;
  using InsertRegistry = VectorLSMInsertRegistry<Vector, DistanceResult>;
  using SearchOptions = VectorLSMSearchOptions;
  using VertexWithDistance = vectorindex::VertexWithDistance<DistanceResult>;
};

using BaseTableKeysBatch = std::vector<std::pair<vectorindex::VertexId, Slice>>;

class VectorLSMKeyValueStorage {
 public:
  virtual Status StoreBaseTableKeys(const BaseTableKeysBatch& batch, HybridTime write_time) = 0;
  virtual Result<KeyBuffer> ReadBaseTableKey(vectorindex::VertexId vertex_id) = 0;

  virtual ~VectorLSMKeyValueStorage() = default;
};

template<vectorindex::IndexableVectorType Vector,
         vectorindex::ValidDistanceResultType DistanceResult>
struct VectorLSMOptions {
  using Types = VectorLSMTypes<Vector, DistanceResult>;
  std::string storage_dir;
  typename Types::ChunkFactory chunk_factory;
  size_t points_per_chunk;
  VectorLSMKeyValueStorage* key_value_storage;
  rpc::ThreadPool* insert_thread_pool;
};

template<vectorindex::IndexableVectorType VectorType,
         vectorindex::ValidDistanceResultType DistanceResultType>
class VectorLSM {
 public:
  using DistanceResult = DistanceResultType;
  using Vector = VectorType;
  using Types = VectorLSMTypes<Vector, DistanceResult>;
  using Chunk = typename Types::Chunk;
  using ChunkPtr = typename Types::ChunkPtr;
  using ChunkFactory = typename Types::ChunkFactory;
  using SearchResults = typename Types::SearchResults;
  using InsertEntry = typename Types::InsertEntry;
  using InsertEntries = typename Types::InsertEntries;
  using Options = typename Types::Options;
  using InsertRegistry = typename Types::InsertRegistry;
  using SearchOptions = typename Types::SearchOptions;

  VectorLSM();
  ~VectorLSM();

  Status Open(Options options);

  Status Insert(std::vector<InsertEntry> entries, HybridTime write_time);

  Result<SearchResults> Search(const Vector& query_vector, const SearchOptions& options) const;

  size_t TEST_num_immutable_chunks() const;
  bool TEST_HasBackgroundInserts() const;

 private:
  // Saves the current mutable chunk to disk and creates a new one.
  Status RollChunk() REQUIRES(mutex_);

  Status CreateNewMutableChunk() REQUIRES(mutex_);

  Options options_;
  rocksdb::Env* const env_;

  size_t current_chunk_serial_no_ = 0;
  mutable rw_spinlock mutex_;
  ChunkPtr mutable_chunk_ GUARDED_BY(mutex_);
  std::vector<ChunkPtr> immutable_chunks_ GUARDED_BY(mutex_);
  size_t entries_in_mutable_chunks_ GUARDED_BY(mutex_) = 0;
  std::unique_ptr<InsertRegistry> insert_registry_;
};

template <template<class, class> class Factory, class VectorIndex>
using MakeChunkFactory =
    Factory<typename VectorIndex::Vector, typename VectorIndex::DistanceResult>;

template<vectorindex::ValidDistanceResultType DistanceResult>
void MergeChunkResults(
    std::vector<vectorindex::VertexWithDistance<DistanceResult>>& combined_results,
    std::vector<vectorindex::VertexWithDistance<DistanceResult>>& chunk_results,
    size_t max_num_results);

}  // namespace yb::docdb
