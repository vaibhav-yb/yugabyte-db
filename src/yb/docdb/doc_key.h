// Copyright (c) YugaByte, Inc.
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

#include "yb/docdb/doc_key_base.h"

namespace yb {
namespace docdb {

// ------------------------------------------------------------------------------------------------
// DocKey
// ------------------------------------------------------------------------------------------------

// A key that allows us to locate a document. This is the prefix of all RocksDB keys of records
// inside this document. A document key contains:
//   - An optional ID (cotable id or colocation id).
//   - An optional fixed-width hash prefix.
//   - A group of primitive values representing "hashed" components (this is what the hash is
//     computed based on, so this group is present/absent together with the hash).
//   - A group of "range" components suitable for doing ordered scans.
//
// The encoded representation of the key is as follows:
//   - Optional ID:
//     * For cotable id, the byte ValueType::kTableId followed by a sixteen byte UUID.
//     * For colocation id, the byte ValueType::kColocationId followed by a four byte
//       ColocationId.
//   - Optional fixed-width hash prefix, followed by hashed components:
//     * The byte ValueType::kUInt16Hash, followed by two bytes of the hash prefix.
//     * Hashed components:
//       1. Each hash component consists of a type byte (ValueType) followed by the encoded
//          representation of the respective type (see PrimitiveValue's key encoding).
//       2. ValueType::kGroupEnd terminates the sequence.
//   - Range components are stored similarly to the hashed components:
//     1. Each range component consists of a type byte (ValueType) followed by the encoded
//        representation of the respective type (see PrimitiveValue's key encoding).
//     2. ValueType::kGroupEnd terminates the sequence.
YB_DEFINE_ENUM(
    DocKeyPart,
    (kUpToHashCode)
    (kUpToHash)
    (kUpToId)
    // Includes all doc key components up to hashed ones. If there are no hashed components -
    // includes the first range component.
    (kUpToHashOrFirstRange)
    (kWholeDocKey));

class DocKeyDecoder;

YB_STRONGLY_TYPED_BOOL(HybridTimeRequired)

// Key in DocDB is named - SubDocKey. It consist of DocKey (i.e. row identifier) and
// subkeys (i.e. column id + possible sub values).
// DocKey consists of hash part followed by range part.
// hash_part_size - size of hash part of the key.
// doc_key_size - size of hash part + range part of the key.
struct DocKeySizes {
  size_t hash_part_size;
  size_t doc_key_size;
};

class DocKey : public DocKeyBase {
 public:
  // Constructs an empty document key with no hash component.
  DocKey();

  // Construct a document key with only a range component, but no hashed component.
  explicit DocKey(std::vector<KeyEntryValue> range_components);

  // Construct a document key including a hashed component and a range component. The hash value has
  // to be calculated outside of the constructor, and we're not assuming any specific hash function
  // here.
  // @param hash A hash value calculated using the appropriate hash function based on
  //             hashed_components.
  // @param hashed_components Components of the key that go into computing the hash prefix.
  // @param range_components Components of the key that we want to be able to do range scans on.
  DocKey(DocKeyHash hash,
         std::vector<KeyEntryValue> hashed_components,
         std::vector<KeyEntryValue> range_components = std::vector<KeyEntryValue>());

  DocKey(const Uuid& cotable_id,
         DocKeyHash hash,
         std::vector<KeyEntryValue> hashed_components,
         std::vector<KeyEntryValue> range_components = std::vector<KeyEntryValue>());

  DocKey(ColocationId colocation_id,
         DocKeyHash hash,
         std::vector<KeyEntryValue> hashed_components,
         std::vector<KeyEntryValue> range_components = std::vector<KeyEntryValue>());

  explicit DocKey(const Uuid& cotable_id);

  explicit DocKey(ColocationId colocation_id);

  // Constructors to create a DocKey for the given schema to support co-located tables.
  explicit DocKey(const Schema& schema);
  DocKey(const Schema& schema, std::vector<KeyEntryValue> range_components);
  DocKey(const Schema& schema, DocKeyHash hash,
         std::vector<KeyEntryValue> hashed_components,
         std::vector<KeyEntryValue> range_components = std::vector<KeyEntryValue>());

  void AppendTo(KeyBytes* out) const override;

  // Encodes DocKey to binary representation returning result as RefCntPrefix.
  RefCntPrefix EncodeAsRefCntPrefix() const;

  const Uuid& cotable_id() const {
    return cotable_id_;
  }

  bool has_cotable_id() const {
    return !cotable_id_.IsNil();
  }

  ColocationId colocation_id() const {
    return colocation_id_;
  }

  bool has_colocation_id() const {
    return colocation_id_ != kColocationIdNotSet;
  }

  // Decodes a document key from the given RocksDB key.
  // slice (in/out) - a slice corresponding to a RocksDB key. Any consumed bytes are removed.
  // part_to_decode specifies which part of key to decode.
  Status DecodeFrom(
      Slice* slice,
      DocKeyPart part_to_decode = DocKeyPart::kWholeDocKey,
      AllowSpecial allow_special = AllowSpecial::kFalse);

  // Decodes a document key from the given RocksDB key similar to the above but return the number
  // of bytes decoded from the input slice.
  Result<size_t> DecodeFrom(
      const Slice& slice,
      DocKeyPart part_to_decode = DocKeyPart::kWholeDocKey,
      AllowSpecial allow_special = AllowSpecial::kFalse);

  // Splits given RocksDB key into vector of slices that forms range_group of document key.
  static Status PartiallyDecode(Slice* slice,
                                        boost::container::small_vector_base<Slice>* out);

  // Decode just the hash code of a DocKey.
  static Result<DocKeyHash> DecodeHash(const Slice& slice);

  static Result<size_t> EncodedSize(
      Slice slice, DocKeyPart part, AllowSpecial allow_special = AllowSpecial::kFalse);

  // Returns size of the encoded `part` of DocKey and whether it has hash code present.
  static Result<std::pair<size_t, bool>> EncodedSizeAndHashPresent(Slice slice, DocKeyPart part);

  // Returns size of encoded hash part and whole part of DocKey.
  static Result<DocKeySizes> EncodedHashPartAndDocKeySizes(
      Slice slice, AllowSpecial allow_special = AllowSpecial::kFalse);

  // Decode the current document key from the given slice, but expect all bytes to be consumed, and
  // return an error status if that is not the case.
  Status FullyDecodeFrom(const Slice& slice);

  // Converts the document key to a human-readable representation.
  std::string ToString(AutoDecodeKeys auto_decode_keys = AutoDecodeKeys::kFalse) const override;
  static std::string DebugSliceToString(Slice slice);

  bool operator ==(const DocKey& other) const;

  bool operator !=(const DocKey& other) const {
    return !(*this == other);
  }

  int CompareTo(const DocKey& other) const;

  bool operator <(const DocKey& other) const {
    return CompareTo(other) < 0;
  }

  bool operator <=(const DocKey& other) const {
    return CompareTo(other) <= 0;
  }

  bool operator >(const DocKey& other) const {
    return CompareTo(other) > 0;
  }

  bool operator >=(const DocKey& other) const {
    return CompareTo(other) >= 0;
  }

  bool BelongsTo(const Schema& schema) const;

  void set_cotable_id(const Uuid& cotable_id) {
    if (!cotable_id.IsNil()) {
      DCHECK_EQ(colocation_id_, kColocationIdNotSet);
    }
    cotable_id_ = cotable_id;
  }

  void set_colocation_id(const ColocationId colocation_id) {
    if (colocation_id != kColocationIdNotSet) {
      DCHECK(cotable_id_.IsNil());
    }
    colocation_id_ = colocation_id;
  }

  // Converts a redis string key to a doc key
  static DocKey FromRedisKey(uint16_t hash, const std::string& key);
  static KeyBytes EncodedFromRedisKey(uint16_t hash, const std::string &key);

 private:
  class DecodeFromCallback;
  friend class DecodeFromCallback;

  template<class Callback>
  static Status DoDecode(DocKeyDecoder* decoder,
                                 DocKeyPart part_to_decode,
                                 AllowSpecial allow_special,
                                 const Callback& callback);

 protected:
  // Uuid of the non-primary table this DocKey belongs to co-located in a tablet. Nil for the
  // primary or single-tenant table.
  Uuid cotable_id_;

  // Colocation ID used to distinguish a table within a colocation group.
  // kColocationIdNotSet for a primary or single-tenant table.
  ColocationId colocation_id_;
};

class DocKeyEncoder {
 public:
  explicit DocKeyEncoder(KeyBytes* out) : out_(out) {}

  DocKeyEncoderAfterTableIdStep CotableId(const Uuid& cotable_id);

  DocKeyEncoderAfterTableIdStep ColocationId(const ColocationId colocation_id);

  DocKeyEncoderAfterTableIdStep Schema(const Schema& schema);

 private:
  KeyBytes* out_;
};

class DocKeyDecoder : public DocKeyBaseDecoder {
 public:
  explicit DocKeyDecoder(const Slice& input) : DocKeyBaseDecoder(input) {}

  Result<bool> DecodeCotableId(Uuid* uuid = nullptr);
  Result<bool> DecodeColocationId(ColocationId* colocation_id = nullptr);

  Status DecodeToRangeGroup() override;
};

// Clears range components from provided key. Returns true if they were exists.
Result<bool> ClearRangeComponents(KeyBytes* out, AllowSpecial allow_special = AllowSpecial::kFalse);

// Returns true if both keys have hashed components and them are equal or both keys don't have
// hashed components and first range components are equal and false otherwise.
Result<bool> HashedOrFirstRangeComponentsEqual(const Slice& lhs, const Slice& rhs);

bool DocKeyBelongsTo(Slice doc_key, const Schema& schema);

Result<boost::optional<DocKeyHash>> DecodeDocKeyHash(const Slice& encoded_key);

inline std::ostream& operator <<(std::ostream& out, const DocKey& doc_key) {
  out << doc_key.ToString();
  return out;
}

// ------------------------------------------------------------------------------------------------
// SubDocKey
// ------------------------------------------------------------------------------------------------

// A key pointing to a subdocument. Consists of a DocKey identifying the document, a list of
// primitive values leading to the subdocument in question, from the outermost to innermost order,
// and an optional hybrid_time of when the subdocument (which may itself be a primitive value) was
// last fully overwritten or deleted.
//
// Keys stored in RocksDB should always have the hybrid_time field set. However, it is useful to
// make the hybrid_time field optional while a SubDocKey is being constructed. If the hybrid_time
// is not set, it is omitted from the encoded representation of a SubDocKey.
//
// Implementation note: we use HybridTime::kInvalid to represent an omitted hybrid_time.
// We rely on that being the default-constructed value of a HybridTime.
//
// TODO: this should be renamed to something more generic, e.g. Key or LogicalKey, to reflect that
// this is actually the logical representation of keys that we store in the RocksDB key-value store.
class SubDocKey {
 public:
  SubDocKey() {}
  explicit SubDocKey(const DocKey& doc_key) : doc_key_(doc_key) {}
  explicit SubDocKey(DocKey&& doc_key) : doc_key_(std::move(doc_key)) {}

  SubDocKey(const DocKey& doc_key, HybridTime hybrid_time)
      : doc_key_(doc_key),
        doc_ht_(DocHybridTime(hybrid_time)) {
  }

  SubDocKey(DocKey&& doc_key,
            HybridTime hybrid_time)
      : doc_key_(std::move(doc_key)),
        doc_ht_(DocHybridTime(hybrid_time)) {
  }

  SubDocKey(const DocKey& doc_key, const DocHybridTime& hybrid_time)
      : doc_key_(doc_key),
        doc_ht_(std::move(hybrid_time)) {
  }

  SubDocKey(const DocKey& doc_key,
            DocHybridTime doc_hybrid_time,
            const std::vector<KeyEntryValue>& subkeys)
      : doc_key_(doc_key),
        doc_ht_(doc_hybrid_time),
        subkeys_(subkeys) {
  }

  SubDocKey(const DocKey& doc_key,
            HybridTime hybrid_time,
            const std::vector<KeyEntryValue>& subkeys)
      : doc_key_(doc_key),
        doc_ht_(DocHybridTime(hybrid_time)),
        subkeys_(subkeys) {
  }

  template <class ...T>
  SubDocKey(const DocKey& doc_key, T... subkeys_and_maybe_hybrid_time)
      : doc_key_(doc_key),
        doc_ht_(DocHybridTime::kInvalid) {
    AppendSubKeysAndMaybeHybridTime(subkeys_and_maybe_hybrid_time...);
  }

  Status FromDocPath(const DocPath& doc_path);

  // Return the subkeys within this SubDocKey
  const std::vector<KeyEntryValue>& subkeys() const {
    return subkeys_;
  }

  std::vector<KeyEntryValue>& subkeys() {
    return subkeys_;
  }

  // Append a sequence of sub-keys to this key.
  template<class ...T>
  void AppendSubKeysAndMaybeHybridTime(KeyEntryValue subdoc_key,
                                       T... subkeys_and_maybe_hybrid_time) {
    subkeys_.push_back(std::move(subdoc_key));
    AppendSubKeysAndMaybeHybridTime(subkeys_and_maybe_hybrid_time...);
  }

  template<class ...T>
  void AppendSubKeysAndMaybeHybridTime(KeyEntryValue subdoc_key) {
    subkeys_.emplace_back(std::move(subdoc_key));
  }

  template<class ...T>
  void AppendSubKeysAndMaybeHybridTime(KeyEntryValue subdoc_key, HybridTime hybrid_time) {
    DCHECK(!has_hybrid_time());
    subkeys_.emplace_back(std::move(subdoc_key));
    DCHECK(hybrid_time.is_valid());
    doc_ht_ = DocHybridTime(hybrid_time);
  }

  void AppendSubKey(KeyEntryValue subkey);

  void RemoveLastSubKey();

  void KeepPrefix(size_t num_sub_keys_to_keep);

  void remove_hybrid_time() {
    doc_ht_ = DocHybridTime::kInvalid;
  }

  void Clear();

  bool IsValid() const {
    return !doc_key_.empty();
  }

  KeyBytes Encode() const { return DoEncode(true /* include_hybrid_time */); }
  KeyBytes EncodeWithoutHt() const { return DoEncode(false /* include_hybrid_time */); }

  // Decodes a SubDocKey from the given slice, typically retrieved from a RocksDB key.
  // @param slice
  //     A pointer to the slice containing the bytes to decode the SubDocKey from. This slice is
  //     modified, with consumed bytes being removed.
  // @param require_hybrid_time
  //     Whether a hybrid_time is required in the end of the SubDocKey. If this is true, we require
  //     a ValueType::kHybridTime byte followed by a hybrid_time to be present in the input slice.
  //     Otherwise, we allow decoding an incomplete SubDocKey without a hybrid_time in the end. Note
  //     that we also allow input that has a few bytes in the end but not enough to represent a
  //     hybrid_time.
  // @param allow_special
  //     Whether it is allowed to have special value types in slice, that are used during seek.
  //     If such value type is found, decoding is stopped w/o error.
  Status DecodeFrom(Slice* slice,
                    HybridTimeRequired require_hybrid_time = HybridTimeRequired::kTrue,
                    AllowSpecial allow_special = AllowSpecial::kFalse);

  // Similar to DecodeFrom, but requires that the entire slice is decoded, and thus takes a const
  // reference to a slice. This still respects the require_hybrid_time parameter, but in case a
  // hybrid_time is omitted, we don't allow any extra bytes to be present in the slice.
  Status FullyDecodeFrom(
      const Slice& slice,
      HybridTimeRequired hybrid_time_required = HybridTimeRequired::kTrue);

  // Splits given RocksDB key into vector of slices that forms range_group of document key and
  // hybrid_time.
  static Status PartiallyDecode(Slice* slice,
                                        boost::container::small_vector_base<Slice>* out);

  // Splits the given RocksDB sub key into a vector of slices that forms the range group of document
  // key and sub keys.
  //
  // We don't use Result<...> to be able to reuse memory allocated by out.
  //
  // When key does not start with a hash component, the returned prefix would start with the first
  // range component.
  //
  // For instance, for a (hash_value, h1, h2, r1, r2, s1) doc key the following values will be
  // returned:
  // encoded_length(hash_value, h1, h2) <-------------- (includes the kGroupEnd of the hashed part)
  // encoded_length(hash_value, h1, h2, r1)
  // encoded_length(hash_value, h1, h2, r1, r2)
  // encoded_length(hash_value, h1, h2, r1, r2, s1) <------- (includes kGroupEnd of the range part).
  static Status DecodePrefixLengths(
      Slice slice, boost::container::small_vector_base<size_t>* out);

  // Fills out with ends of SubDocKey components.  First item in out will be size of ID part
  // (cotable id or colocation id) of DocKey (0 if ID is not present), second size of whole DocKey,
  // third size of DocKey + size of first subkey, and so on.
  //
  // To illustrate,
  // * for key
  //     SubDocKey(DocKey(0xfca0, [3], []), [SystemColumnId(0); HT{ physical: 1581475435181551 }])
  //   aka
  //     47FCA0488000000321214A80238001B5E605A0CA10804A
  //   (and with spaces to make it clearer)
  //     47FCA0 4880000003 21 21 4A80 238001B5E605A0CA10804A
  //   the ends will be
  //     {0, 10, 12}
  // * for key
  //     SubDocKey(DocKey(ColocationId=16385, [], [5]), [SystemColumnId(0); HT{ physical: ... }])
  //   aka
  //     30000040014880000005214A80238001B5E700309553804A
  //   (and with spaces to make it clearer)
  //     3000004001 4880000005 21 4A80 238001B5E700309553804A
  //   the ends will be
  //     {5, 11, 13}
  // * for key
  //     SubDocKey(DocKey(ColocationId=16385, [], []), [HT{ physical: 1581471227403848 }])
  //   aka
  //     300000400121238001B5E7006E61B7804A
  //   (and with spaces to make it clearer)
  //     3000004001 21 238001B5E7006E61B7804A
  //   the ends will be
  //     {5}
  //
  // If out is not empty, then it will be interpreted as partial result for this decoding operation
  // and the appropriate prefix will be skipped.
  static Status DecodeDocKeyAndSubKeyEnds(
      Slice slice, boost::container::small_vector_base<size_t>* out);

  // Attempts to decode a subkey at the beginning of the given slice, consuming the corresponding
  // prefix of the slice. Returns false if there is no next subkey, as indicated by the slice being
  // empty or encountering an encoded hybrid time.
  static Result<bool> DecodeSubkey(Slice* slice);

  Status FullyDecodeFromKeyWithOptionalHybridTime(const Slice& slice);

  std::string ToString(AutoDecodeKeys auto_decode_keys = AutoDecodeKeys::kFalse) const;
  static std::string DebugSliceToString(Slice slice);
  static Result<std::string> DebugSliceToStringAsResult(Slice slice);

  const DocKey& doc_key() const {
    return doc_key_;
  }

  DocKey& doc_key() {
    return doc_key_;
  }

  size_t num_subkeys() const {
    return subkeys_.size();
  }

  bool StartsWith(const SubDocKey& prefix) const;

  bool operator ==(const SubDocKey& other) const;

  bool operator !=(const SubDocKey& other) const {
    return !(*this == other);
  }

  const KeyEntryValue& last_subkey() const {
    assert(!subkeys_.empty());
    return subkeys_.back();
  }

  int CompareTo(const SubDocKey& other) const;
  int CompareToIgnoreHt(const SubDocKey& other) const;

  bool operator <(const SubDocKey& other) const {
    return CompareTo(other) < 0;
  }

  bool operator <=(const SubDocKey& other) const {
    return CompareTo(other) <= 0;
  }

  bool operator >(const SubDocKey& other) const {
    return CompareTo(other) > 0;
  }

  bool operator >=(const SubDocKey& other) const {
    return CompareTo(other) >= 0;
  }

  HybridTime hybrid_time() const {
    DCHECK(has_hybrid_time());
    return doc_ht_.hybrid_time();
  }

  const DocHybridTime& doc_hybrid_time() const {
    DCHECK(has_hybrid_time());
    return doc_ht_;
  }

  void set_hybrid_time(const DocHybridTime& hybrid_time) {
    DCHECK(hybrid_time.is_valid());
    doc_ht_ = hybrid_time;
  }

  bool has_hybrid_time() const {
    return doc_ht_.is_valid();
  }

  // Generate a RocksDB key that would allow us to seek to the smallest SubDocKey that has a
  // lexicographically higher sequence of subkeys than this one, but is not an extension of this
  // sequence of subkeys.  In other words, ensure we advance to the next field (subkey) either
  // within the object (subdocument) we are currently scanning, or at any higher level, including
  // advancing to the next document key.
  //
  // E.g. assuming the SubDocKey this is being called on is #2 from the following example,
  // performing a RocksDB seek on the return value of this takes us to #7.
  //
  // 1. SubDocKey(DocKey([], ["a"]), [HT(1)]) -> {}
  // 2. SubDocKey(DocKey([], ["a"]), ["x", HT(1)]) -> {} ---------------------------.
  // 3. SubDocKey(DocKey([], ["a"]), ["x", "x", HT(2)]) -> null                     |
  // 4. SubDocKey(DocKey([], ["a"]), ["x", "x", HT(1)]) -> {}                       |
  // 5. SubDocKey(DocKey([], ["a"]), ["x", "x", "y", HT(1)]) -> {}                  |
  // 6. SubDocKey(DocKey([], ["a"]), ["x", "x", "y", "x", HT(1)]) -> true           |
  // 7. SubDocKey(DocKey([], ["a"]), ["y", HT(3)]) -> {}                  <---------
  // 8. SubDocKey(DocKey([], ["a"]), ["y", "y", HT(3)]) -> {}
  // 9. SubDocKey(DocKey([], ["a"]), ["y", "y", "x", HT(3)]) ->
  //
  // This is achieved by simply appending a byte that is higher than any ValueType in an encoded
  // representation of a SubDocKey that extends the vector of subkeys present in the current one,
  // or has the same vector of subkeys, i.e. key/value pairs #3-6 in the above example. HybridTime
  // is omitted from the resulting encoded representation.
  KeyBytes AdvanceOutOfSubDoc() const;

  // Similar to AdvanceOutOfSubDoc, but seek to the smallest key that skips documents with this
  // DocKey and DocKeys that have the same hash components but add more range components to it.
  //
  // E.g. assuming the SubDocKey this is being called on is #2 from the following example:
  //
  //  1. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), [HT(1)]) -> {}
  //  2. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["x", HT(1)]) -> {} <----------------.
  //  3. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["x", "x", HT(2)]) -> null           |
  //  4. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["x", "x", HT(1)]) -> {}             |
  //  5. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["x", "x", "y", HT(1)]) -> {}        |
  //  6. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["x", "x", "y", "x", HT(1)]) -> true |
  //  7. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["y", HT(3)]) -> {}                  |
  //  8. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["y", "y", HT(3)]) -> {}             |
  //  9. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"]), ["y", "y", "x", HT(3)]) -> {}        |
  // ...                                                                                        |
  // 20. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d", "e"]), ["y", HT(3)]) -> {}             |
  // 21. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d", "e"]), ["z", HT(3)]) -> {}             |
  // 22. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "f"]), [HT(1)]) -> {}      <--- (*** 1 ***)-|
  // 23. SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "f"]), ["x", HT(1)]) -> {}                  |
  // ...                                                                                        |
  // 30. SubDocKey(DocKey(0x2345, ["a", "c"], ["c", "f"]), [HT(1)]) -> {}      <--- (*** 2 ***)-
  // 31. SubDocKey(DocKey(0x2345, ["a", "c"], ["c", "f"]), ["x", HT(1)]) -> {}
  //
  // SubDocKey(DocKey(0x1234, ["a", "b"], ["c", "d"])).AdvanceOutOfDocKeyPrefix() will seek to #22
  // (*** 1 ***), pass doc keys with additional range components when they are present.
  //
  // And when given a doc key without range component like below, it can help seek pass all doc
  // keys with the same hash components, e.g.
  // SubDocKey(DocKey(0x1234, ["a", "b"], [])).AdvanceOutOfDocKeyPrefix() will seek to #30
  // (*** 2 ***).

  KeyBytes AdvanceOutOfDocKeyPrefix() const;

 private:
  class DecodeCallback;
  friend class DecodeCallback;

  // Attempts to decode and consume a subkey from the beginning of the given slice.
  // A non-error false result means e.g. that the slice is empty or if the next thing is an encoded
  // hybrid time.
  template<class Callback>
  static Result<bool> DecodeSubkey(Slice* slice, const Callback& callback);

  template<class Callback>
  static Status DoDecode(Slice* slice,
                         HybridTimeRequired require_hybrid_time,
                         AllowSpecial allow_special,
                         const Callback& callback);

  KeyBytes DoEncode(bool include_hybrid_time) const;

  DocKey doc_key_;
  DocHybridTime doc_ht_;

  // TODO: make this a small_vector.
  std::vector<KeyEntryValue> subkeys_;
};

inline std::ostream& operator <<(std::ostream& out, const SubDocKey& subdoc_key) {
  out << subdoc_key.ToString();
  return out;
}

// A best-effort to decode the given sequence of key bytes as either a DocKey or a SubDocKey.
// If not possible to decode, return the key_bytes directly as a readable string.
std::string BestEffortDocDBKeyToStr(const KeyBytes &key_bytes);
std::string BestEffortDocDBKeyToStr(const Slice &slice);

class DocDbAwareFilterPolicyBase : public rocksdb::FilterPolicy {
 public:
  explicit DocDbAwareFilterPolicyBase(size_t filter_block_size_bits, rocksdb::Logger* logger) {
    builtin_policy_.reset(rocksdb::NewFixedSizeFilterPolicy(
        filter_block_size_bits, rocksdb::FilterPolicy::kDefaultFixedSizeFilterErrorRate, logger));
  }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;

  rocksdb::FilterBitsBuilder* GetFilterBitsBuilder() const override;

  rocksdb::FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override;

  FilterType GetFilterType() const override;

 private:
  std::unique_ptr<const rocksdb::FilterPolicy> builtin_policy_;
};

// This filter policy only takes into account hashed components of keys for filtering.
class DocDbAwareHashedComponentsFilterPolicy : public DocDbAwareFilterPolicyBase {
 public:
  DocDbAwareHashedComponentsFilterPolicy(size_t filter_block_size_bits, rocksdb::Logger* logger)
      : DocDbAwareFilterPolicyBase(filter_block_size_bits, logger) {}

  const char* Name() const override { return "DocKeyHashedComponentsFilter"; }

  const KeyTransformer* GetKeyTransformer() const override;
};

// Together with the fix for BlockBasedTableBuild::Add
// (https://github.com/yugabyte/yugabyte-db/issues/6435) we also disable DocKeyV2Filter
// for range-partitioned tablets. For hash-partitioned tablets it will be supported during read
// path and will work the same way as DocDbAwareV3FilterPolicy.
class DocDbAwareV2FilterPolicy : public DocDbAwareFilterPolicyBase {
 public:
  DocDbAwareV2FilterPolicy(size_t filter_block_size_bits, rocksdb::Logger* logger)
      : DocDbAwareFilterPolicyBase(filter_block_size_bits, logger) {}

  const char* Name() const override { return "DocKeyV2Filter"; }

  const KeyTransformer* GetKeyTransformer() const override;
};

// This filter policy takes into account following parts of keys for filtering:
// - For range-based partitioned tables (such tables have 0 hashed components):
// use all hash components of the doc key.
// - For hash-based partitioned tables (such tables have >0 hashed components):
// use first range component of the doc key.
class DocDbAwareV3FilterPolicy : public DocDbAwareFilterPolicyBase {
 public:
  DocDbAwareV3FilterPolicy(size_t filter_block_size_bits, rocksdb::Logger* logger)
      : DocDbAwareFilterPolicyBase(filter_block_size_bits, logger) {}

  const char* Name() const override { return "DocKeyV3Filter"; }

  const KeyTransformer* GetKeyTransformer() const override;
};

}  // namespace docdb
}  // namespace yb
