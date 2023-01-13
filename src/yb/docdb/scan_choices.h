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

#include "yb/common/ql_scanspec.h"

#include "yb/docdb/doc_key.h"
#include "yb/docdb/doc_ql_scanspec.h"
#include "yb/docdb/value.h"
#include "yb/docdb/docdb_fwd.h"

#include "yb/docdb/value_type.h"
#include "yb/util/slice.h"
#include "yb/util/status_fwd.h"

namespace yb {
namespace docdb {

class ScanChoices {
 public:
  explicit ScanChoices(bool is_forward_scan) : is_forward_scan_(is_forward_scan) {}
  virtual ~ScanChoices() {}

  bool CurrentTargetMatchesKey(const Slice& curr);

  // Returns false if there are still target keys we need to scan, and true if we are done.
  virtual bool FinishedWithScanChoices() const { return finished_; }

  virtual bool IsInitialPositionKnown() const { return false; }

  // Go to the next scan target if any.
  virtual Status DoneWithCurrentTarget() = 0;

  // Go (directly) to the new target (or the one after if new_target does not
  // exist in the desired list/range). If the new_target is larger than all scan target options it
  // means we are done.
  // Returns false if key parsing fails for any reason. If new_target is a valid user key, this
  // method will correctly parse it. If it is not a valid user key, this method will return false.
  // The caller should then determine how to deal with this key; e.g. if it's a valid system key,
  // the caller can seek past this key before calling this method again with a new key.
  virtual Result<bool> SkipTargetsUpTo(const Slice& new_target) = 0;

  // If the given doc_key isn't already at the desired target, seek appropriately to go to the
  // current target.
  virtual Status SeekToCurrentTarget(IntentAwareIteratorIf* db_iter) = 0;

  static Result<std::vector<KeyEntryValue>> DecodeKeyEntryValue(
      DocKeyDecoder* decoder, size_t num_cols);

  static ScanChoicesPtr Create(
      const Schema& schema, const DocQLScanSpec& doc_spec, const KeyBytes& lower_doc_key,
      const KeyBytes& upper_doc_key, const size_t prefix_length);

  static ScanChoicesPtr Create(
      const Schema& schema, const DocPgsqlScanSpec& doc_spec, const KeyBytes& lower_doc_key,
      const KeyBytes& upper_doc_key, const size_t prefix_length);

 protected:
  const bool is_forward_scan_;
  KeyBytes current_scan_target_;
  bool finished_ = false;

 private:
  DISALLOW_COPY_AND_ASSIGN(ScanChoices);
};

// This class combines the notions of option filters (col1 IN (1,2,3)) and
// singular range bound filters (col1 < 4 AND col1 >= 1) into a single notion of
// lists of options of ranges, each encoded by an OptionRange instance as
// below. So a filter for a column given in the
// Doc(QL/PGSQL)ScanSpec is converted into an OptionRange.
// In the end, each HybridScanChoices
// instance should have a sorted list of disjoint ranges for every IN/EQ clause.
// Each OptionRange has a begin_idx_ and an end_idx_. This is used to implement
// a mini run-length encoding for duplicate OptionRange values. A particular
// OptionRange with Range [lower, upper] implies that that range applies for
// all option indexes in [begin_idx_, end_idx_).
// We illustrate here why this run-encoding might be useful with an example.
// Consider an options matrix on a table with primary key (r1,r2,r3) and filters
// (r1, r3) IN ((1,3), (1,7), (5,6), (5,7), (5,9)) AND r2 IN (7,9)
// The options matrix without run compression will look something like
// r1 | [1,1]  [1,1] ([5,5]) [5,5] [5,5]
// r2 | [7,7] ([9,9])
// r3 | [3,3]  [7,7] ([6,6]) [7,7] [9,9]
// Where r1 and r3 are in a group (see the comments for HybridScanChoices::col_groups_)
// Let us say our current scan target is (5,9,6). The active options are parenthesized above.
// Let us say we wish to invoke SkipTargetsUpTo(5,9,7). In that case we must
// move the active option for r1 to the fourth option. Usually, if r1 and r3 were
// not grouped then this move to the next option for r1 would necessitate that
// the options for r2 and r3 be reset to their first options. That should not be
// the case here as the option value of r1 is not changing here even though we are
// altering the active logical option. We do not want to reset r2 here.
// We see that the given target is satisfied by the currently active r2 option so we don't mess
// with that.
// Furthermore, when selecting the appropriate option for r3 we can only consider
// options that preserve the active option values for r1. That corresponds to the options between
// the third [6,6] and fifth [9,9]. If r3 was not in a group then we
// could have considered all the available r3 options as valid active candidates.
// This results in the following options matrix:
// r1 | [1,1]  [1,1] [5,5] ([5,5]) [5,5]
// r2 | [7,7] ([9,9])
// r3 | [3,3]  [7,7] [6,6] ([7,7]) [9,9]

// So there were two mechanics of note here:
// 1) We need to be able to identify when we're moving the active option for a key expression
// to another option that's identical. In these cases, we shouldn't reset the
// following key expression active options.
// 2) If a certain key expression is in a group then we need to be able to identify what the valid
// range of options we can consider is.
// Say we run-compressed the initial options matrix as follows by encoding
// each option into option "ranges". Each entry now has an option value and
// an affixed range to show which logical options this option value corresponds to:
// r1 | [1,1](r:[0,2)) ([5,5](r:[2,5)))
// r2 | [7,7](r:[0,1)) ([9,9](r:[1,2)))
// r3 | [3,3](r:[0,1)) [7,7](r:[1,2)) ([6,6](r:[2,3))) [7,7](r:[3,4)) [9,9](r:[4,5))

// Now each option range can contain a range of logical options. When moving from
// one logical option to another we can check to see if
// we are in the same option range to satisfy mechanic (1).
// In order to carry out (2) to find a valid set of option ranges to consider for r3,
// we can look at the currently active option range for the predecessor in r3's group, r1.
// That would be ([5,5](r:[2,5))) in this case.
// This tells us we can only consider logical options from index 2 to 4.
// This range is determined by GetSearchSpaceLowerBound and GetSearchSpaceUpperBound.
// Right now this supports a conjunction of range bound and discrete filters.
// Disjunctions are also supported but are UNTESTED.
// TODO: Test disjunctions when YSQL and YQL support pushing those down
// The lower and upper values are vectors to incorporate multi-column IN clause.
class OptionRange {
 public:
  OptionRange(KeyEntryValue lower, bool lower_inclusive, KeyEntryValue upper, bool upper_inclusive)
      : lower_(lower),
        lower_inclusive_(lower_inclusive),
        upper_(upper),
        upper_inclusive_(upper_inclusive),
        begin_idx_(0),
        end_idx_(0) {}

  OptionRange(
      KeyEntryValue lower, bool lower_inclusive, KeyEntryValue upper, bool upper_inclusive,
      size_t begin_idx, size_t end_idx)
      : lower_(lower),
        lower_inclusive_(lower_inclusive),
        upper_(upper),
        upper_inclusive_(upper_inclusive),
        begin_idx_(begin_idx),
        end_idx_(end_idx) {}

  // Convenience constructors for testing
  OptionRange(int begin, int end, SortOrder sort_order = SortOrder::kAscending)
      : OptionRange(
            {KeyEntryValue::Int32(begin, sort_order)}, true,
            {KeyEntryValue::Int32(end, sort_order)}, true) {}

  OptionRange(int value, SortOrder sort_order = SortOrder::kAscending) // NOLINT
      : OptionRange(value, value, sort_order) {}

  OptionRange(int bound, bool upper, SortOrder sort_order = SortOrder::kAscending)
      : OptionRange(
            {upper ? KeyEntryValue(KeyEntryType::kLowest)
                   : KeyEntryValue::Int32(bound, sort_order)},
            true,
            {upper ? KeyEntryValue::Int32(bound, sort_order)
                   : KeyEntryValue(KeyEntryType::kHighest)},
            true) {}
  OptionRange()
      : OptionRange(
            {KeyEntryValue(KeyEntryType::kLowest)},
            true,
            {KeyEntryValue(KeyEntryType::kHighest)},
            true) {}

  const KeyEntryValue& lower() const { return lower_; }
  bool lower_inclusive() const { return lower_inclusive_; }
  const KeyEntryValue& upper() const { return upper_; }
  bool upper_inclusive() const { return upper_inclusive_; }

  size_t begin_idx() const { return begin_idx_; }
  size_t end_idx() const { return end_idx_; }

  bool HasIndex(size_t opt_index) const { return begin_idx_ <= opt_index && opt_index < end_idx_; }

  static bool upper_lt(const OptionRange& range1, const OptionRange& range2) {
    return range1.upper_ < range2.upper_;
  }

  static bool lower_gt(const OptionRange& range1, const OptionRange& range2) {
    return range1.lower_ > range2.lower_;
  }

  static bool end_idx_leq(const OptionRange& range1, const OptionRange& range2) {
    return range1.end_idx_ <= range2.end_idx_;
  }

 private:
  KeyEntryValue lower_;
  bool lower_inclusive_;
  KeyEntryValue upper_;
  bool upper_inclusive_;

  size_t begin_idx_;
  size_t end_idx_;

  friend class ScanChoicesTest;
  bool operator==(const OptionRange& other) const {
    return lower_inclusive() == other.lower_inclusive() &&
           upper_inclusive() == other.upper_inclusive() && lower() == other.lower() &&
           upper() == other.upper();
  }
};

inline std::ostream& operator<<(std::ostream& str, const OptionRange& opt) {
  if (opt.lower_inclusive()) {
    str << "[";
  } else {
    str << "(";
  }

  str << opt.lower().ToString() << ", " << opt.upper().ToString();

  if (opt.upper_inclusive()) {
    str << "]";
  } else {
    str << ")";
  }
  return str;
}

class HybridScanChoices : public ScanChoices {
 public:
  // Constructs a list of ranges for each IN/EQ clause from the given scanspec.
  // A filter of the form col1 IN (1,2) is converted to col1 IN ([[1], [1]], [[2], [2]]).
  // And filter of the form (col2, col3) IN ((3,4), (5,6)) is converted to
  // (col2, col3) IN ([[3, 4], [3, 4]], [[5, 6], [5, 6]]).

  HybridScanChoices(
      const Schema& schema,
      const KeyBytes& lower_doc_key,
      const KeyBytes& upper_doc_key,
      bool is_forward_scan,
      const std::vector<ColumnId>& range_options_col_ids,
      const std::shared_ptr<std::vector<OptionList>>& range_options,
      const std::vector<ColumnId>& range_bounds_col_ids,
      const QLScanRange* range_bounds,
      const ColGroupHolder& col_groups,
      const size_t prefix_length);

  HybridScanChoices(
      const Schema& schema,
      const DocPgsqlScanSpec& doc_spec,
      const KeyBytes& lower_doc_key,
      const KeyBytes& upper_doc_key,
      const size_t prefix_length);

  HybridScanChoices(
      const Schema& schema,
      const DocQLScanSpec& doc_spec,
      const KeyBytes& lower_doc_key,
      const KeyBytes& upper_doc_key,
      const size_t prefix_length);

  Result<bool> SkipTargetsUpTo(const Slice& new_target) override;
  Status DoneWithCurrentTarget() override;
  Status SeekToCurrentTarget(IntentAwareIteratorIf* db_iter) override;

 protected:
  friend class ScanChoicesTest;
  // Utility function for (multi)key scans. Updates the target scan key by
  // incrementing the option index for an OptionList. Will handle overflow by setting current
  // index to 0 and incrementing the previous index instead. If it overflows at first index
  // it means we are done, so it clears the scan target idxs array.
  Status IncrementScanTargetAtOptionList(int start_option_list_idx);

  // Utility function for testing
  std::vector<OptionRange> TEST_GetCurrentOptions();

 private:
  // Returns an iterator reference to the lowest option in the current search
  // space of this option list index. See comment for OptionRange.
  std::vector<OptionRange>::const_iterator GetSearchSpaceLowerBound(size_t opt_list_idx);

  // Returns an iterator reference to the exclusive highest option range in the current search
  // space of this option list index. See comment for OptionRange.
  std::vector<OptionRange>::const_iterator GetSearchSpaceUpperBound(size_t opt_list_idx);

  // Gets the option range that corresponds to the given option index at the given
  // option list index.
  std::vector<OptionRange>::const_iterator GetOptAtIndex(size_t opt_list_idx, size_t opt_index);

  // Sets the option that corresponds to the given option index at the given
  // logical option list index.
  void SetOptToIndex(size_t opt_list_idx, size_t opt_index);

  // Sets an entire group to a particular logical option index.
  void SetGroup(size_t opt_list_idx, size_t opt_index);

  KeyBytes prev_scan_target_;

  // The following fields aid in the goal of iterating through all possible
  // scan key values based on given IN-lists and range filters.

  // The following encodes the list of ranges we are iterating over
  std::vector<std::vector<OptionRange>> range_cols_scan_options_;

  // Vector of references to currently active elements being used
  // in range_cols_scan_options_
  // current_scan_target_ranges_[i] gives us the current OptionRange
  // column i is iterating over of the elements in
  // range_cols_scan_options_[i]
  mutable std::vector<std::vector<OptionRange>::const_iterator> current_scan_target_ranges_;

  bool is_options_done_ = false;

  const KeyBytes lower_doc_key_;
  const KeyBytes upper_doc_key_;

  // When we have tuple IN filters such as (r1,r3) IN ((1,3), (2,5) ...) where
  // (r1,r2,r3) is the index key, we cannot simply just populate
  // range_cols_scan_options_ with the values r1 -> {[1,1], [2,2]...} and
  // r3 -> {[3,3], [5,5]...}. This by itself implies that (r1,r3) = (1,5) is allowed.
  // In order to account for this case, we must introduce the possiblity of correlations amongst
  // key expressions. In order to achieve this, we put key expressions into "groups".
  // In the above example, we must put r1 and r3 into one such group and r2 into another by itself.
  // col_groups_ holds these groups. We maintain that all the key expressions in a group.
  // are on the same option index at any given time.
  // For example: Say (r1,r2,r3) is the index key.
  // If we received a filter of the form (r1,r3) IN ((1,3), (2,5)) AND r2 IN (4,6)
  // We would have groups [[0,2], [1]]
  // The scan keys we would iterate over would be
  // (1,4,3), (1,6,3), (2,4,3), (2,6,3)
  ColGroupHolder col_groups_;

  size_t prefix_length_ = 0;
};

}  // namespace docdb
}  // namespace yb
