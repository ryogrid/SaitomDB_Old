//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "container/hash/linear_probe_hash_table.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
class IdentityHashFunction : public HashFunction<hash_t> {
 public:
  /**
   * Hashes the key.
   * @param key the key to be hashed
   * @return the hashed value
   */
  uint64_t GetHash(size_t key) override { return key; }
};

/**
 * A simple hash table that supports hash joins.
 */
class SimpleHashJoinHashTable {
 public:
  /** Creates a new simple hash join hash table. */
  SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
                          const IdentityHashFunction &hash_fn) {}

  /**
   * Inserts a (hash key, tuple) pair into the hash table.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param t the tuple to associate with the key
   * @return true if the insert succeeded
   */
  bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
    hash_table_[h].emplace_back(t);
    return true;
  }

  /**
   * Gets the values in the hash table that match the given hash key.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param[out] t the list of tuples that matched the key
   */
  void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }

 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
// using HT = SimpleHashJoinHashTable;

using HashJoinKeyType = hash_t;
using HashJoinValType = TmpTuple;
using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, HashComparator>;

/**
 * HashJoinExecutor executes hash join operations.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new hash join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the hash join plan node
   * @param left the left child, used by convention to build the hash table
   * @param right the right child, used by convention to probe the hash table
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
                   std::unique_ptr<AbstractExecutor> &&right)
      : AbstractExecutor(exec_ctx),
        jht_("", exec_ctx->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_) {
    value_index_ = 0;
    plan_ = plan;
    left_ = std::move(left);
    right_ = std::move(right);
  }

  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    left_->Init();
    // init hash table
    Tuple tuple;
    page_id_t tmp_page_id;
    auto tmp_tuple_page =
        reinterpret_cast<TmpTuplePage *>(exec_ctx_->GetBufferPoolManager()->NewPage(&tmp_page_id, nullptr)->GetData());
    tmp_tuple_page->Init(tmp_page_id, PAGE_SIZE);
    tmp_page_list.push_back(tmp_page_id);
    while (left_->Next(&tuple)) {
      hash_t t = HashValues(&tuple, plan_->GetLeftPlan()->OutputSchema(), plan_->GetLeftKeys());
      TmpTuple out(INVALID_PAGE_ID, 0);
      if (!tmp_tuple_page->Insert(tuple, &out)) {
        exec_ctx_->GetBufferPoolManager()->UnpinPage(tmp_page_id, true, nullptr);
        tmp_tuple_page = reinterpret_cast<TmpTuplePage *>(
            exec_ctx_->GetBufferPoolManager()->NewPage(&tmp_page_id, nullptr)->GetData());
        tmp_tuple_page->Init(tmp_page_id, PAGE_SIZE);
        // insert again
        tmp_tuple_page->Insert(tuple, &out);
        tmp_page_list.push_back(tmp_page_id);
      }
      if (!jht_.Insert(exec_ctx_->GetTransaction(), t, out)) {
        jht_.Resize(jht_.GetSize() * 2);
        jht_.Insert(exec_ctx_->GetTransaction(), t, out);
      }
    }
    exec_ctx_->GetBufferPoolManager()->UnpinPage(tmp_page_id, true, nullptr);
    // init right iter
    right_->Init();
  }

  bool Next(Tuple *tuple) override {
    if (!right_tuple_.IsAllocated()) {
      if (!right_->Next(&right_tuple_)) {
        return false;
      }
    }
    while (true) {
      hash_t h = HashValues(&right_tuple_, plan_->GetRightPlan()->OutputSchema(), plan_->GetRightKeys());
      std::vector<TmpTuple> t;
      jht_.GetValue(exec_ctx_->GetTransaction(), h, &t);
      if (t.empty()) {
        if (!right_->Next(&right_tuple_)) {
          return false;
        }
      }
      while (value_index_ < t.size()) {
        auto tmp_tuple_page = reinterpret_cast<TmpTuplePage *>(
            exec_ctx_->GetBufferPoolManager()->FetchPage(t[value_index_].GetPageId(), nullptr)->GetData());
        Tuple left_tuple;
        tmp_tuple_page->GetTuple(t[value_index_].GetOffset(), &left_tuple);
        exec_ctx_->GetBufferPoolManager()->UnpinPage(t[value_index_].GetPageId(), false, nullptr);
        if (plan_->Predicate()
                ->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple_,
                               plan_->GetRightPlan()->OutputSchema())
                .GetAs<bool>()) {
          std::vector<Value> values;
          for (auto col : plan_->OutputSchema()->GetColumns()) {
            values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(),
                                                         &right_tuple_, plan_->GetRightPlan()->OutputSchema()));
          }
          Tuple tuple1(values, plan_->OutputSchema());
          *tuple = tuple1;
          value_index_++;
          return true;
        }
        value_index_++;
      }
      value_index_ = 0;
      if (!right_->Next(&right_tuple_)) {
        return false;
      }
    }
    return false;
  }

  /**
   * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
   * @param tuple tuple to be hashed
   * @param schema schema to evaluate the tuple on
   * @param exprs expressions to evaluate the tuple with
   * @return the hashed tuple
   */
  hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
    hash_t curr_hash = 0;
    // For every expression,
    for (const auto &expr : exprs) {
      // We evaluate the tuple on the expression and schema.
      Value val = expr->Evaluate(tuple, schema);
      // If this produces a value,
      if (!val.IsNull()) {
        // We combine the hash of that value into our current hash.
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }

 private:
  Tuple right_tuple_;
  size_t value_index_;
  std::unique_ptr<AbstractExecutor> left_;
  std::unique_ptr<AbstractExecutor> right_;
  /** The hash join plan node. */
  const HashJoinPlanNode *plan_;
  /** The comparator is used to compare hashes. */
  [[maybe_unused]] HashComparator jht_comp_{};
  /** The identity hash function. */
  IdentityHashFunction jht_hash_fn_{};
  TableMetadata *meta_;
  std::vector<page_id_t> tmp_page_list;

  /** The hash table that we are using. */
  HT jht_;
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;
};
}  // namespace bustub
