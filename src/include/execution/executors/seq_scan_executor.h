//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) { plan_ = plan; }

  void Init() override {
    meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    // Start an iterator from the first page.
    auto page = static_cast<TablePage *>(exec_ctx_->GetBufferPoolManager()->FetchPage(meta_->table_->GetFirstPageId()));
    page->RLatch();
    RID rid;
    // If this fails because there is no tuple, then RID will be the default-constructed value, which means EOF.
    page->GetFirstTupleRid(&rid);
    page->RUnlatch();
    exec_ctx_->GetBufferPoolManager()->UnpinPage(meta_->table_->GetFirstPageId(), false);
    iter_ = std::make_unique<TableIterator>(meta_->table_.get(), rid, exec_ctx_->GetTransaction());
  }

  bool Next(Tuple *tuple) override {
    if (meta_->table_.get() == nullptr) return false;
    while (plan_->GetPredicate() != nullptr && (*iter_ != meta_->table_->End()) &&
           !plan_->GetPredicate()->Evaluate(iter_->operator->(), &meta_->schema_).GetAs<bool>()) {
      ++(*(iter_.get()));
    }
    if (*iter_ == meta_->table_->End()) {
      return false;
    }
    std::vector<Value> values;
    for (auto col : plan_->OutputSchema()->GetColumns()) {
      values.push_back(col.GetExpr()->Evaluate(iter_->operator->(), plan_->OutputSchema()));
    }
    Tuple tuple1(values, plan_->OutputSchema());
    *tuple = tuple1;
    ++(*(iter_.get()));
    return true;
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  std::unique_ptr<TableIterator> iter_;
  TableMetadata *meta_;
};
}  // namespace bustub
