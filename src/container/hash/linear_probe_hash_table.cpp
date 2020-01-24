//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // init buckets
  num_buckets_ = num_buckets;
  num_entries_ = 0;
  InitPages(num_buckets_, &this->header_page_id_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::InitPages(size_t num_buckets, page_id_t *header_page_id) {
  size_t block_num = (num_buckets / BLOCK_ARRAY_SIZE) + 1;
  // init header page
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(this->buffer_pool_manager_->NewPage(header_page_id, nullptr)->GetData());
  header_page->SetPageId(*header_page_id);
  header_page->SetSize(block_num);

  for (size_t i = 0; i < block_num; i++) {
    // get a block page from the BufferPoolManager
    page_id_t block_page_id = INVALID_PAGE_ID;
    this->buffer_pool_manager_->NewPage(&block_page_id, nullptr);
    header_page->AddBlockPageId(block_page_id);
    this->buffer_pool_manager_->UnpinPage(block_page_id, false);
  }
  this->buffer_pool_manager_->UnpinPage(this->header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  if (num_entries_ <= 0) {
    table_latch_.RUnlock();
    return false;
  }
  uint64_t hash_key = hash_fn_.GetHash(key);
  size_t bucket_idx = hash_key % num_buckets_;
  size_t old_bucket_idx = bucket_idx;
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  bool find = false;
  do {
    size_t block_idx = bucket_idx / BLOCK_ARRAY_SIZE;
    page_id_t page_id = header_page->GetBlockPageId(block_idx);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(page_id, nullptr)->GetData());
    do {
      auto bucket_ind = bucket_idx % BLOCK_ARRAY_SIZE;
      // std::cout << "get num_buckets: " << num_buckets_ << " block_idx:" << block_idx << " bucket_ind:" << bucket_ind
      // << " key:"<<key << std::endl;
      // not any more
      if (!block_page->IsOccupied(bucket_ind)) {
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->UnpinPage(header_page_id_, false);
        table_latch_.RUnlock();
        return find;
      }
      if (block_page->IsReadable(bucket_ind) && comparator_(block_page->KeyAt(bucket_ind), key) == 0) {
        find = true;
        result->push_back(block_page->ValueAt(bucket_ind));
      }
      bucket_idx = (bucket_idx + 1) % num_buckets_;
    } while (old_bucket_idx != bucket_idx && bucket_idx % BLOCK_ARRAY_SIZE != 0);
    buffer_pool_manager_->UnpinPage(page_id, false);
  } while (old_bucket_idx != bucket_idx);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return find;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  bool ret = InsertImpl(transaction, header_page, key, value);
  if (ret) {
    num_entries_++;
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  uint64_t hash_key = hash_fn_.GetHash(key);
  size_t bucket_idx = hash_key % num_buckets_;
  size_t old_bucket_idx = bucket_idx;
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  do {
    size_t block_idx = bucket_idx / BLOCK_ARRAY_SIZE;
    page_id_t page_id = header_page->GetBlockPageId(block_idx);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(page_id, nullptr)->GetData());
    do {
      // not found
      auto bucket_ind = bucket_idx % BLOCK_ARRAY_SIZE;
      if (!block_page->IsOccupied(bucket_ind)) {
        buffer_pool_manager_->UnpinPage(page_id, true);
        buffer_pool_manager_->UnpinPage(header_page_id_, false);
        table_latch_.RUnlock();
        return false;
      }
      // exist?
      if (block_page->IsReadable(bucket_ind)) {
        if (comparator_(block_page->KeyAt(bucket_ind), key) == 0 && block_page->ValueAt(bucket_ind) == value) {
          block_page->Remove(bucket_ind);
          buffer_pool_manager_->UnpinPage(page_id, true);
          buffer_pool_manager_->UnpinPage(header_page_id_, false);
          num_entries_--;
          table_latch_.RUnlock();
          return true;
        }
      }
      bucket_idx = (bucket_idx + 1) % num_buckets_;
    } while (old_bucket_idx != bucket_idx && bucket_idx % BLOCK_ARRAY_SIZE != 0);
    buffer_pool_manager_->UnpinPage(page_id, false);
  } while (old_bucket_idx != bucket_idx);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  if (num_entries_ >= initial_size) {
    table_latch_.WUnlock();
    return;
  }
  num_buckets_ = initial_size;
  page_id_t new_header_page_id = INVALID_PAGE_ID;
  InitPages(initial_size, &new_header_page_id);
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  auto new_header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(new_header_page_id, nullptr)->GetData());
  size_t bucket_cnt = 0;
  for (size_t block_idx = 0; block_idx < header_page->GetSize(); block_idx++) {
    page_id_t page_id = header_page->GetBlockPageId(block_idx);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(page_id, nullptr)->GetData());
    for (size_t bucket_ind = 0; bucket_ind < BLOCK_ARRAY_SIZE && bucket_cnt < num_buckets_;
         bucket_ind++, bucket_cnt++) {
      if (block_page->IsReadable(bucket_ind)) {
        // LOG_DEBUG("bucket_ind:%ld, ", bucket_ind);
        InsertImpl(nullptr, new_header_page, block_page->KeyAt(bucket_ind), block_page->ValueAt(bucket_ind));
      }
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    bool ret = buffer_pool_manager_->DeletePage(page_id);
    assert(ret);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  bool ret = buffer_pool_manager_->DeletePage(header_page_id_);
  assert(ret);
  header_page_id_ = new_header_page_id;
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return num_buckets_;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InsertImpl(Transaction *transaction, HashTableHeaderPage *header_page, const KeyType &key,
                                 const ValueType &value) {
  uint64_t hash_key = hash_fn_.GetHash(key);
  size_t bucket_idx = hash_key % num_buckets_;
  size_t old_bucket_idx = bucket_idx;
  do {
    size_t block_idx = bucket_idx / BLOCK_ARRAY_SIZE;
    page_id_t page_id = header_page->GetBlockPageId(block_idx);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(page_id, nullptr)->GetData());
    do {
      auto bucket_ind = bucket_idx % BLOCK_ARRAY_SIZE;
      if (block_page->Insert(bucket_ind, key, value)) {
        // std::cout << "insert num_buckets: "<< num_buckets_ << ", block_idx:" << block_idx << " bucket_ind:" <<
        // bucket_ind << " key:"<<key << " value:" << value << std::endl;
        buffer_pool_manager_->UnpinPage(page_id, true);
        return true;
      }
      // is already exist?
      if (block_page->IsReadable(bucket_ind)) {
        if (comparator_(block_page->KeyAt(bucket_ind), key) == 0 && block_page->ValueAt(bucket_ind) == value) {
          buffer_pool_manager_->UnpinPage(page_id, false);
          return false;
        }
      }
      bucket_idx = (bucket_idx + 1) % num_buckets_;
    } while (old_bucket_idx != bucket_idx && bucket_idx % BLOCK_ARRAY_SIZE != 0);
    buffer_pool_manager_->UnpinPage(page_id, false);
  } while (old_bucket_idx != bucket_idx);
  return false;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
