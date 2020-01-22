//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}
frame_id_t BufferPoolManager::GetAvailableFrameImpl() {
    // first find the free list
  frame_id_t r = -1;
  std::unique_lock<std::mutex> lock(latch_);
  if (free_list_.size() > 0) {
    r = free_list_.front();
    free_list_.pop_front();
  } else {
    lock.unlock();
    // get from replacement
    bool ret = replacer_->Victim(&r);
    assert(ret == true);
    Page* page = &(pages_[r]);
    if (page->IsDirty()) {
      // write it back
      page->WLatch();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
      page->WUnlatch();
    }
    lock.lock();
    // remove from table
    page_table_.erase(page->GetPageId());
  }
  lock.unlock();
  return r;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      Page *page = &(pages_[it->second]);
      page->pin_count_++;
      replacer_->Pin(it->second);
      return page;
    }
  }
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  // first find the free list
  frame_id_t r = GetAvailableFrameImpl();
  Page* page = &(pages_[r]);
  page->WLatch();
  page->pin_count_++;
  page->page_id_ = page_id;
  // read it from disk
  disk_manager_->ReadPage(page->GetPageId(), page->GetData());
  page->WUnlatch();
  std::unique_lock<std::mutex> lock(latch_);
  // set new page to table
  page_table_[page_id] = r;
  lock.unlock();
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  Page* page = nullptr;
  frame_id_t frame_id = -1;
  {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }
    frame_id = it->second;
    page = &(pages_[frame_id]);
  }
  page->WLatch();
  page->is_dirty_ = is_dirty;
  if (page->GetPinCount() <= 0) {
    page->WUnlatch();
    return false;
  }
  page->pin_count_--;
  if (page->GetPinCount() <= 0) {
    replacer_->Unpin(frame_id);
  }
  page->WUnlatch();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  Page* page = nullptr;
  {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      // not found
      return false;
    }
    page = &(pages_[it->second]);
  }
  if (!page->IsDirty()) {
    return true;
  }
  page->WLatch();
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  page->WUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t r = GetAvailableFrameImpl();
  *page_id = disk_manager_->AllocatePage();
  Page* page = &(pages_[r]);
  page->WLatch();
  page->pin_count_++;
  page->page_id_ = *page_id;
  // read it from disk
  page->ResetMemory();
  page->WUnlatch();
  std::unique_lock<std::mutex> lock(latch_);
  // set new page to table
  page_table_[*page_id] = r;
  lock.unlock();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  Page *page = nullptr;
  {
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      // not found
      return false;
    }
    page = &(pages_[it->second]);
  }
  
  page->WLatch();
  if(page->GetPinCount() > 0) {
    page->WUnlatch();
    return false;
  }
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  page->WUnlatch();
  std::lock_guard<std::mutex> lock(latch_);
  page_table_.erase(page_id);
  
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (auto it : page_table_) {
    if (!pages_[it.second].IsDirty()) {
      continue;
    }
    pages_[it.second].WLatch();
    disk_manager_->WritePage(it.first, pages_[it.second].GetData());
    pages_[it.second].is_dirty_ = false;
    pages_[it.second].WUnlatch();
  }
}

}  // namespace bustub
