//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
void LogManager::Flush() {
  std::unique_lock lock(latch_);
  auto lsn = log_buffer_lsn_.load();
  auto offset = offset_.load();
  offset_ = 0;
  lock.unlock();
  std::swap(log_buffer_, flush_buffer_);
  printf("offset:%lu\n", offset);
  disk_manager_->WriteLog(flush_buffer_, offset);
  persistent_lsn_ = lsn;
}
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() { enable_logging = true; }

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() { enable_logging = false; }

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  // First, serialize the must have fields(20 bytes in total)
  // std::unique_lock lock(latch_);
  if (LOG_BUFFER_SIZE - offset_ < LogRecord::HEADER_SIZE) {
    Flush();
  }
  log_record->lsn_ = next_lsn_++;
  memcpy(log_buffer_ + offset_, log_record, LogRecord::HEADER_SIZE);
  if ((int32_t)(LOG_BUFFER_SIZE - offset_) < log_record->size_) {
    Flush();
    // do it again in new buffer
    memcpy(log_buffer_ + offset_, log_record, LogRecord::HEADER_SIZE);
  }
  log_buffer_lsn_ = log_record->lsn_;
  int pos = offset_ + LogRecord::HEADER_SIZE;
  offset_ += log_record->size_;
  // lock.unlock();

  if (log_record->log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    // we have provided serialize function for tuple class
    log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::APPLYDELETE ||
             log_record->log_record_type_ == LogRecordType::MARKDELETE ||
             log_record->log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    // we have provided serialize function for tuple class
    log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
    memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
    pos += sizeof(RID);
    // we have provided serialize function for tuple class
    log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += sizeof(log_record->old_tuple_.GetLength() + sizeof(uint32_t));
    log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
    memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
  }

  return log_record->lsn_;
}

}  // namespace bustub
