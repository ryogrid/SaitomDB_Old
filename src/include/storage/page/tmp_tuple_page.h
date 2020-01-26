#pragma once

#include <iostream>
#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  void Init(page_id_t page_id, uint32_t page_size) {
    // Set the page ID.
    memcpy(GetData(), &page_id, sizeof(page_id));
    SetFreeSpacePointer(page_size);
  }

  page_id_t GetTablePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    BUSTUB_ASSERT(tuple.GetLength() > 0, "Cannot have empty tuples.");
    // If there is not enough space, then return false.
    if (GetFreeSpaceRemaining() < tuple.GetLength() + SIZE_TUPLE) {
      return false;
    }

    // set tuple data
    SetFreeSpacePointer(GetFreeSpacePointer() - tuple.GetLength());
    memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.GetLength());

    // set tuple size
    SetFreeSpacePointer(GetFreeSpacePointer() - sizeof(uint32_t));
    memcpy(GetData() + GetFreeSpacePointer(), &tuple.size_, sizeof(uint32_t));
    TmpTuple tmp(GetTablePageId(), GetFreeSpacePointer());
    *out = tmp;
    return true;
  }
  /**
   * Read a tuple from a table.
   * @param rid rid of the tuple to read
   * @param[out] tuple the tuple that was read
   * @return true if the read is successful (i.e. the tuple exists)
   */
  bool GetTuple(const size_t offset, Tuple *tuple) {
    //  Copy the tuple data into our result.
    uint32_t tuple_size = *reinterpret_cast<uint32_t *>(GetData() + offset);
    uint32_t tuple_offset = offset + sizeof(uint32_t);
    tuple->size_ = tuple_size;
    if (tuple->allocated_) {
      delete[] tuple->data_;
    }
    tuple->data_ = new char[tuple->size_];
    memcpy(tuple->data_, GetData() + tuple_offset, tuple->size_);
    tuple->allocated_ = true;
    return true;
  }

 private:
  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 12;
  static constexpr size_t SIZE_TUPLE = 4;
  static constexpr size_t OFFSET_FREE_SPACE = sizeof(page_id_t) + sizeof(lsn_t);
  /** @return pointer to the end of the current free space, see header comment */
  uint32_t GetFreeSpacePointer() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE); }

  /** Sets the pointer, this should be the end of the current free space. */
  void SetFreeSpacePointer(uint32_t free_space_pointer) {
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space_pointer, sizeof(uint32_t));
  }
  uint32_t GetFreeSpaceRemaining() { return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER; }
  static_assert(sizeof(page_id_t) == 4);
};

}  // namespace bustub
