//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  clock_hand_ = 0;
  clock_.resize(num_pages);
  // init clock
  for (size_t i = 0; i < num_pages; i++) {
    clock_[i].first = -1;
    clock_[i].second = false;
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_map_.size() == 0) {
    return false;
  }
  bool found = false;
  do {
    if (clock_[clock_hand_].first != -1) {
      if (clock_[clock_hand_].second == false) {
        *frame_id = clock_[clock_hand_].first;
        clock_[clock_hand_].first = -1;
        found = true;
        frame_map_.erase(*frame_id);
      } else {
        clock_[clock_hand_].second = false;
      }
    }
    clock_hand_ = (clock_hand_ + 1) % clock_.size();
  } while (!found);
  return found;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = frame_map_.find(frame_id);
  if (it != frame_map_.end()) {
      clock_[it->second].first = -1;
      clock_[it->second].second = false;
      frame_map_.erase(it);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    for (size_t i = 0; i < clock_.size(); i++) {
      if (clock_[i].first == -1) {
        clock_[i].first = frame_id;
        clock_[i].second = true;
        frame_map_[frame_id] = i;
        return;
      }
    }
  }
  clock_[it->second].second = true;
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> lock(latch_);
  return frame_map_.size();
}

}  // namespace bustub
