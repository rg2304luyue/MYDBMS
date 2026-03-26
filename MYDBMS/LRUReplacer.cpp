#include "LRUReplacer.h"
using namespace std;

LRUReplacer::LRUReplacer(size_t num_pages): capacity_(num_pages) {}
LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(int* frame_id) {
	lock_guard<mutex> lock(latch_);
	if (lru_list_.empty()) {
		return false; // 没有可替换的页面
	}

	*frame_id = lru_list_.back(); // 获取最久未使用的 frame_id
	lru_map_.erase(*frame_id); // 从映射中移除
	lru_list_.pop_back(); // 从列表中移除
	return true;
}

void LRUReplacer::Pin(int frame_id) {
	lock_guard<mutex> lock(latch_);

	// 如果它在候选列表里，把它移除（它现在被人用了，不能被踢）
	auto it = lru_map_.find(frame_id);
	if (it != lru_map_.end()) {
		lru_list_.erase(it->second); // 从列表中移除
		lru_map_.erase(it); // 从映射中移除
	}
	// 如果不在列表里，说明它本来就是 pin 状态，什么都不用做
}

void LRUReplacer::Unpin(int frame_id) {
	lock_guard<mutex> lock(latch_);

	if (lru_map_.count(frame_id)) return; // 已经在候选项中，无需重复添加

	lru_list_.push_front(frame_id); // 将新的 frame_id 添加到列表前面
	lru_map_[frame_id] = lru_list_.begin(); // 更新映射
}

size_t LRUReplacer::Size() {
	lock_guard<mutex> lock(latch_);
	return lru_list_.size(); // 返回当前候选项的数量
}