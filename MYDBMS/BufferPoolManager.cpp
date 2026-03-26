#include "BufferPoolManager.h"
using namespace std;

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
	: pool_size_(pool_size), disk_manager_(disk_manager) {
	pages_ = new Page[pool_size_];
	replacer_ = new LRUReplacer(pool_size_);
	for(size_t i = 0; i < pool_size_; ++i) {
		free_list_.push_back(static_cast<int>(i));
	}
}

BufferPoolManager::~BufferPoolManager() {
	delete[] pages_;
	delete replacer_;
}

Page* BufferPoolManager::fetch_page(int page_id) {
	lock_guard<mutex> lock(latch_);
	// 如果页已经在缓冲池中，直接返回
	if(page_table_.count(page_id)) {
		int frame_id = page_table_[page_id];
		pages_[frame_id].pin_count++;
		replacer_->Pin(frame_id);
		return &pages_[frame_id];
	}

	// 找一个可用的内存位置(frame)
	int frame_id = -1;
	if(!free_list_.empty()) {
		frame_id = free_list_.front();
		free_list_.pop_front();
	} else if(replacer_->Victim(&frame_id)) {
		// 如果是从 LRU 踢出的，检查是否需要写回磁盘
		if (pages_[frame_id].is_dirty) {
			disk_manager_->write_page(pages_[frame_id].page_id, pages_[frame_id].data);
		}
		page_table_.erase(pages_[frame_id].page_id);
	} else if(frame_id == -1) {
		return nullptr; // 无可用页
	}

	// 从磁盘读取页数据到缓冲池
	pages_[frame_id].page_id = page_id;
	disk_manager_->read_page(page_id, pages_[frame_id].data);
	pages_[frame_id].pin_count = 1;
	pages_[frame_id].is_dirty = false;
	page_table_[page_id] = frame_id;
	replacer_->Pin(frame_id);
	return &pages_[frame_id];
}

bool BufferPoolManager::unpin_page(int page_id, bool is_dirty) {
	lock_guard<mutex> lock(latch_);
	if(!page_table_.count(page_id)) {
		return false; // 页不在缓冲池中
	}
	int frame_id = page_table_[page_id];
	if (is_dirty) pages_[frame_id].is_dirty = true;

	if (pages_[frame_id].pin_count > 0) {
		pages_[frame_id].pin_count--;
		if(pages_[frame_id].pin_count == 0) {
			replacer_->Unpin(frame_id);
		}
		return true;
	}
	return false; // 页未被固定
}

Page* BufferPoolManager::new_page(int& page_id) {
	lock_guard<mutex> lock(latch_);
	int frame_id = -1;
	if(!free_list_.empty()) {
		frame_id = free_list_.front();
		free_list_.pop_front();
	} else if(replacer_->Victim(&frame_id)) {
		if (pages_[frame_id].is_dirty) {
			disk_manager_->write_page(pages_[frame_id].page_id, pages_[frame_id].data);
		}
		page_table_.erase(pages_[frame_id].page_id);
	} else if(frame_id == -1) {
		return nullptr; // 无可用页
	}
	page_id = disk_manager_->get_total_pages(); // 获取新页ID
	pages_[frame_id].page_id = page_id;
	memset(pages_[frame_id].data, 0, PAGE_SIZE); // 初始化页数据
	pages_[frame_id].pin_count = 1;
	pages_[frame_id].is_dirty = false;
	page_table_[page_id] = frame_id;
	replacer_->Pin(frame_id);
	return &pages_[frame_id];
}