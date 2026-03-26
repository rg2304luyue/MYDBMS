#include "BufferPoolManager.h"
using namespace std;

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
	: pool_size_(pool_size), disk_manager_(disk_manager) {
	pages_ = new Page[pool_size_];
	replacer_ = new LRUReplacer(pool_size_);

	// 一开始所有 frame 都是空闲的
	for(size_t i = 0; i < pool_size_; ++i) {
		free_list_.push_back(static_cast<int>(i));
	}
}

BufferPoolManager::~BufferPoolManager() {
	// 析构时把所有脏页写回磁盘，防止数据丢失
	for (size_t i = 0; i < pool_size_; ++i) {
		if (pages_[i].is_dirty && pages_[i].page_id != -1) {
			disk_manager_->write_page(pages_[i].page_id, pages_[i].data);
		}
	}

	delete[] pages_;
	delete replacer_;
}

int BufferPoolManager::find_free_frame() {
	// 调用者已持有 latch_，这里不重复加锁

	if (!free_list_.empty()) {
		// 优先用从未使用过的空闲 frame
		int frame_id = free_list_.front();
		free_list_.pop_front();
		return frame_id;
	}

	// 没有空闲 frame，尝试让 LRU 踢出一个
	int frame_id = -1;
	if (!replacer_->Victim(&frame_id)) {
		return -1;  // 所有 frame 都被 pin 住了，真的没法腾出位置.
	}

	// 被踢出的 frame 如果是脏页，先写回磁盘
	Page& victim = pages_[frame_id];
	if (victim.is_dirty) {
		disk_manager_->write_page(victim.page_id, victim.data);
	}

	// 从页表中移除被踢出的页
	page_table_.erase(victim.page_id);

	return frame_id;
}

Page* BufferPoolManager::fetch_page(int page_id) {
	lock_guard<mutex> lock(latch_);

	// 页已经在缓冲池中，直接返回
	auto it = page_table_.find(page_id);
	if(it != page_table_.end()) {
		int frame_id = it->second;
		pages_[frame_id].pin_count++;
		replacer_->Pin(frame_id); // 从 LRU 候选列表移除，它正在被用
		return &pages_[frame_id];
	}

	// 页不在缓冲池，需要从磁盘加载
	int frame_id = find_free_frame();;
	if(frame_id == -1) {
		return nullptr; // 缓冲池满了且所有页都被 pin 住
	} 

	// 从磁盘读取页数据到缓冲池
	Page& page = pages_[frame_id];
	page.page_id = page_id;
	page.is_dirty = false;
	page.pin_count = 1; // 刚被 fetch 出来，pin_count 从 0 变成 1
	disk_manager_->read_page(page_id, page.data);

	// 更新页表和 LRU 替换器
	page_table_[page_id] = frame_id;
	replacer_->Pin(frame_id); // 从 LRU 候选列表移除，它正在被用

	return &page;
}

bool BufferPoolManager::unpin_page(int page_id, bool is_dirty) {
	lock_guard<mutex> lock(latch_);

	auto it = page_table_.find(page_id);
	if (it == page_table_.end()) {
		return false;  // 这页根本不在缓冲池里
	}

	int frame_id = it->second;
	Page& page = pages_[frame_id];

	if (page.pin_count <= 0) {
		return false;  // 已经是 0 了，说明调用方多调了一次 unpin，属于 bug
	}

	// is_dirty 只能从 false 变 true，不能反向清除脏标记
	// （写回磁盘是 flush_page 的职责，不是 unpin 的）
	if (is_dirty) {
		page.is_dirty = true;
	}

	page.pin_count--;
	if (page.pin_count == 0) {
		replacer_->Unpin(frame_id); // 加入 LRU 候选列表，可能被踢出
	}

	return true;
}

Page* BufferPoolManager::new_page(int& page_id) {
	lock_guard<mutex> lock(latch_);

	int frame_id = find_free_frame();
	if (frame_id == -1) {
		return nullptr;
	}

	// 分配新的 page_id：文件末尾追加一页
	page_id = disk_manager_->get_total_pages();

	Page& page = pages_[frame_id];
	page.page_id = page_id;
	page.is_dirty = false;
	page.pin_count = 1;
	memset(page.data, 0, PAGE_SIZE);  // 新页内容清零

	// 立刻在磁盘上占位，否则文件大小不变，
	// 下次调用 get_total_pages() 会分配到同一个 page_id
	disk_manager_->write_page(page_id, page.data);

	// 更新页表和 LRU 替换器
	page_table_[page_id] = frame_id;
	replacer_->Pin(frame_id); // 从 LRU 候选列表移除，它正在被用

	return &page;
}

bool BufferPoolManager::flush_page(int page_id) {
	lock_guard<mutex> lock(latch_);

	auto it = page_table_.find(page_id);
	if (it == page_table_.end()) {
		return false;  // 这页根本不在缓冲池里
	}

	Page& page = pages_[it->second];
	disk_manager_->write_page(page_id, page.data);
	page.is_dirty = false; // 刷新后不再是脏页

	return true;
}