#pragma once
#include "DiskManager.h"
#include "LRUReplacer.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <shared_mutex>
using namespace std;

struct Page {
	int page_id = -1;					// 页ID
	char data[PAGE_SIZE] = { 0 };		// 页数据
	bool is_dirty = false;				// 是否被修改过
	int pin_count = 0;					// 引用计数, 0 才能被踢出
	mutable shared_mutex page_latch;	// 页级读写锁
};

class BufferPoolManager {
public:
	BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
	~BufferPoolManager();

	Page* fetch_page(int page_id);                 // 获取页
	bool unpin_page(int page_id, bool is_dirty);   // 取消固定页
	Page* new_page(int& page_id);                  // 创建新页
	bool flush_page(int page_id);                  // 刷新页到磁盘

private:
	size_t pool_size_;           
	Page* pages_;                // frame 数组，固定大小
	DiskManager* disk_manager_;  
	LRUReplacer* replacer_;      

	unordered_map<int, size_t> page_table_;  // 页ID到页数组索引的映射
	list<int> free_list_;                    // 空闲页列表, 还没用过的空闲 frame_id
	mutex latch_;                            
	
	// 抽出来复用：找一个可用的 frame（先查空闲列表，再让 LRU 踢人）
	// 返回 frame_id，-1 表示找不到
	// 注意：调用前必须已持有 latch_，所以这里不加锁
	int find_free_frame();
};