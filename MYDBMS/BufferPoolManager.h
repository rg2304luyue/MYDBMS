#pragma once
#include "DiskManager.h"
#include "LRUReplacer.h"
#include <unordered_map>
using namespace std;

struct Page {
	int page_id = -1; // 页ID
	char data[PAGE_SIZE] = { 0 }; // 页数据
	bool is_dirty = false; // 是否被修改过
	int pin_count = 0; // 引用计数
};

class BufferPoolManager {
public:
	BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
	~BufferPoolManager();

	Page* fetch_page(int page_id); // 获取页
	bool unpin_page(int page_id, bool is_dirty); // 取消固定页
	Page* new_page(int& page_id); // 创建新页
	bool flush_page(int page_id); // 刷新页到磁盘

private:
	size_t pool_size_; // 缓冲池大小
	Page* pages_; // 页数组
	DiskManager* disk_manager_; // 磁盘管理器
	unordered_map<int, size_t> page_table_; // 页ID到页数组索引的映射
	list<int> free_list_; // 空闲页列表
	mutex latch_; // 互斥锁
	LRUReplacer* replacer_; // LRU替换器
};