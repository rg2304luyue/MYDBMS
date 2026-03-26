#pragma once
#include <list>
#include <unordered_map>
#include <mutex>
#include <vector>
using namespace std;

class LRUReplacer {
public:
	explicit LRUReplacer(size_t num_pages);
	~LRUReplacer();

	// 选出一个最久未使用的frame_id进行替换
	bool Victim(int* frame_id);

	// 当页面被 Pin(正在被使用)时，从候选项移除
	void Pin(int frame_id);

	// 当页面被 Unpin(不再被使用)时，加入候选项
	void Unpin(int frame_id);

	size_t Size();

private:
	mutex latch_; // 保护 lru_list_ 和 cache_map_ 的互斥锁
	list<int> lru_list_; // 存储未被 Pin 的 frame_id，最前面是最近使用的
	unordered_map<int, list<int>::iterator> cache_map_; // frame_id 到 lru_list_ 迭代器的映射
};