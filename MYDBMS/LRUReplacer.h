#pragma once
#include <list>
#include <unordered_map>
#include <mutex>
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

	size_t Size(); // 当前有多少个 frame 在候选列表里

private:
	size_t capacity_;  // 最大可管理的 frame 数量
	mutex latch_; // 保护 lru_list_ 和 lru_map_ 的互斥锁
	list<int> lru_list_; // 存储未被 Pin 的 frame_id，头部=最近使用，尾部=最久未用
	unordered_map<int, list<int>::iterator> lru_map_; // frame_id 到 lru_list_ 迭代器的映射
};