#pragma once
#include "BufferPoolManager.h"
using namespace std;

// 节点类型标识
enum class PageType { LEAF = 0, INTERNAL = 1};

// B+树公共头部
struct BTreePageHeader {
	PageType type;   // 节点类型
	int size;        // 当前存储的键值对数量
	int max_size;    // 最大容量
	int parent_id;   // 父节点 ID, 根节点为-1
	int page_id;     // 当前节点 ID
};

// 记录在数据文件中的物理位置
struct Rid{
	int page_id;   // 页 ID
	int slot_num;  // 插槽号
};

// ── 叶子节点布局 ──────────────────────────────────────────────
// 整个结构体 reinterpret_cast 到一个 Page::data[4096] 上
// max_size = (PAGE_SIZE - sizeof(header) - sizeof(next_page_id)) / sizeof(KV)
struct LeafPage {
	BTreePageHeader header;
	int next_page_id; // 叶子节点间的横向指针
	pair<int, Rid> data[1]; // 柔性数组或固定大小偏移计算

	static int calc_max_size() {
		int header_bytes = sizeof(BTreePageHeader) + sizeof(int);
		return static_cast<int>(
			(PAGE_SIZE - header_bytes) / sizeof(pair<int, Rid>)
			);
	}
};

// ── 内部节点布局 ──────────────────────────────────────────────
// 结构：array[0].child | array[1].key, array[1].child | array[2].key, array[2].child ...
// 含义：array[0].child 是最左子树，array[i].key 是第 i 个分割键
// size 表示有多少个有效的 child（比 key 的数量多 1）
struct InternalPage {
	BTreePageHeader header;
	struct Entry {
		int key;            // array[0].key 不使用（或设为 INT_MIN）
		int child_page_id;
	};
	Entry array[1];

	static int calc_max_size() {
		int header_bytes = sizeof(BTreePageHeader);
		return static_cast<int>(
			(PAGE_SIZE - header_bytes) / sizeof(Entry)
			);
	}
};

class BPlusTree {
public:
	explicit BPlusTree(BufferPoolManager* bpm);

	bool Insert(int key, const Rid& rid);   // 插入键值对
	bool Search(int key, Rid* result);      // 查找 key 对应的 RID
	void Print();                           // 调试用，打印树结构

private:
	BufferPoolManager* bpm_;
	int root_page_id_;

	// ── 查找 ──
	// 返回 key 所在叶子节点的 Page*，调用方负责 unpin
	Page* find_leaf(int key);

	// 在内部节点中查找 key 应该走哪个 child
	int lookup_internal(InternalPage* node, int key);

	// ── 插入辅助 ──
	void insert_into_leaf(LeafPage* leaf, int key, const Rid& rid);
	void insert_into_parent(Page* old_page, int push_up_key, Page* new_page);
	void insert_into_internal(InternalPage* node, int key, int new_child_id);

	// ── 分裂 ──
	void split_leaf(Page* leaf_page);
	void split_internal(Page* internal_page, int key, int new_child_id);

	// ── 工具 ──
	void init_leaf(LeafPage* leaf, int page_id, int parent_id);
	void init_internal(InternalPage* node, int page_id, int parent_id);
	void update_parent_id(int child_page_id, int new_parent_id);
};