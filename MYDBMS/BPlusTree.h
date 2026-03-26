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
	int parent_id;   // 父节点 ID
	int page_id;     // 当前节点 ID
};

// 具体的键值对结构
struct Rid{
	int page_id;   // 页 ID
	int slot_num;  // 插槽号
};

struct LeafNode {
	BTreePageHeader header;
	int next_page_id; // 叶子节点间的横向指针
	pair<int, Rid> data[1]; // 柔性数组或固定大小偏移计算
};

struct InternalNode {
	BTreePageHeader header;
	// 内部节点：第一个 child_id 对应的 Key 通常为空或负无穷
	// 结构：[Pointer 0, Key 1, Pointer 1, Key 2, Pointer 2...]
	struct MappingType {
		int32_t key;
		int32_t child_page_id;
	};
	MappingType array[250];
};

class BPlusTree {
public:
	BPlusTree(BufferPoolManager* bpm) : bpm_(bpm), root_page_id_(-1) {}

	// 核心功能：查找key所在的叶子节点
	Page* FindLeafNode(int key) {
		if (root_page_id_ == -1) return nullptr; // 树为空

		Page* page = bpm_->fetch_page(root_page_id_);
		BTreePageHeader* header = reinterpret_cast<BTreePageHeader*>(page->data);

		// 从根节点开始遍历
		while (header->type != PageType::LEAF) {
			int next_id = LookupInternal(page, key);
			bpm_->unpin_page(page->page_id, false); // 释放当前页
			page = bpm_->fetch_page(next_id); // 获取下一个节点
			header = reinterpret_cast<BTreePageHeader*>(page->data);
		}
		return page;
	}

private:
	BufferPoolManager* bpm_;
	int root_page_id_;

	int LookupInternal(Page* page, int key) {
		InternalNode* node = reinterpret_cast<InternalNode*>(page->data);
		for (int i = 1; i < node->header.size; ++i) { // 内部节点通常从第二个Key开始比较
			if (key < node->array[i].key) {
				return node->array[i - 1].child_page_id;
			}
		}
		return node->array[node->header.size - 1].child_page_id;
	}

	void InsertIntoParent(int old_page_id, int key, int new_page_id) {
		// 获取旧页面的Page对象已找到父节点ID
		Page* old_raw_page = bpm_->fetch_page(old_page_id);
		BTreePageHeader* old_header = reinterpret_cast<BTreePageHeader*>(old_raw_page->data);
		int parent_id = old_header->parent_id;
		bpm_->unpin_page(old_page_id, false); // 释放旧页面

		// 如果旧页面是根节点，创建新的根节点
		if (parent_id == -1) {
			int new_root_id;
			Page* root_raw_page = bpm_->new_page(&new_root_id);
			InternalNode* root = reinterpret_cast<InternalNode*>(root_raw_page->data);

			root->header.type = PageType::INTERNAL;
			root->header.size = 2; // 包含两个指针old, new
			root->header.parent_id = -1;
			root->header.page_id = new_root_id;

			// 设置指针和键值
			root->array[0].child_page_id = old_page_id;
			root->array[1].key = key; // 这里简单使用key作为分割键
			root->array[1].child_page_id = new_page_id;

			// 更新旧页面和新页面的父节点ID
			UpdateParentId(old_page_id, new_root_id);
			UpdateParentId(new_page_id, new_root_id);

			root_page_id_ = new_root_id; // 更新内存中的根节点 ID
			bpm_->unpin_page(new_root_id, true);
			return;
		}
		
		// 3. 如果已有父节点，将 Key 插入父节点
		Page* parent_raw_page = bpm_->fetch_page(parent_id);
		InternalNode* parent = reinterpret_cast<InternalNode*>(parent_raw_page->data);

		if (parent->header.size < parent->header.max_size) {
			// 父节点未满，直接按顺序插入并排序
			InsertIntoInternal(parent, key, new_page_id);
			bpm_->unpin_page(parent_id, true);
		}
		else {
			// 父节点也满了，继续触发【内部节点分裂】（递归逻辑）
			SplitInternal(parent_raw_page, key, new_page_id);
		}
	}

	void UpdateParentId(int child_page_id, int new_parent_id) {
		Page* page = bpm_->fetch_page(child_page_id);
		if (page != nullptr) {
			BTreePageHeader* header = reinterpret_cast<BTreePageHeader*>(page->data);
			header->parent_id = new_parent_id;
			bpm_->unpin_page(child_page_id, true); // 标记为脏页并释放
		}
	}

	void InsertIntoInternal(InternalNode* node, int key, int new_page_id) {
		int i = node->header.size;
		// 从后往前挪动，为新 Key 腾出位置
		while (i > 0 && node->array[i - 1].key > key) {
			node->array[i] = node->array[i - 1];
			i--;
		}
		node->array[i].key = key;
		node->array[i].child_page_id = new_page_id;
		node->header.size++;
	}

	void SplitInternal(Page* old_raw_page, int key, int new_page_id) {
		int new_internal_id;
		Page* new_raw_page = bpm_->new_page(&new_internal_id);

		InternalNode* old_node = reinterpret_cast<InternalNode*>(old_raw_page->data);
		InternalNode* new_node = reinterpret_cast<InternalNode*>(new_raw_page->data);

		new_node->header.type = PageType::INTERNAL;
		new_node->header.parent_id = old_node->header.parent_id;

		// 逻辑：拷贝一半数据，并递归调用 InsertIntoParent
		// ... (此处逻辑与 Leaf 分裂类似，但需处理 child_id 的父指针更新)

		bpm_->unpin_page(new_internal_id, true);
	}

	void split_leaf(Page* old_page) {
		int new_page_id;
		// 从缓冲池申请一个新页面
		Page* new_raw_page = bpm_->new_page(&new_page_id);
		LeafNode* old_leaf = reinterpret_cast<LeafNode*>(old_page->data);
		LeafNode* new_leaf = reinterpret_cast<LeafNode*>(new_raw_page->data);

		// 初始化新叶子节点
		new_leaf->header.type = PageType::LEAF;
		new_leaf->header.parent_id = old_leaf->header.parent_id;

		// 迁移数据
		int split_point = old_leaf->header.size / 2;
		int move_count = old_leaf->header.size - split_point;

		for (int i = 0; i < move_count; ++i) {
			new_leaf->data[i] = old_leaf->data[split_point + i];
		}

		new_leaf->header.size = move_count;
		old_leaf->header.size = split_point;

		// 维护横向指针
		new_leaf->next_page_id = old_leaf->next_page_id;
		old_leaf->next_page_id = new_page_id;

		// 将新页面第一个key向上推送到父节点
		InsertIntoParent(old_page->page_id, new_leaf->data[0].first, new_page_id);

		// 标记脏页并释放
		bpm_->unpin_page(old_page->page_id, true);
		bpm_->unpin_page(new_page_id, true);
	}
};