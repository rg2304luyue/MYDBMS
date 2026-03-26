#include "BPlusTree.h"
#include <climits>
#include <iostream>
#include <queue>
using namespace std;

BPlusTree::BPlusTree(BufferPoolManager* bpm) : bpm_(bpm), root_page_id_(-1) {}

void BPlusTree::init_leaf(LeafPage* leaf, int page_id, int parent_id) {
	leaf->header.type       = PageType::LEAF;
	leaf->header.size       = 0;
	leaf->header.max_size   = LeafPage::calc_max_size();
	leaf->header.parent_id  = parent_id;
	leaf->header.page_id    = page_id;
	leaf->next_page_id      = -1; // 初始化时没有兄弟
}

void BPlusTree::init_internal(InternalPage* node, int page_id, int parent_id) {
	node->header.type       = PageType::INTERNAL;
	node->header.size       = 0;
	node->header.max_size   = InternalPage::calc_max_size();
	node->header.parent_id  = parent_id;
	node->header.page_id    = page_id;
	// array[0] 的 key 不使用，child_page_id 初始化为 -1
	for (int i = 0; i < node->header.max_size; i++) {
		node->array[i].key = INT_MIN; // 或者其他哨兵值
		node->array[i].child_page_id = -1;
	}
}

void BPlusTree::update_parent_id(int child_page_id, int new_parent_id) {
	Page* page = bpm_->fetch_page(child_page_id);
	if (page == nullptr) {
		cerr << "Failed to fetch child page " << child_page_id << endl;
		return;
	}
	
	BTreePageHeader* header = reinterpret_cast<BTreePageHeader*>(page->data);
	header->parent_id = new_parent_id;
	bpm_->unpin_page(child_page_id, true); // 修改了 parent_id，标记为脏页
}

int BPlusTree::lookup_internal(InternalPage* node, int key) {
	// 二分查找找到第一个大于 key 的位置
	int left = 1; // array[0] 不使用
	int right = node->header.size; // size 是 child 的数量，比 key 的数量多 1
	while (left < right) {
		int mid = left + (right - left) / 2;
		if (node->array[mid].key > key) {
			right = mid;
		} else {
			left = mid + 1;
		}
	}
	return node->array[left - 1].child_page_id; // 返回对应 child_page_id
}

Page* BPlusTree::find_leaf(int key) {
	if (root_page_id_ == -1) {
		return nullptr; // 树为空
	}
	
	Page* page = bpm_->fetch_page(root_page_id_);
	if (page == nullptr) {
		cerr << "Failed to fetch root page " << root_page_id_ << endl;
		return nullptr;
	}
	
	while (true) {
		BTreePageHeader* header = reinterpret_cast<BTreePageHeader*>(page->data);
		if (header->type == PageType::LEAF) break;

		InternalPage* node = reinterpret_cast<InternalPage*>(page->data);
		int next_id = lookup_internal(node, key);

		bpm_->unpin_page(page->page_id, false); // 访问完当前节点，取消固定
		page = bpm_->fetch_page(next_id);
	}

	return page; // 调用方负责 unpin
}

bool BPlusTree::Search(int key, Rid* result) {
	Page* leaf_page = find_leaf(key);
	if (leaf_page == nullptr) return false; // 树为空

	LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);

	// 在叶子节点里线性查找（节点内数据量小，线性扫描够用）
	for (int i = 0; i < leaf->header.size; i++) {
		if (leaf->data[i].first == key) {
			*result = leaf->data[i].second;
			bpm_->unpin_page(leaf_page->page_id, false); // 查找完成，取消固定
			return true;
		}
	}

	bpm_->unpin_page(leaf_page->page_id, false); // 查找完成，取消固定
	return false; // 没找到
}

void BPlusTree::insert_into_leaf(LeafPage* leaf, int key, const Rid& rid) {
	// 找到插入位置（保持有序），从后往前移动腾出空间
	int i = leaf->header.size - 1;
	while (i >= 0 && leaf->data[i].first > key){
		leaf->data[i + 1] = leaf->data[i]; 
		i--;
	}
	leaf->data[i + 1] = { key, rid }; // 插入新键值对
	leaf->header.size++;
}

bool BPlusTree::Insert(int key, const Rid& rid) {
	if (root_page_id_ == -1) {
		// 树为空，创建根节点（叶子节点）
		int page_id;
		Page* raw_page = bpm_->new_page(page_id);
		if (raw_page == nullptr) {
			cerr << "Failed to create new page for root" << endl;
			return false;
		}

		LeafPage* leaf = reinterpret_cast<LeafPage*>(raw_page->data);
		init_leaf(leaf, page_id, -1); // 根节点没有父节点
		leaf->data[0] = { key, rid };
		leaf->header.size = 1;

		root_page_id_ = page_id;
		bpm_->unpin_page(page_id, true); // 修改了页内容，标记为脏页
		return true;
	}

	// 找到应该插入的叶子节点
	Page* leaf_page = find_leaf(key);
	if (leaf_page == nullptr) {
		cerr << "Failed to find leaf page for key " << key << endl;
		return false;
	}
	LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);
	
	// 重复 key 不插入（简化处理，实际 DB 会看约束）
	for (int i = 0; i < leaf->header.size; i++) {
		if (leaf->data[i].first == key) {
			bpm_->unpin_page(leaf_page->page_id, false); // 取消固定
			return false; // 已存在相同 key
		}
	}

	insert_into_leaf(leaf, key, rid); // 插入到叶子节点

	// 叶子没满, 直接脏标返回
	if (leaf->header.size < leaf->header.max_size) {
		bpm_->unpin_page(leaf_page->page_id, true); // 修改了页内容，标记为脏页
		return true;
	}

	// 叶子满了，需要分裂
	split_leaf(leaf_page);
	// 分裂后会修改当前页和父节点，所以都标记为脏页
	return true;
}

void BPlusTree::split_leaf(Page* old_page) {
	int new_page_id;
	Page* new_raw_page = bpm_->new_page(new_page_id);

	LeafPage* old_leaf = reinterpret_cast<LeafPage*>(old_page->data);
	LeafPage* new_leaf = reinterpret_cast<LeafPage*>(new_raw_page->data);
	init_leaf(new_leaf, new_page_id, old_leaf->header.parent_id);

	// 把后半段数据移到新叶子
	// 分裂点：前半留在 old，后半移到 new
	int split = old_leaf->header.size / 2;
	int move = old_leaf->header.size - split;

	for (int i = 0; i < move; i++) {
		new_leaf->data[i] = old_leaf->data[split + i];
	}
	new_leaf->header.size = move;
	old_leaf->header.size = split;

	// 维护叶子链表
	new_leaf->next_page_id = old_leaf->next_page_id;
	old_leaf->next_page_id = new_page_id;

	// 新叶子的第一个 key 需要"推上去"告诉父节点分界在哪
	int push_up_key = new_leaf->data[0].first;

	// 把分界 key 插入父节点
	insert_into_parent(old_page, push_up_key, new_raw_page);
	// insert_into_parent 内部负责 unpin 两个页
}

// ── 向父节点插入分割键 ────────────────────────────────────────

void BPlusTree::insert_into_parent(Page* old_page, int push_up_key, Page* new_page) {
	BTreePageHeader* old_header = reinterpret_cast<BTreePageHeader*>(old_page->data);
	int parent_id = old_header->parent_id;

	// 情况 1：old_page 是根节点，需要创建新根
	if (parent_id == -1) {
		int new_root_id;
		Page* root_raw = bpm_->new_page(new_root_id);
		InternalPage* root = reinterpret_cast<InternalPage*>(root_raw->data);
		init_internal(root, new_root_id, -1);

		// 新根有两个 child：old 和 new，一个分割键 push_up_key
		// array[0]: 无 key，child = old
		// array[1]: key = push_up_key，child = new
		root->array[0].child_page_id = old_page->page_id;
		root->array[1].key = push_up_key;
		root->array[1].child_page_id = new_page->page_id;
		root->header.size = 2;  // 2 个 child

		update_parent_id(old_page->page_id, new_root_id);
		update_parent_id(new_page->page_id, new_root_id);

		root_page_id_ = new_root_id;

		bpm_->unpin_page(new_root_id, true);
		bpm_->unpin_page(old_page->page_id, true);
		bpm_->unpin_page(new_page->page_id, true);
		return;
	}

	// 情况 2：已有父节点，把 push_up_key 插入父节点
	bpm_->unpin_page(old_page->page_id, true);
	bpm_->unpin_page(new_page->page_id, true);

	Page* parent_raw = bpm_->fetch_page(parent_id);
	InternalPage* parent = reinterpret_cast<InternalPage*>(parent_raw->data);

	if (parent->header.size < parent->header.max_size) {
		// 父节点没满，直接插入
		insert_into_internal(parent, push_up_key, new_page->page_id);
		bpm_->unpin_page(parent_id, true);
	}
	else {
		// 父节点也满了，内部节点分裂（递归向上）
		split_internal(parent_raw, push_up_key, new_page->page_id);
		// split_internal 内部负责 unpin parent_raw
	}
}

void BPlusTree::insert_into_internal(InternalPage* node, int key, int new_child_id) {
	// 从后往前移动，给新 key 腾位置（保持有序）
	int i = node->header.size;
	while (i > 1 && node->array[i - 1].key > key) {
		node->array[i] = node->array[i - 1];
		i--;
	}
	node->array[i].key = key;
	node->array[i].child_page_id = new_child_id;
	node->header.size++;
}

// ── 内部节点分裂 ──────────────────────────────────────────────

void BPlusTree::split_internal(Page* old_page, int key, int new_child_id) {
	// 先把新 key 插进来（超出 max_size 一个位置，临时用）
	InternalPage* old_node = reinterpret_cast<InternalPage*>(old_page->data);
	insert_into_internal(old_node, key, new_child_id);

	// 从中间分裂：中间的 key 被"推上去"，不留在任何一侧
	//
	// 分裂前（size = max_size + 1）：
	// [P0 | K1 | P1 | K2 | P2 | K3 | P3 | K4 | P4]
	//                          ↑ mid
	// 分裂后：
	// old: [P0 | K1 | P1 | K2 | P2]   push_up: K3   new: [P3 | K4 | P4]

	int total = old_node->header.size;  // max_size + 1
	int mid = total / 2;              // 中间位置，这个 key 要推上去
	int push_up_key = old_node->array[mid].key;

	int new_page_id;
	Page* new_raw = bpm_->new_page(new_page_id);
	InternalPage* new_node = reinterpret_cast<InternalPage*>(new_raw->data);
	init_internal(new_node, new_page_id, old_node->header.parent_id);

	// 把 mid 右侧的数据移到新节点
	// new_node->array[0] 的 child 是 old_node->array[mid] 的 child
	new_node->array[0].child_page_id = old_node->array[mid].child_page_id;
	int move = total - mid - 1;
	for (int i = 0; i < move; i++) {
		new_node->array[i + 1] = old_node->array[mid + 1 + i];
	}
	new_node->header.size = move + 1;  // +1 是因为 array[0] 也算一个 child
	old_node->header.size = mid;       // old 只保留 mid 个 child

	// 更新被移走的 child 的 parent_id
	for (int i = 0; i < new_node->header.size; i++) {
		update_parent_id(new_node->array[i].child_page_id, new_page_id);
	}

	// 继续向上插入
	insert_into_parent(old_page, push_up_key, new_raw);
}

// ── 调试打印 ──────────────────────────────────────────────────

// ── 打印（层序遍历）──────────────────────────────────────────
//
// 输出格式示例（插入 1~12 后）：
//
// ========== B+ Tree (root=3) ==========
// [Layer 0]
//   [Internal page=3]  size=2  keys: 7
//
// [Layer 1]
//   [Internal page=1]  size=2  keys: 4
//   [Internal page=2]  size=2  keys: 10
//
// [Layer 2]
//   [Internal page=...]  ...
//
// [Leaf chain →]
//   [Leaf page=0]  size=3  keys: 1 2 3  → next=4
//   [Leaf page=4]  size=3  keys: 4 5 6  → next=5
//   ...
// =========================================
void BPlusTree::Print() {
	if (root_page_id_ == -1) {
		cout << "[空树]\n";
		return;
	}

	cout << "\n========== B+ Tree (root=" << root_page_id_ << ") ==========\n";

	// ── 第一步：BFS 遍历所有节点，按层打印内部节点 ──────────
	// queue 存 {page_id, layer}
	queue<pair<int, int>> bfs;
	bfs.push({ root_page_id_, 0 });

	int cur_layer = -1;
	int first_leaf = -1;   // BFS 遇到的第一个叶子，用于后续链表遍历

	while (!bfs.empty()) {
		auto [page_id, layer] = bfs.front();
		bfs.pop();

		Page* raw = bpm_->fetch_page(page_id);
		if (!raw) continue;

		BTreePageHeader* hdr = reinterpret_cast<BTreePageHeader*>(raw->data);

		// 换层时打印层标题
		if (layer != cur_layer) {
			cur_layer = layer;
			cout << "\n[Layer " << layer << "]\n";
		}

		if (hdr->type == PageType::INTERNAL) {
			InternalPage* node = reinterpret_cast<InternalPage*>(raw->data);

			cout << "  [Internal page=" << page_id << "]"
				<< "  size=" << node->header.size
				<< "  keys: ";
			for (int i = 1; i < node->header.size; i++)
				cout << node->array[i].key << " ";
			cout << "\n";

			// 收集子节点后再 unpin，防止缓冲池满
			vector<int> children;
			for (int i = 0; i < node->header.size; i++)
				children.push_back(node->array[i].child_page_id);

			bpm_->unpin_page(page_id, false);

			for (int cid : children)
				bfs.push({ cid, layer + 1 });

		}
		else {
			// 叶子节点只记录第一个，链表在下面统一打印
			if (first_leaf == -1) first_leaf = page_id;
			bpm_->unpin_page(page_id, false);
		}
	}

	// ── 第二步：沿叶子链表横向打印所有叶子 ──────────────────
	// 叶子之间有 next_page_id 指针串成单向链表，
	// 这正是 B+ 树支持高效范围查询的原因
	cout << "\n[Leaf chain →]\n";

	int cur = first_leaf;
	int leaf_count = 0;
	while (cur != -1) {
		Page* raw = bpm_->fetch_page(cur);
		if (!raw) break;
		LeafPage* leaf = reinterpret_cast<LeafPage*>(raw->data);

		cout << "  [Leaf page=" << cur << "]"
			<< "  size=" << leaf->header.size
			<< "  keys: ";
		for (int i = 0; i < leaf->header.size; i++)
			cout << leaf->data[i].first << " ";

		int nxt = leaf->next_page_id;
		cout << " -> " << (nxt == -1 ? "END" : "page=" + to_string(nxt)) << "\n";

		bpm_->unpin_page(cur, false);
		cur = nxt;
		leaf_count++;
	}

	cout << "  共 " << leaf_count << " 个叶子节点\n";
	cout << "===========================================\n";
}