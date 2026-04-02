#include "BPlusTree.h"
#include <climits>
#include <iostream>
#include <queue>
#include <shared_mutex>
using namespace std;

BPlusTree::BPlusTree(BufferPoolManager* bpm) : bpm_(bpm), root_page_id_(-1) {}

// ── 释放祖先锁链 ─────────────────────────────────────────────
void BPlusTree::release_ancestors(vector<Page*>& ancestors) {
    for (Page* p : ancestors) {
        p->page_latch.unlock();           // 释放 X-lock
        bpm_->unpin_page(p->page_id, false);
    }
    ancestors.clear();
}

// ── 读路径：Crabbing S-lock ──────────────────────────────────
Page* BPlusTree::find_leaf_read(int key) {
    root_latch_.lock_shared();
    if (root_page_id_ == -1) {
        root_latch_.unlock_shared();
        return nullptr;
    }

    Page* page = bpm_->fetch_page(root_page_id_);
    page->page_latch.lock_shared();    // S-lock 根节点
    root_latch_.unlock_shared();       // root_latch_ 可以释放了

    while (true) {
        BTreePageHeader* hdr = reinterpret_cast<BTreePageHeader*>(page->data);
        if (hdr->type == PageType::LEAF) break;

        InternalPage* node = reinterpret_cast<InternalPage*>(page->data);
        int next_id = lookup_internal(node, key);

        Page* child = bpm_->fetch_page(next_id);
        child->page_latch.lock_shared();   // 先拿子节点 S-lock
        page->page_latch.unlock_shared();  // 再释放父节点 S-lock
        bpm_->unpin_page(page->page_id, false);

        page = child;
    }
    return page;  // 调用方持有叶子的 S-lock，负责 unlock + unpin
}

// ── 写路径：Crabbing X-lock ──────────────────────────────────
Page* BPlusTree::find_leaf_write(int key, vector<Page*>& ancestors) {
    root_latch_.lock();                // 独占根锁（保护 root_page_id_）
    if (root_page_id_ == -1) {
        root_latch_.unlock();
        return nullptr;
    }

    Page* page = bpm_->fetch_page(root_page_id_);
    page->page_latch.lock();           // X-lock 根节点
    root_latch_.unlock();              // root_page_id_ 已读到，可释放

    BTreePageHeader* hdr = reinterpret_cast<BTreePageHeader*>(page->data);

    // 根节点：Insert 时判断是否安全
    bool safe = (hdr->type == PageType::LEAF)
        ? is_leaf_safe_insert(reinterpret_cast<LeafPage*>(page->data))
        : is_internal_safe_insert(reinterpret_cast<InternalPage*>(page->data));

    if (safe) {
        // 根节点安全，不需要保留祖先链
    }
    else {
        ancestors.push_back(page);     // 可能需要分裂，先保留
    }

    while (hdr->type != PageType::LEAF) {
        InternalPage* node = reinterpret_cast<InternalPage*>(page->data);
        int next_id = lookup_internal(node, key);

        Page* child = bpm_->fetch_page(next_id);
        child->page_latch.lock();      // X-lock 子节点

        BTreePageHeader* child_hdr = reinterpret_cast<BTreePageHeader*>(child->data);
        bool child_safe = (child_hdr->type == PageType::LEAF)
            ? is_leaf_safe_insert(reinterpret_cast<LeafPage*>(child->data))
            : is_internal_safe_insert(reinterpret_cast<InternalPage*>(child->data));

        if (child_safe) {
            // 子节点安全 → 释放所有祖先的锁
            release_ancestors(ancestors);
            // page 不在 ancestors 里时单独释放
            if (ancestors.empty()) {
                // page 已经被 release_ancestors 处理了
            }
        }
        else {
            ancestors.push_back(child);
        }

        // 如果当前 page 不在 ancestors 里，需要在这里释放
        bool page_in_ancestors = false;
        for (auto* a : ancestors) if (a == page) { page_in_ancestors = true; break; }
        if (!page_in_ancestors) {
            page->page_latch.unlock();
            bpm_->unpin_page(page->page_id, false);
        }

        page = child;
        hdr = child_hdr;
    }

    return page;  // 调用方持有叶子 X-lock；ancestors 中的页也持有 X-lock
}

bool BPlusTree::Search(int key, Rid* result) {
    Page* leaf_page = find_leaf_read(key);
    if (!leaf_page) return false;

    LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);
    bool found = false;
    for (int i = 0; i < leaf->header.size; i++) {
        if (leaf->data[i].first == key) {
            *result = leaf->data[i].second;
            found = true;
            break;
        }
    }

    leaf_page->page_latch.unlock_shared();
    bpm_->unpin_page(leaf_page->page_id, false);
    return found;
}

bool BPlusTree::Insert(int key, const Rid& rid) {
    // 特殊情况：树为空，需要独占 root_latch_
    {
        unique_lock<shared_mutex> root_guard(root_latch_);
        if (root_page_id_ == -1) {
            int page_id;
            Page* raw = bpm_->new_page(page_id);
            if (!raw) return false;
            LeafPage* leaf = reinterpret_cast<LeafPage*>(raw->data);
            init_leaf(leaf, page_id, -1);
            leaf->data[0] = { key, rid };
            leaf->header.size = 1;
            root_page_id_ = page_id;
            bpm_->unpin_page(page_id, true);
            return true;
        }
    }

    vector<Page*> ancestors;
    Page* leaf_page = find_leaf_write(key, ancestors);
    if (!leaf_page) return false;

    LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);

    // 重复 key 检查
    for (int i = 0; i < leaf->header.size; i++) {
        if (leaf->data[i].first == key) {
            leaf_page->page_latch.unlock();
            bpm_->unpin_page(leaf_page->page_id, false);
            release_ancestors(ancestors);
            return false;
        }
    }

    insert_into_leaf(leaf, key, rid);

    if (leaf->header.size < leaf->header.max_size) {
        // 不需要分裂，释放所有锁
        leaf_page->page_latch.unlock();
        bpm_->unpin_page(leaf_page->page_id, true);
        release_ancestors(ancestors);
        return true;
    }

    // 需要分裂：ancestors 中的锁已持有，split_leaf/insert_into_parent 会处理
    // 注意：分裂逻辑内部会调用 bpm_->unpin_page，
    // 但不会释放 page_latch，所以我们在这里先统一释放 ancestors 的锁
    // 然后把分裂工作交给原有的（无锁版本）函数处理
    // 在实际生产代码中，分裂路径也应该持有锁直到完成，
    // 这里用粗粒度 fallback：直接持有所有祖先锁，分裂完后统一释放
    split_leaf(leaf_page);          // 内部会 unpin leaf_page 和新页
    // ancestors 中的页已在 split 路径中被修改，现在释放它们的锁
    for (Page* a : ancestors) {
        a->page_latch.unlock();
        // 注意：unpin 已在 insert_into_parent / split_internal 中完成
    }
    ancestors.clear();
    return true;
}

bool BPlusTree::Delete(int key) {
    vector<Page*> ancestors;
    Page* leaf_page = find_leaf_write(key, ancestors);
    if (!leaf_page) return false;

    LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);

    bool found = false;
    for (int i = 0; i < leaf->header.size; i++) {
        if (leaf->data[i].first == key) { found = true; break; }
    }
    if (!found) {
        leaf_page->page_latch.unlock();
        bpm_->unpin_page(leaf_page->page_id, false);
        release_ancestors(ancestors);
        return false;
    }

    delete_from_leaf(leaf, key);

    int min_size = leaf->header.max_size / 2;
    int parent_id = leaf->header.parent_id;

    if (leaf->header.size >= min_size || parent_id == -1) {
        leaf_page->page_latch.unlock();
        bpm_->unpin_page(leaf_page->page_id, true);
        release_ancestors(ancestors);
        return true;
    }

    // 需要修复下溢，释放 ancestor 锁（fix_leaf_underflow 会自己 fetch/unpin）
    for (Page* a : ancestors) {
        a->page_latch.unlock();
    }
    ancestors.clear();

    Page* parent_raw = bpm_->fetch_page(parent_id);
    InternalPage* parent = reinterpret_cast<InternalPage*>(parent_raw->data);
    int idx = find_child_index(parent, leaf_page->page_id);
    bpm_->unpin_page(parent_id, false);

    // 释放叶子的 X-lock 再让 fix 函数重新 fetch（避免死锁）
    leaf_page->page_latch.unlock();
    fix_leaf_underflow(leaf_page, parent_id, idx);
    return true;
}

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

bool BPlusTree::Search_Impl(int key, Rid* result) {
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

bool BPlusTree::Insert_Impl(int key, const Rid& rid) {
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

// ── 从叶子中删除指定 key ─────────────────────────────────────
void BPlusTree::delete_from_leaf(LeafPage* leaf, int key) {
    int i = 0;
    // 找到 key 的位置
    while (i < leaf->header.size && leaf->data[i].first != key) i++;
    // 把后面的元素前移（覆盖掉要删的位置）
    for (int j = i; j < leaf->header.size - 1; j++) {
        leaf->data[j] = leaf->data[j + 1];
    }
    leaf->header.size--;
}

// ── 在父节点中找某个 child_page_id 对应的 array 下标 ─────────
int BPlusTree::find_child_index(InternalPage* parent, int child_page_id) {
    for (int i = 0; i < parent->header.size; i++) {
        if (parent->array[i].child_page_id == child_page_id) return i;
    }
    return -1; // 不应该走到这里
}

// ── 从内部节点中删除第 index 个 entry ──────────────────────
// index 表示 array[index]，其 child_page_id 对应被删掉的子节点
void BPlusTree::delete_from_internal(InternalPage* node, int index) {
    // index == 0 是特殊情况：删掉最左 child，
    // 需要把 array[1].key 提上来作为新的"无 key"的 array[0]
    if (index == 0) {
        // 把 array[1] 的 child 挪到 array[0]，然后从 array[1] 开始前移
        node->array[0].child_page_id = node->array[1].child_page_id;
        for (int i = 1; i < node->header.size - 1; i++) {
            node->array[i] = node->array[i + 1];
        }
    }
    else {
        // 删除 array[index]，把后面的前移
        for (int i = index; i < node->header.size - 1; i++) {
            node->array[i] = node->array[i + 1];
        }
    }
    node->header.size--;
}

// ── 叶子下溢处理 ──────────────────────────────────────────────
// leaf_page：发生下溢的叶子 Page*（已 fetch，调用方负责最终 unpin）
// parent_id：父节点 page_id
// index_in_parent：leaf 在父节点 array[] 中的下标
void BPlusTree::fix_leaf_underflow(Page* leaf_page, int parent_id, int index_in_parent) {
    LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);
    int min_size = (leaf->header.max_size) / 2;

    // ── 情况 0：这个叶子本身就是根节点（树只剩一个叶子），不需要处理 ──
    if (parent_id == -1) {
        bpm_->unpin_page(leaf_page->page_id, true);
        return;
    }

    Page* parent_raw = bpm_->fetch_page(parent_id);
    InternalPage* parent = reinterpret_cast<InternalPage*>(parent_raw->data);

    // ── 尝试从左兄弟借 ──
    if (index_in_parent > 0) {
        int left_sib_id = parent->array[index_in_parent - 1].child_page_id;
        Page* left_raw = bpm_->fetch_page(left_sib_id);
        LeafPage* left_sib = reinterpret_cast<LeafPage*>(left_raw->data);

        if (left_sib->header.size > min_size) {
            // 左兄弟够富裕：把左兄弟最后一个 key 移到当前叶子头部
            // 1. 当前叶子腾出位置
            for (int i = leaf->header.size; i > 0; i--)
                leaf->data[i] = leaf->data[i - 1];
            // 2. 把左兄弟的最后一个 entry 移过来
            leaf->data[0] = left_sib->data[left_sib->header.size - 1];
            leaf->header.size++;
            left_sib->header.size--;
            // 3. 更新父节点中的分界 key（index_in_parent 处的 key）
            parent->array[index_in_parent].key = leaf->data[0].first;

            bpm_->unpin_page(left_sib_id, true);
            bpm_->unpin_page(leaf_page->page_id, true);
            bpm_->unpin_page(parent_id, true);
            return;
        }
        bpm_->unpin_page(left_sib_id, false);
    }

    // ── 尝试从右兄弟借 ──
    if (index_in_parent < parent->header.size - 1) {
        int right_sib_id = parent->array[index_in_parent + 1].child_page_id;
        Page* right_raw = bpm_->fetch_page(right_sib_id);
        LeafPage* right_sib = reinterpret_cast<LeafPage*>(right_raw->data);

        if (right_sib->header.size > min_size) {
            // 右兄弟够富裕：把右兄弟第一个 key 移到当前叶子尾部
            leaf->data[leaf->header.size] = right_sib->data[0];
            leaf->header.size++;
            // 右兄弟前移
            for (int i = 0; i < right_sib->header.size - 1; i++)
                right_sib->data[i] = right_sib->data[i + 1];
            right_sib->header.size--;
            // 更新父节点分界 key（右兄弟现在的第一个 key）
            parent->array[index_in_parent + 1].key = right_sib->data[0].first;

            bpm_->unpin_page(right_sib_id, true);
            bpm_->unpin_page(leaf_page->page_id, true);
            bpm_->unpin_page(parent_id, true);
            return;
        }
        bpm_->unpin_page(right_sib_id, false);
    }

    // ── 无法借，必须合并 ──
    // 统一策略：把当前叶子合并到左兄弟（若无左兄弟，则把右兄弟合并进来）
    if (index_in_parent > 0) {
        // 有左兄弟：把当前叶子的数据全部追加到左兄弟
        int left_sib_id = parent->array[index_in_parent - 1].child_page_id;
        Page* left_raw = bpm_->fetch_page(left_sib_id);
        LeafPage* left_sib = reinterpret_cast<LeafPage*>(left_raw->data);

        for (int i = 0; i < leaf->header.size; i++)
            left_sib->data[left_sib->header.size + i] = leaf->data[i];
        left_sib->header.size += leaf->header.size;
        left_sib->next_page_id = leaf->next_page_id; // 维护链表

        bpm_->unpin_page(left_sib_id, true);
        bpm_->unpin_page(leaf_page->page_id, true);

        // 从父节点删掉指向当前叶子的 entry（即 index_in_parent）
        int grandparent_id = parent->header.parent_id;
        int parent_index = find_child_index(parent, leaf_page->page_id);
        // 这里 index_in_parent == parent_index
        delete_from_internal(parent, index_in_parent);

        // 检查父节点是否也下溢
        if (parent->header.size < (parent->header.max_size / 2)
            && grandparent_id != -1) {
            // 注意：这里不能先 unpin parent，fix_internal_underflow 需要用它
            // 所以把 unpin 交给 fix_internal_underflow 内部处理
            int parent_idx_in_grand = -1;
            if (grandparent_id != -1) {
                Page* grand_raw = bpm_->fetch_page(grandparent_id);
                InternalPage* grand = reinterpret_cast<InternalPage*>(grand_raw->data);
                parent_idx_in_grand = find_child_index(grand, parent_id);
                bpm_->unpin_page(grandparent_id, false);
            }
            fix_internal_underflow(parent_raw, grandparent_id, parent_idx_in_grand);
        }
        else {
            bpm_->unpin_page(parent_id, true);
        }

    }
    else {
        // 没有左兄弟：把右兄弟合并进当前叶子
        int right_sib_id = parent->array[index_in_parent + 1].child_page_id;
        Page* right_raw = bpm_->fetch_page(right_sib_id);
        LeafPage* right_sib = reinterpret_cast<LeafPage*>(right_raw->data);

        for (int i = 0; i < right_sib->header.size; i++)
            leaf->data[leaf->header.size + i] = right_sib->data[i];
        leaf->header.size += right_sib->header.size;
        leaf->next_page_id = right_sib->next_page_id;

        bpm_->unpin_page(right_sib_id, true);
        bpm_->unpin_page(leaf_page->page_id, true);

        int grandparent_id = parent->header.parent_id;
        delete_from_internal(parent, index_in_parent + 1);

        if (parent->header.size < (parent->header.max_size / 2)
            && grandparent_id != -1) {
            int parent_idx_in_grand = -1;
            Page* grand_raw = bpm_->fetch_page(grandparent_id);
            InternalPage* grand = reinterpret_cast<InternalPage*>(grand_raw->data);
            parent_idx_in_grand = find_child_index(grand, parent_id);
            bpm_->unpin_page(grandparent_id, false);
            fix_internal_underflow(parent_raw, grandparent_id, parent_idx_in_grand);
        }
        else {
            bpm_->unpin_page(parent_id, true);
        }
    }
}

// ── 内部节点下溢处理 ────────────────────────────────────────
void BPlusTree::fix_internal_underflow(Page* node_page, int parent_id, int index_in_parent) {
    InternalPage* node = reinterpret_cast<InternalPage*>(node_page->data);
    int min_size = node->header.max_size / 2;

    // 如果是根节点
    if (parent_id == -1) {
        // 根节点只剩 1 个 child 时，把那个 child 提升为新根
        if (node->header.size == 1) {
            int new_root_id = node->array[0].child_page_id;
            root_page_id_ = new_root_id;
            update_parent_id(new_root_id, -1);
        }
        bpm_->unpin_page(node_page->page_id, true);
        return;
    }

    Page* parent_raw = bpm_->fetch_page(parent_id);
    InternalPage* parent = reinterpret_cast<InternalPage*>(parent_raw->data);

    // ── 从左兄弟借 ──
    if (index_in_parent > 0) {
        int left_id = parent->array[index_in_parent - 1].child_page_id;
        Page* left_raw = bpm_->fetch_page(left_id);
        InternalPage* left_sib = reinterpret_cast<InternalPage*>(left_raw->data);

        if (left_sib->header.size > min_size) {
            // 父节点把分界 key 下放到 node 头部，左兄弟最右 child 挪过来
            // 1. node 腾位置
            for (int i = node->header.size; i > 0; i--)
                node->array[i] = node->array[i - 1];
            // 2. 父节点的分界 key 下放（index_in_parent 处的 key）
            node->array[1].key = parent->array[index_in_parent].key;
            node->array[0].child_page_id = left_sib->array[left_sib->header.size - 1].child_page_id;
            node->header.size++;
            // 3. 左兄弟最右的 key 上升到父节点
            parent->array[index_in_parent].key = left_sib->array[left_sib->header.size - 1].key;
            left_sib->header.size--;
            // 4. 更新被移走的 child 的 parent_id
            update_parent_id(node->array[0].child_page_id, node_page->page_id);

            bpm_->unpin_page(left_id, true);
            bpm_->unpin_page(node_page->page_id, true);
            bpm_->unpin_page(parent_id, true);
            return;
        }
        bpm_->unpin_page(left_id, false);
    }

    // ── 从右兄弟借 ──
    if (index_in_parent < parent->header.size - 1) {
        int right_id = parent->array[index_in_parent + 1].child_page_id;
        Page* right_raw = bpm_->fetch_page(right_id);
        InternalPage* right_sib = reinterpret_cast<InternalPage*>(right_raw->data);

        if (right_sib->header.size > min_size) {
            // 父节点分界 key 下放到 node 尾部，右兄弟最左 child 挪过来
            int n = node->header.size;
            node->array[n].key = parent->array[index_in_parent + 1].key;
            node->array[n].child_page_id = right_sib->array[0].child_page_id;
            node->header.size++;
            // 右兄弟最左的 key 上升到父节点
            parent->array[index_in_parent + 1].key = right_sib->array[1].key;
            // 右兄弟前移
            right_sib->array[0].child_page_id = right_sib->array[1].child_page_id;
            for (int i = 1; i < right_sib->header.size - 1; i++)
                right_sib->array[i] = right_sib->array[i + 1];
            right_sib->header.size--;

            update_parent_id(node->array[n].child_page_id, node_page->page_id);

            bpm_->unpin_page(right_id, true);
            bpm_->unpin_page(node_page->page_id, true);
            bpm_->unpin_page(parent_id, true);
            return;
        }
        bpm_->unpin_page(right_id, false);
    }

    // ── 合并内部节点 ──
    if (index_in_parent > 0) {
        // 把 node 合并到左兄弟
        int left_id = parent->array[index_in_parent - 1].child_page_id;
        Page* left_raw = bpm_->fetch_page(left_id);
        InternalPage* left_sib = reinterpret_cast<InternalPage*>(left_raw->data);

        // 父节点的分界 key 下放到 left_sib 作为衔接 key
        int sink_key = parent->array[index_in_parent].key;
        int n = left_sib->header.size;
        left_sib->array[n].key = sink_key;
        left_sib->array[n].child_page_id = node->array[0].child_page_id;
        left_sib->header.size++;

        for (int i = 1; i < node->header.size; i++) {
            left_sib->array[left_sib->header.size] = node->array[i];
            left_sib->header.size++;
        }

        // 更新被移走的所有 child 的 parent_id
        for (int i = 0; i < node->header.size; i++)
            update_parent_id(node->array[i].child_page_id, left_id);

        bpm_->unpin_page(left_id, true);
        bpm_->unpin_page(node_page->page_id, true);

        int grandparent_id = parent->header.parent_id;
        delete_from_internal(parent, index_in_parent);

        if (parent->header.size < min_size && grandparent_id != -1) {
            int parent_idx = -1;
            Page* grand_raw = bpm_->fetch_page(grandparent_id);
            InternalPage* grand = reinterpret_cast<InternalPage*>(grand_raw->data);
            parent_idx = find_child_index(grand, parent_id);
            bpm_->unpin_page(grandparent_id, false);
            fix_internal_underflow(parent_raw, grandparent_id, parent_idx);
        }
        else {
            bpm_->unpin_page(parent_id, true);
        }
    }
    else {
        // 把右兄弟合并进 node
        int right_id = parent->array[index_in_parent + 1].child_page_id;
        Page* right_raw = bpm_->fetch_page(right_id);
        InternalPage* right_sib = reinterpret_cast<InternalPage*>(right_raw->data);

        int sink_key = parent->array[index_in_parent + 1].key;
        int n = node->header.size;
        node->array[n].key = sink_key;
        node->array[n].child_page_id = right_sib->array[0].child_page_id;
        node->header.size++;

        for (int i = 1; i < right_sib->header.size; i++) {
            node->array[node->header.size] = right_sib->array[i];
            node->header.size++;
        }

        for (int i = 0; i < right_sib->header.size; i++)
            update_parent_id(right_sib->array[i].child_page_id, node_page->page_id);

        bpm_->unpin_page(right_id, true);
        bpm_->unpin_page(node_page->page_id, true);

        int grandparent_id = parent->header.parent_id;
        delete_from_internal(parent, index_in_parent + 1);

        if (parent->header.size < min_size && grandparent_id != -1) {
            int parent_idx = -1;
            Page* grand_raw = bpm_->fetch_page(grandparent_id);
            InternalPage* grand = reinterpret_cast<InternalPage*>(grand_raw->data);
            parent_idx = find_child_index(grand, parent_id);
            bpm_->unpin_page(grandparent_id, false);
            fix_internal_underflow(parent_raw, grandparent_id, parent_idx);
        }
        else {
            bpm_->unpin_page(parent_id, true);
        }
    }
}

// ── 对外接口：Delete ─────────────────────────────────────────
bool BPlusTree::Delete_Impl(int key) {
    Page* leaf_page = find_leaf(key);
    if (leaf_page == nullptr) return false;

    LeafPage* leaf = reinterpret_cast<LeafPage*>(leaf_page->data);

    // 检查 key 是否存在
    bool found = false;
    for (int i = 0; i < leaf->header.size; i++) {
        if (leaf->data[i].first == key) { found = true; break; }
    }
    if (!found) {
        bpm_->unpin_page(leaf_page->page_id, false);
        return false;
    }

    delete_from_leaf(leaf, key);

    int parent_id = leaf->header.parent_id;
    int min_size = leaf->header.max_size / 2;

    // 没有下溢，或者本身就是根节点
    if (leaf->header.size >= min_size || parent_id == -1) {
        bpm_->unpin_page(leaf_page->page_id, true);
        return true;
    }

    // 需要修复下溢
    // 先找到自己在父节点中的 index
    Page* parent_raw = bpm_->fetch_page(parent_id);
    InternalPage* parent = reinterpret_cast<InternalPage*>(parent_raw->data);
    int idx = find_child_index(parent, leaf_page->page_id);
    bpm_->unpin_page(parent_id, false); // fix_leaf_underflow 会重新 fetch parent

    fix_leaf_underflow(leaf_page, parent_id, idx);
    return true;
}