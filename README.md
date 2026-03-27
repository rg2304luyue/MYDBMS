MYDBMS 核心组件文档
本篇文档详细介绍了 MYDBMS 数据库管理系统的底层核心类、结构定义及其方法实现逻辑。

1. 存储层 (Storage Layer)
DiskManager 类
负责管理数据库文件在磁盘上的物理 I/O 操作。

DiskManager(const string& db_file): 构造函数，初始化数据库文件名并打开文件流。

write_page(int page_id, const char* data): 将 4KB 的二进制数据写入磁盘指定的 page_id 偏移位置。

read_page(int page_id, char* data): 从磁盘指定的 page_id 位置读取 4KB 数据到内存缓冲区。

get_total_pages(): 返回当前数据库文件包含的总页数，常用于分配新的 page_id。

2. 内存管理层 (Memory Management Layer)
LRUReplacer 类
实现 LRU（最近最少使用）页面置换算法，管理可被剔出的页面槽位。

Victim(int* frame_id): 选出一个最久未使用的 frame_id 进行置换，若无可用则返回 false。

Pin(int frame_id): 当页面被使用时，将其从 LRU 候选项列表中移除，确保其不会被置换。

Unpin(int frame_id): 当页面引用计数清零时，将其加入 LRU 列表，使其成为可置换候选。

Size(): 返回当前 LRU 列表中候选项的数量。

BufferPoolManager 类
数据库的中枢，负责协调内存与磁盘之间的数据交换。

fetch_page(int page_id): 从缓冲池获取页面。若页面不在内存，则触发磁盘读取并根据 LRU 策略进行置换。

unpin_page(int page_id, bool is_dirty): 减少页面的引用计数。若 is_dirty 为 true，则标记该页为脏页。

new_page(int& page_id): 在缓冲池和磁盘中申请一个全新的页面。

flush_page(int page_id): 强制将内存中的某个脏页立即写回磁盘。

3. 索引层 (Indexing Layer)
B+ 树节点结构 (Internal & Leaf)
通过强转物理页数据实现的高效索引布局。

BTreePageHeader: 公共头部，包含页面类型（叶子/内部）、大小、最大容量及父节点 ID。

LeafPage: 叶子节点，存储实际的 pair<int, Rid> 数据，并通过 next_page_id 形成双向链表。

InternalPage: 内部节点，存储导航键（Key）和子节点的 page_id。

BPlusTree 类
基于缓冲池实现的持久化 B+ 树索引。

Insert(int key, const Rid& rid): 向索引中插入新的键值对，并在必要时触发节点分裂。

Search(int key, Rid* result): 查找指定 Key 对应的记录 ID（Rid）。

split_leaf(Page* leaf_page): 当叶子节点满时，将其分裂为两个，并将分界键推送到父节点。

split_internal(Page* internal_page, int key, int new_child_id): 递归处理内部节点的分裂逻辑，并更新子节点的父节点 ID。

find_leaf(int key): 内部私有方法，从根节点开始导航，返回存储该 Key 的目标叶子节点页指针。

Print(): 以层序遍历方式打印整棵树的结构，用于调试索引的平衡性。

4. 辅助结构
Page (struct): 内存页实体，包含 4096 字节的数据缓冲区、脏位标记及 pin_count 引用计数。

Rid (struct): 记录 ID，由 page_id 和 slot_num 组成，精确定位数据在磁盘上的物理位置。
