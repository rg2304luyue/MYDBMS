## 1.  存储层 (Storage Layer)

### `DiskManager`

负责数据库文件在磁盘上的物理 I/O 操作，是系统与持久化存储之间的桥梁。

#### 📌 构造函数

```
DiskManager(const string& db_file);
```

- 初始化数据库文件路径
- 打开文件流，准备进行读写操作

------

#### 📌 核心方法

```
void write_page(int page_id, const char* data);
```

- 将 **4KB 页面数据**写入磁盘
- 写入位置 = `page_id * PAGE_SIZE`

```
void read_page(int page_id, char* data);
```

- 从磁盘读取指定页面数据到内存缓冲区
- 若页面不存在，通常返回空数据或初始化页

```
int get_total_pages();
```

- 获取当前数据库文件中的总页数
- 常用于：
  - 分配新页面 ID
  - 判断文件增长情况

------

## 2.  内存管理层 (Memory Management Layer)

------

### `LRUReplacer`

实现 **LRU（Least Recently Used）页面替换策略**，用于选择可被淘汰的缓存页。

#### 📌 核心方法

```
bool Victim(int* frame_id);
```

- 选择最久未使用的页面
- 返回：
  - `true`：成功找到可替换页面
  - `false`：没有可替换页面

```
void Pin(int frame_id);
```

- 将页面从 LRU 列表中移除
- 表示该页面正在被使用，不能被淘汰

```
void Unpin(int frame_id);
```

- 当页面引用计数为 0 时调用
- 将其加入 LRU 候选集合

```
size_t Size();
```

- 返回当前可替换页面数量

------

### `BufferPoolManager`

数据库系统的核心调度组件，负责 **内存页与磁盘页之间的数据交换**。

#### 📌 核心方法

```
Page* fetch_page(int page_id);
```

- 从缓冲池获取页面：
  - 若在内存中：直接返回
  - 若不在：
    - 从磁盘加载
    - 通过 LRU 选择 victim 页进行替换

------

```
bool unpin_page(int page_id, bool is_dirty);
```

- 释放页面引用（pin_count--）

- 若 

  ```
  is_dirty = true
  ```

  ：

  - 标记该页为脏页（后续需要写回磁盘）

------

```
Page* new_page(int& page_id);
```

- 分配一个新页面：
  - 分配新的 `page_id`
  - 在缓冲池中创建页
  - 必要时触发页面置换

------

```
bool flush_page(int page_id);
```

- 强制将指定页面写回磁盘
- 常用于：
  - checkpoint
  - 数据持久化保障

------

## 3.  索引层 (Indexing Layer)

------

### 📄 B+ 树节点结构

通过直接操作页内存（reinterpret_cast）实现高效存储。

------

#### `BTreePageHeader`

公共页头结构：

```
struct BTreePageHeader {
    bool is_leaf;
    int size;
    int max_size;
    int parent_page_id;
};
```

- `is_leaf`：是否为叶子节点
- `size`：当前存储元素数量
- `max_size`：最大容量
- `parent_page_id`：父节点页 ID

------

#### `LeafPage`

```
struct LeafPage {
    BTreePageHeader header;
    pair<int, Rid> array[];
    int next_page_id;
};
```

- 存储实际数据 `(key, Rid)`
- 叶子节点之间通过 `next_page_id` 串联（链表结构）
- 支持范围查询（range scan）

------

#### `InternalPage`

```
struct InternalPage {
    BTreePageHeader header;
    pair<int, int> array[]; // key, child_page_id
};
```

- 存储索引键和子节点指针
- 用于导航查找路径

------

### `BPlusTree`

基于缓冲池实现的持久化 B+ 树索引结构。

------

#### 📌 核心方法

```
bool Insert(int key, const Rid& rid);
```

- 插入键值对 `(key, rid)`
- 若节点满：
  - 执行分裂操作
  - 递归向上调整

------

```
bool Search(int key, Rid* result);
```

- 查找指定 key
- 若存在：
  - 返回对应 `Rid`

------

```
Page* find_leaf(int key);
```

- 从根节点开始查找
- 返回目标叶子节点

------

```
void split_leaf(Page* leaf_page);
```

- 叶子节点分裂：
  - 拆分为两个节点
  - 更新链表指针
  - 向父节点插入分裂键

------

```
void split_internal(Page* internal_page, int key, int new_child_id);
```

- 内部节点分裂：
  - 递归向上处理
  - 更新子节点的父指针

------

```
void Print();
```

- 层序遍历打印 B+ 树结构
- 用于调试：
  - 树高度
  - 平衡性
  - 分裂情况

------

## 4.  辅助结构 (Supporting Structures)

------

### `Page`

```
struct Page {
    char data[4096];
    bool is_dirty;
    int pin_count;
};
```

- `data`：实际数据（4KB）
- `is_dirty`：是否被修改
- `pin_count`：引用计数（防止被替换）

------

### `Rid` (Record ID)

```
struct Rid {
    int page_id;
    int slot_num;
};
```

- 唯一标识一条记录
- 用于定位数据在磁盘中的位置：
  - `page_id`：所在页
  - `slot_num`：页内偏移


