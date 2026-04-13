#pragma once
#include "BPlusTree.h"
#include "Record.h"
#include <functional>
using namespace std;

// ── 堆页头部（紧贴 Page::data 开头） ─────────────────────────
//
// 每个堆页的布局：
//   [HeapPageHeader][slot_0][slot_1]...[slot_N-1]
//
// slot 大小固定为 record_size，由 Schema 在运行期计算
struct HeapPageHeader {
    int page_id;
    int record_count;   // 当前页已写入的 slot 数量
    int max_records;    // 该页最多能放多少 slot
    int next_page_id;   // 下一个堆页（-1 = 末尾）
};

// ── TableHeap ─────────────────────────────────────────────────
//
// 职责：管理一张表所有行数据的物理存储
//   - insert()：找有空位的页，写入序列化行，返回 Rid
//   - fetch() ：按 Rid 随机访问一条记录
//   - scan()  ：按页链表顺序遍历所有行（全表扫描）
//
// 设计说明：
//   1. 每行定长，slot 按下标寻址，O(1) 随机读写
//   2. 删除暂用"逻辑删除"（tombstone bit），这里简化为只支持索引删
//   3. 多页通过 next_page_id 串成单向链表，支持大表
class TableHeap {
public:
    // 新建表：first_page_id = -1
    // 加载已有表：传入 first_page_id
    TableHeap(BufferPoolManager* bpm, const Schema& schema, int first_page_id = -1)
        : bpm_(bpm), schema_(schema)
    {
        record_size_ = Record::record_size(schema);
        max_per_page_ = (PAGE_SIZE - (int)sizeof(HeapPageHeader)) / record_size_;

        if (first_page_id == -1) {
            // 新建第一个堆页
            int pid;
            Page* p = bpm_->new_page(pid);
            if (!p) throw runtime_error("TableHeap: cannot allocate first page");
            init_heap_page(p, pid);
            bpm_->unpin_page(pid, true);
            first_page_id_ = pid;
        }
        else {
            first_page_id_ = first_page_id;
        }
    }

    // ── 插入一条记录 ──────────────────────────────────────────
    // 遍历页链表，找到第一个有空位的页；若全满则追加新页
    Rid insert(const Record& rec) {
        vector<char> buf(record_size_, 0);
        rec.serialize(buf.data(), schema_);

        int cur_pid = first_page_id_;
        while (true) {
            Page* page = bpm_->fetch_page(cur_pid);
            if (!page) throw runtime_error("TableHeap::insert: fetch failed");
            HeapPageHeader* hdr = reinterpret_cast<HeapPageHeader*>(page->data);

            if (hdr->record_count < hdr->max_records) {
                // 有空位
                int slot = hdr->record_count;
                char* slot_ptr = page->data + sizeof(HeapPageHeader) + slot * record_size_;
                memcpy(slot_ptr, buf.data(), record_size_);
                hdr->record_count++;
                Rid rid{ cur_pid, slot };
                bpm_->unpin_page(cur_pid, true);
                return rid;
            }

            int next_pid = hdr->next_page_id;
            bpm_->unpin_page(cur_pid, false);

            if (next_pid == -1) {
                // 当前页是最后一页且已满，分配新页
                int new_pid;
                Page* np = bpm_->new_page(new_pid);
                if (!np) throw runtime_error("TableHeap::insert: cannot allocate new page");
                init_heap_page(np, new_pid);
                bpm_->unpin_page(new_pid, true);

                // 把前一页的 next 指针指向新页
                Page* prev = bpm_->fetch_page(cur_pid);
                HeapPageHeader* ph = reinterpret_cast<HeapPageHeader*>(prev->data);
                ph->next_page_id = new_pid;
                bpm_->unpin_page(cur_pid, true);

                cur_pid = new_pid;
            }
            else {
                cur_pid = next_pid;
            }
        }
    }

    // ── 按 Rid 随机读取一条记录 ───────────────────────────────
    Record fetch(const Rid& rid) const {
        Page* page = bpm_->fetch_page(rid.page_id);
        if (!page) throw runtime_error("TableHeap::fetch: fetch failed");
        const char* slot_ptr =
            page->data + sizeof(HeapPageHeader) + rid.slot_num * record_size_;
        Record rec = Record::deserialize(slot_ptr, schema_);
        bpm_->unpin_page(rid.page_id, false);
        return rec;
    }

    // ── 全表扫描 ──────────────────────────────────────────────
    // callback(rid, record) 返回 false 时提前停止扫描
    void scan(function<bool(const Rid&, const Record&)> callback) const {
        int cur_pid = first_page_id_;
        while (cur_pid != -1) {
            Page* page = bpm_->fetch_page(cur_pid);
            if (!page) break;
            HeapPageHeader* hdr = reinterpret_cast<HeapPageHeader*>(page->data);
            int count = hdr->record_count;
            int next_pid = hdr->next_page_id;

            for (int i = 0; i < count; i++) {
                const char* slot_ptr =
                    page->data + sizeof(HeapPageHeader) + i * record_size_;
                Record rec = Record::deserialize(slot_ptr, schema_);
                Rid rid{ cur_pid, i };
                bpm_->unpin_page(cur_pid, false);  // 回调期间先 unpin，避免回调内嵌套 fetch 死锁
                if (!callback(rid, rec)) return;
                // 重新 fetch 以读下一个 slot（简化实现；生产中可持锁遍历）
                page = bpm_->fetch_page(cur_pid);
                if (!page) return;
                hdr = reinterpret_cast<HeapPageHeader*>(page->data);
            }
            bpm_->unpin_page(cur_pid, false);
            cur_pid = next_pid;
        }
    }

    int get_first_page_id() const { return first_page_id_; }
    int get_record_size()   const { return record_size_; }
    const Schema& get_schema() const { return schema_; }

private:
    BufferPoolManager* bpm_;
    Schema schema_;
    int first_page_id_;
    int record_size_;
    int max_per_page_;

    void init_heap_page(Page* page, int page_id) {
        memset(page->data, 0, PAGE_SIZE);
        HeapPageHeader* hdr = reinterpret_cast<HeapPageHeader*>(page->data);
        hdr->page_id = page_id;
        hdr->record_count = 0;
        hdr->max_records = max_per_page_;
        hdr->next_page_id = -1;
    }
};
