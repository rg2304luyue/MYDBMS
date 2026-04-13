#pragma once
#include "TableHeap.h"
#include "BPlusTree.h"
#include <unordered_map>
#include <memory>
#include <string>
using namespace std;

// ── 每张表的元数据 ────────────────────────────────────────────
struct TableMeta {
    Schema schema;
    int    heap_first_page_id;  // 堆文件第一页（用于重启恢复，本实现暂为内存态）
};

// ── Catalog ───────────────────────────────────────────────────
//
// 职责：管理所有表的 Schema、TableHeap、BPlusTree 实例
//
// 简化说明：
//   - 本实现为"内存 Catalog"，重启后元数据丢失
//   - 生产系统（如 MySQL）会把 schema 写入系统表页（page 0/1），
//     启动时先读系统表重建 Catalog
class Catalog {
public:
    explicit Catalog(BufferPoolManager* bpm) : bpm_(bpm) {}

    // ── 建表 ──────────────────────────────────────────────────
    // 返回 false 表示表已存在
    bool create_table(const Schema& schema) {
        string name(schema.table_name);
        if (tables_.count(name)) return false;

        TableMeta meta;
        meta.schema = schema;

        // 建堆文件
        auto heap = make_unique<TableHeap>(bpm_, schema);
        meta.heap_first_page_id = heap->get_first_page_id();
        heaps_[name] = std::move(heap);

        // 若有主键列，建 B+ 树索引（只支持 INT 主键）
        if (schema.pk_col_index >= 0 &&
            schema.columns[schema.pk_col_index].type == ColType::INT)
        {
            indexes_[name] = make_unique<BPlusTree>(bpm_);
        }

        tables_[name] = meta;
        return true;
    }

    // ── 查询接口 ──────────────────────────────────────────────
    TableHeap* get_heap(const string& name) { return heaps_.count(name) ? heaps_[name].get() : nullptr; }
    BPlusTree* get_index(const string& name) { return indexes_.count(name) ? indexes_[name].get() : nullptr; }
    const TableMeta* get_meta(const string& name) const { auto it = tables_.find(name); return it == tables_.end() ? nullptr : &it->second; }
    bool             exists(const string& name) const { return tables_.count(name) > 0; }

    // 列出所有表名（SHOW TABLES 用）
    vector<string> all_tables() const {
        vector<string> res;
        for (auto& [k, _] : tables_) res.push_back(k);
        return res;
    }

private:
    BufferPoolManager* bpm_;
    unordered_map<string, TableMeta>            tables_;
    unordered_map<string, unique_ptr<TableHeap>> heaps_;
    unordered_map<string, unique_ptr<BPlusTree>> indexes_;
};
