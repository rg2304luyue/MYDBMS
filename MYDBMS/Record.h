#pragma once
#include <vector>
#include <string>
#include <variant>
#include <cstring>
#include <sstream>
#include <stdexcept>
using namespace std;

// ── 支持的列类型 ──────────────────────────────────────────────
enum class ColType { INT, VARCHAR };

// ── 列定义 ────────────────────────────────────────────────────
struct ColumnDef {
    char name[32];      // 列名（定长，方便序列化）
    ColType type;
    int max_len;        // VARCHAR 最大字节数；INT 固定为 4
};

// ── 表 Schema ─────────────────────────────────────────────────
struct Schema {
    char table_name[32];
    int col_count;
    ColumnDef columns[16];  // 最多 16 列
    int pk_col_index;       // 主键列下标，-1 表示无主键

    // 按列名查下标，找不到返回 -1
    int col_index(const string& name) const {
        for (int i = 0; i < col_count; i++)
            if (string(columns[i].name) == name) return i;
        return -1;
    }
};

// ── 一个字段值（运行期多态） ──────────────────────────────────
using FieldVal = variant<int, string>;

// ── 一条记录（一行数据） ──────────────────────────────────────
//
// 磁盘格式（定长，便于按 slot 随机访问）：
//   INT    列：4 字节小端整数
//   VARCHAR列：4 字节实际长度 + max_len 字节内容（不足补 0）
struct Record {
    vector<FieldVal> fields;

    // 序列化到 buf（调用方保证 buf 足够大）
    void serialize(char* buf, const Schema& schema) const {
        int off = 0;
        for (int i = 0; i < schema.col_count; i++) {
            const ColumnDef& col = schema.columns[i];
            if (col.type == ColType::INT) {
                int v = get<int>(fields[i]);
                memcpy(buf + off, &v, 4);
                off += 4;
            }
            else {
                const string& s = get<string>(fields[i]);
                int len = (int)s.size();
                memcpy(buf + off, &len, 4);
                off += 4;
                memset(buf + off, 0, col.max_len);
                memcpy(buf + off, s.data(), len);
                off += col.max_len;
            }
        }
    }

    // 从 buf 反序列化
    static Record deserialize(const char* buf, const Schema& schema) {
        Record rec;
        int off = 0;
        for (int i = 0; i < schema.col_count; i++) {
            const ColumnDef& col = schema.columns[i];
            if (col.type == ColType::INT) {
                int v;
                memcpy(&v, buf + off, 4);
                rec.fields.push_back(v);
                off += 4;
            }
            else {
                int len;
                memcpy(&len, buf + off, 4);
                off += 4;
                string s(buf + off, len);
                rec.fields.push_back(s);
                off += col.max_len;
            }
        }
        return rec;
    }

    // 计算定长记录大小（字节）
    static int record_size(const Schema& schema) {
        int sz = 0;
        for (int i = 0; i < schema.col_count; i++) {
            const ColumnDef& col = schema.columns[i];
            sz += (col.type == ColType::INT) ? 4 : (4 + col.max_len);
        }
        return sz;
    }

    // 调试用：转成可读字符串
    string to_string(const Schema& schema) const {
        ostringstream oss;
        for (int i = 0; i < (int)fields.size(); i++) {
            if (i) oss << " | ";
            visit([&](auto& v) { oss << v; }, fields[i]);
        }
        return oss.str();
    }
};