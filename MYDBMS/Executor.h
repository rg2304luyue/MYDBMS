#pragma once
#define _CRT_SECURE_NO_WARNINGS
#include "Catalog.h"
#include <sstream>
#include <algorithm>
#include <stdexcept>
#include <climits>
using namespace std;

// ── 极简 SQL 执行引擎 ─────────────────────────────────────────
//
// 支持语法：
//   CREATE TABLE t (id INT PK, name VARCHAR(32), age INT)
//   INSERT INTO t VALUES (1, 'alice', 20)
//   SELECT * FROM t
//   SELECT * FROM t WHERE col = val     ← INT 主键走索引，其余全扫
//   DELETE FROM t WHERE col = val       ← INT 主键走索引删除
//   SHOW TABLES
//   DESC t
//
// 架构说明：
//   parse → 分词/切词（手写，无外部依赖）
//   plan  → 根据是否有索引决定访问路径（Index Scan vs Full Scan）
//   exec  → 调用 Catalog / TableHeap / BPlusTree 完成实际操作
class Executor {
public:
    explicit Executor(Catalog* catalog) : cat_(catalog) {}

    // 入口：执行一条 SQL，返回结果字符串
    string execute(const string& sql) {
        try {
            string s = trim(sql);
            string sl = to_lower(s);
            if (sl.rfind("create table", 0) == 0) return exec_create(s);
            if (sl.rfind("insert into", 0) == 0) return exec_insert(s);
            if (sl.rfind("select", 0) == 0) return exec_select(s);
            if (sl.rfind("delete from", 0) == 0) return exec_delete(s);
            if (sl.rfind("show tables", 0) == 0) return exec_show();
            if (sl.rfind("desc ", 0) == 0) return exec_desc(trim(s.substr(5)));
            return "ERROR: Unknown SQL: " + s;
        }
        catch (const exception& e) {
            return string("ERROR: ") + e.what();
        }
    }

private:
    Catalog* cat_;

    // ═══════════════════════════════════════════════════════════
    // CREATE TABLE t (col TYPE [PK], ...)
    // ═══════════════════════════════════════════════════════════
    string exec_create(const string& sql) {
        // 跳过 "CREATE TABLE "
        string rest = trim(sql.substr(12));
        auto paren = rest.find('(');
        if (paren == string::npos) return "ERROR: Expected '(' after table name";

        string table_name = trim(rest.substr(0, paren));
        string cols_str = rest.substr(paren + 1);
        auto close = cols_str.rfind(')');
        if (close != string::npos) cols_str = cols_str.substr(0, close);

        Schema schema;
        memset(&schema, 0, sizeof(schema));
        strncpy(schema.table_name, table_name.c_str(), 31);
        schema.col_count = 0;
        schema.pk_col_index = -1;

        for (auto& def : split(cols_str, ',')) {
            def = trim(def);
            auto parts = split_ws(def);
            if (parts.size() < 2)
                return "ERROR: Bad column definition: " + def;

            ColumnDef cd;
            memset(&cd, 0, sizeof(cd));
            strncpy(cd.name, parts[0].c_str(), 31);

            string type_str = to_lower(parts[1]);
            if (type_str == "int") {
                cd.type = ColType::INT;
                cd.max_len = 4;
            }
            else if (type_str.rfind("varchar", 0) == 0) {
                cd.type = ColType::VARCHAR;
                auto lp = type_str.find('('), rp = type_str.find(')');
                cd.max_len = (lp != string::npos && rp != string::npos)
                    ? stoi(type_str.substr(lp + 1, rp - lp - 1)) : 32;
            }
            else {
                return "ERROR: Unknown type '" + type_str + "'";
            }

            // 检查 PK 关键字
            for (int k = 2; k < (int)parts.size(); k++)
                if (to_lower(parts[k]) == "pk")
                    schema.pk_col_index = schema.col_count;

            schema.columns[schema.col_count++] = cd;
            if (schema.col_count >= 16)
                return "ERROR: Too many columns (max 16)";
        }

        if (cat_->exists(table_name))
            return "ERROR: Table '" + table_name + "' already exists";

        cat_->create_table(schema);
        return "OK: Table '" + table_name + "' created ("
            + to_string(schema.col_count) + " columns)";
    }

    // ═══════════════════════════════════════════════════════════
    // INSERT INTO t VALUES (v1, v2, ...)
    // ═══════════════════════════════════════════════════════════
    string exec_insert(const string& sql) {
        string rest = trim(sql.substr(12));  // skip "INSERT INTO "
        auto vp = to_lower(rest).find("values");
        if (vp == string::npos) return "ERROR: Missing VALUES keyword";

        string table_name = trim(rest.substr(0, vp));
        const TableMeta* meta = cat_->get_meta(table_name);
        if (!meta) return "ERROR: Table '" + table_name + "' not found";

        string vals_str = rest.substr(vp + 6);
        auto lp = vals_str.find('('), rp = vals_str.rfind(')');
        if (lp == string::npos || rp == string::npos)
            return "ERROR: Missing parentheses in VALUES";
        vals_str = vals_str.substr(lp + 1, rp - lp - 1);

        auto tokens = split(vals_str, ',');
        if ((int)tokens.size() != meta->schema.col_count)
            return "ERROR: Column count mismatch ("
            + to_string(tokens.size()) + " vs "
            + to_string(meta->schema.col_count) + ")";

        Record rec;
        for (int i = 0; i < (int)tokens.size(); i++) {
            string v = trim(tokens[i]);
            const ColumnDef& col = meta->schema.columns[i];
            if (col.type == ColType::INT) {
                rec.fields.push_back(stoi(v));
            }
            else {
                // 去掉单引号
                if (v.size() >= 2 && v.front() == '\'' && v.back() == '\'')
                    v = v.substr(1, v.size() - 2);
                if ((int)v.size() > col.max_len)
                    return "ERROR: VARCHAR value too long for column "
                    + string(col.name);
                rec.fields.push_back(v);
            }
        }

        // 先检查主键是否重复（查索引）
        BPlusTree* idx = cat_->get_index(table_name);
        if (idx && meta->schema.pk_col_index >= 0) {
            int pk_val = get<int>(rec.fields[meta->schema.pk_col_index]);
            Rid dummy;
            if (idx->Search(pk_val, &dummy))
                return "ERROR: Duplicate primary key " + to_string(pk_val);
        }

        // 写入堆文件
        TableHeap* heap = cat_->get_heap(table_name);
        Rid rid = heap->insert(rec);

        // 更新 B+ 树索引
        if (idx && meta->schema.pk_col_index >= 0) {
            int pk_val = get<int>(rec.fields[meta->schema.pk_col_index]);
            idx->Insert(pk_val, rid);
        }

        return "OK: 1 row inserted";
    }

    // ═══════════════════════════════════════════════════════════
    // SELECT * FROM t [WHERE col = val]
    // ═══════════════════════════════════════════════════════════
    string exec_select(const string& sql) {
        string sl = to_lower(sql);
        auto from_pos = sl.find(" from ");
        if (from_pos == string::npos) return "ERROR: Missing FROM";

        string after_from = trim(sql.substr(from_pos + 6));
        string table_name;
        bool   has_where = false;
        string where_col;
        int    where_val = INT_MIN;

        auto where_pos = to_lower(after_from).find(" where ");
        if (where_pos != string::npos) {
            table_name = trim(after_from.substr(0, where_pos));
            string cond = trim(after_from.substr(where_pos + 7));
            auto   eq = cond.find('=');
            if (eq == string::npos) return "ERROR: Bad WHERE clause";
            where_col = trim(cond.substr(0, eq));
            where_val = stoi(trim(cond.substr(eq + 1)));
            has_where = true;
        }
        else {
            table_name = trim(after_from);
        }

        const TableMeta* meta = cat_->get_meta(table_name);
        if (!meta) return "ERROR: Table '" + table_name + "' not found";

        vector<Record> results;

        // ── 访问路径选择 ──────────────────────────────────────
        BPlusTree* idx = cat_->get_index(table_name);
        int        pk_col = meta->schema.pk_col_index;
        bool       use_idx = has_where && idx &&
            pk_col >= 0 &&
            string(meta->schema.columns[pk_col].name) == where_col;

        if (use_idx) {
            // Index Scan：O(log N)，精确点查
            Rid rid;
            if (idx->Search(where_val, &rid)) {
                TableHeap* heap = cat_->get_heap(table_name);
                results.push_back(heap->fetch(rid));
            }
        }
        else {
            // Full Table Scan：顺序遍历堆文件
            TableHeap* heap = cat_->get_heap(table_name);
            heap->scan([&](const Rid&, const Record& rec) -> bool {
                if (!has_where) {
                    results.push_back(rec);
                }
                else {
                    int col_i = meta->schema.col_index(where_col);
                    if (col_i < 0) return false;  // 列不存在，终止
                    if (meta->schema.columns[col_i].type == ColType::INT) {
                        if (get<int>(rec.fields[col_i]) == where_val)
                            results.push_back(rec);
                    }
                }
                return true;
                });
        }

        return format_table(meta->schema, results,
            use_idx ? "[Index Scan]" : "[Full Scan]");
    }

    // ═══════════════════════════════════════════════════════════
    // DELETE FROM t WHERE col = val
    // ═══════════════════════════════════════════════════════════
    string exec_delete(const string& sql) {
        string rest = trim(sql.substr(12));  // skip "DELETE FROM "
        auto where_pos = to_lower(rest).find(" where ");
        if (where_pos == string::npos)
            return "ERROR: DELETE requires a WHERE clause (safety guard)";

        string table_name = trim(rest.substr(0, where_pos));
        string cond = trim(rest.substr(where_pos + 7));
        auto eq = cond.find('=');
        if (eq == string::npos) return "ERROR: Bad WHERE clause";

        string where_col = trim(cond.substr(0, eq));
        int    where_val = stoi(trim(cond.substr(eq + 1)));

        const TableMeta* meta = cat_->get_meta(table_name);
        if (!meta) return "ERROR: Table '" + table_name + "' not found";

        int    pk_col = meta->schema.pk_col_index;
        bool   is_pk = pk_col >= 0 &&
            string(meta->schema.columns[pk_col].name) == where_col;
        BPlusTree* idx = cat_->get_index(table_name);

        if (is_pk && idx) {
            // 主键删除：直接走 B+ 树，O(log N)
            // 注意：堆文件中该 slot 标记为逻辑删除（本实现简化：只删索引）
            // 生产系统：用 tombstone bit 或 MVCC 版本链
            bool ok = idx->Delete(where_val);
            return ok ? "OK: 1 row deleted (index updated)"
                : "ERROR: Key " + to_string(where_val) + " not found";
        }
        else {
            return "ERROR: DELETE currently only supports primary key columns with index";
        }
    }

    // ═══════════════════════════════════════════════════════════
    // SHOW TABLES
    // ═══════════════════════════════════════════════════════════
    string exec_show() {
        auto tables = cat_->all_tables();
        if (tables.empty()) return "(no tables)";
        ostringstream oss;
        oss << "Tables:\n";
        for (auto& t : tables) oss << "  " << t << "\n";
        return oss.str();
    }

    // ═══════════════════════════════════════════════════════════
    // DESC t
    // ═══════════════════════════════════════════════════════════
    string exec_desc(const string& table_name) {
        const TableMeta* meta = cat_->get_meta(table_name);
        if (!meta) return "ERROR: Table '" + table_name + "' not found";

        ostringstream oss;
        oss << "Table: " << table_name << "\n";
        oss << string(40, '-') << "\n";
        oss << "Column          Type        PK\n";
        oss << string(40, '-') << "\n";
        for (int i = 0; i < meta->schema.col_count; i++) {
            const ColumnDef& col = meta->schema.columns[i];
            string type_str = (col.type == ColType::INT)
                ? "INT"
                : "VARCHAR(" + to_string(col.max_len) + ")";
            bool is_pk = (i == meta->schema.pk_col_index);
            oss << left;
            oss.width(16); oss << col.name;
            oss.width(12); oss << type_str;
            oss << (is_pk ? "YES" : "") << "\n";
        }
        return oss.str();
    }

    // ── 格式化输出为表格 ──────────────────────────────────────
    string format_table(const Schema& schema,
        const vector<Record>& rows,
        const string& scan_hint = "") {
        ostringstream oss;
        if (!scan_hint.empty()) oss << scan_hint << "\n";

        // 列名行
        for (int i = 0; i < schema.col_count; i++) {
            if (i) oss << " | ";
            oss << schema.columns[i].name;
        }
        oss << "\n" << string(schema.col_count * 14, '-') << "\n";

        // 数据行
        for (const auto& rec : rows) {
            for (int i = 0; i < (int)rec.fields.size(); i++) {
                if (i) oss << " | ";
                visit([&](auto& v) { oss << v; }, rec.fields[i]);
            }
            oss << "\n";
        }
        oss << "(" << rows.size() << " row"
            << (rows.size() != 1 ? "s" : "") << ")";
        return oss.str();
    }

    // ── 字符串工具 ────────────────────────────────────────────
    static string trim(const string& s) {
        auto l = s.find_first_not_of(" \t\r\n");
        auto r = s.find_last_not_of(" \t\r\n");
        return l == string::npos ? "" : s.substr(l, r - l + 1);
    }
    static string to_lower(string s) {
        transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s;
    }
    static vector<string> split(const string& s, char d) {
        vector<string> res;
        stringstream ss(s);
        string item;
        while (getline(ss, item, d)) res.push_back(item);
        return res;
    }
    static vector<string> split_ws(const string& s) {
        vector<string> res;
        istringstream iss(s);
        string w;
        while (iss >> w) res.push_back(w);
        return res;
    }
};