// ============================================================
//  MyDBMS  --  集成演示 & 回归测试
// ============================================================
#include "Executor.h"
#include <iostream>
#include <cassert>
#include <cstdio>
using namespace std;

// ── 彩色输出（终端）────────────────────────────────────────
#define GRN "\033[32m"
#define RED "\033[31m"
#define CYN "\033[36m"
#define YEL "\033[33m"
#define RST "\033[0m"

static void sql(Executor& exec, const string& s) {
    cout << CYN << "SQL> " << RST << s << "\n";
    string res = exec.execute(s);
    cout << res << "\n\n";
}

static void assert_ok(Executor& exec, const string& s) {
    string res = exec.execute(s);
    if (res.rfind("OK", 0) != 0 && res.rfind("(", 0) != 0) {
        cerr << RED << "FAIL: " << s << "\n  -> " << res << RST << "\n";
        exit(1);
    }
}

static bool contains(const string& res, const string& sub) {
    return res.find(sub) != string::npos;
}

// ── 测试 1：基础 CRUD ────────────────────────────────────────
void test_basic_crud() {
    cout << YEL << "\n===== Test 1: Basic CRUD =====\n" << RST;
    remove("mydb.db");

    DiskManager disk("mydb.db");
    BufferPoolManager bpm(32, &disk);
    Catalog catalog(&bpm);
    Executor exec(&catalog);

    sql(exec, "CREATE TABLE users (id INT PK, name VARCHAR(32), age INT)");
    sql(exec, "INSERT INTO users VALUES (1, 'Alice', 20)");
    sql(exec, "INSERT INTO users VALUES (2, 'Bob', 22)");
    sql(exec, "INSERT INTO users VALUES (3, 'Charlie', 21)");

    // SELECT ALL
    string res = exec.execute("SELECT * FROM users");
    assert(contains(res, "Alice") && contains(res, "Bob") && contains(res, "Charlie"));
    cout << CYN << "SQL> " RST "SELECT * FROM users\n" << res << "\n\n";

    // SELECT with index
    res = exec.execute("SELECT * FROM users WHERE id = 2");
    assert(contains(res, "Bob"));
    assert(contains(res, "Index Scan"));
    cout << CYN << "SQL> " RST "SELECT * FROM users WHERE id = 2\n" << res << "\n\n";

    // DELETE + verify
    assert_ok(exec, "DELETE FROM users WHERE id = 1");
    res = exec.execute("SELECT * FROM users WHERE id = 1");
    assert(contains(res, "0 rows"));
    cout << GRN << "✓ Basic CRUD passed\n" << RST;
}

// ── 测试 2：大批量插入（触发 B+ 树多次分裂）───────────────
void test_split_and_scale() {
    cout << YEL << "\n===== Test 2: Split & Scale (500 rows) =====\n" << RST;
    remove("mydb.db");

    DiskManager disk("mydb.db");
    BufferPoolManager bpm(64, &disk);
    Catalog catalog(&bpm);
    Executor exec(&catalog);

    exec.execute("CREATE TABLE nums (id INT PK, val INT)");

    const int N = 500;
    cout << "Inserting " << N << " rows...\n";
    for (int i = 1; i <= N; i++) {
        string sql_str = "INSERT INTO nums VALUES ("
            + to_string(i) + ", " + to_string(i * 10) + ")";
        string res = exec.execute(sql_str);
        if (res.rfind("OK", 0) != 0) {
            cerr << RED << "Insert failed at i=" << i << ": " << res << RST << "\n";
            exit(1);
        }
    }

    // 验证每一条
    cout << "Verifying all " << N << " rows via index...\n";
    for (int i = 1; i <= N; i++) {
        string q = "SELECT * FROM nums WHERE id = " + to_string(i);
        string res = exec.execute(q);
        if (!contains(res, to_string(i)) || !contains(res, "Index Scan")) {
            cerr << RED << "Verify failed at i=" << i << ": " << res << RST << "\n";
            exit(1);
        }
    }
    cout << GRN << "✓ All " << N << " rows verified\n" << RST;
}

// ── 测试 3：重复主键拒绝插入 ────────────────────────────────
void test_duplicate_pk() {
    cout << YEL << "\n===== Test 3: Duplicate PK rejection =====\n" << RST;
    remove("mydb.db");

    DiskManager disk("mydb.db");
    BufferPoolManager bpm(16, &disk);
    Catalog catalog(&bpm);
    Executor exec(&catalog);

    exec.execute("CREATE TABLE t (id INT PK, v INT)");
    exec.execute("INSERT INTO t VALUES (1, 100)");
    string res = exec.execute("INSERT INTO t VALUES (1, 200)");
    assert(contains(res, "ERROR") && contains(res, "Duplicate"));
    cout << "Result: " << res << "\n";
    cout << GRN << "✓ Duplicate PK correctly rejected\n" << RST;
}

// ── 测试 4：多表操作 ─────────────────────────────────────────
void test_multi_table() {
    cout << YEL << "\n===== Test 4: Multi-table =====\n" << RST;
    remove("mydb.db");

    DiskManager disk("mydb.db");
    BufferPoolManager bpm(32, &disk);
    Catalog catalog(&bpm);
    Executor exec(&catalog);

    sql(exec, "CREATE TABLE orders (oid INT PK, amount INT)");
    sql(exec, "CREATE TABLE products (pid INT PK, name VARCHAR(20))");
    sql(exec, "SHOW TABLES");
    sql(exec, "INSERT INTO orders VALUES (1, 500)");
    sql(exec, "INSERT INTO products VALUES (42, 'Widget')");
    sql(exec, "DESC orders");

    string r1 = exec.execute("SELECT * FROM orders");
    string r2 = exec.execute("SELECT * FROM products");
    assert(contains(r1, "500") && contains(r2, "Widget"));
    cout << GRN << "✓ Multi-table passed\n" << RST;
}

// ── 测试 5：全表扫描 WHERE（非主键列）────────────────────────
void test_full_scan_where() {
    cout << YEL << "\n===== Test 5: Full scan WHERE on non-PK column =====\n" << RST;
    remove("mydb.db");

    DiskManager disk("mydb.db");
    BufferPoolManager bpm(16, &disk);
    Catalog catalog(&bpm);
    Executor exec(&catalog);

    exec.execute("CREATE TABLE emp (id INT PK, dept INT, name VARCHAR(20))");
    exec.execute("INSERT INTO emp VALUES (1, 10, 'Alice')");
    exec.execute("INSERT INTO emp VALUES (2, 20, 'Bob')");
    exec.execute("INSERT INTO emp VALUES (3, 10, 'Carol')");

    string res = exec.execute("SELECT * FROM emp WHERE dept = 10");
    assert(contains(res, "Alice") && contains(res, "Carol") && !contains(res, "Bob"));
    assert(contains(res, "Full Scan"));
    cout << res << "\n";
    cout << GRN << "✓ Full scan WHERE passed\n" << RST;
}

// ── main ─────────────────────────────────────────────────────
int main() {
    test_basic_crud();
    test_split_and_scale();
    test_duplicate_pk();
    test_multi_table();
    test_full_scan_where();

    cout << GRN << "\n=============================\n";
    cout << "  All tests passed!\n";
    cout << "=============================\n" << RST;
    return 0;
}