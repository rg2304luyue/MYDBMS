#include "BPlusTree.h"
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "BPlusTree.h"
#include <iostream>
#include <cassert>
#include <vector>
#include <cstdio>

using namespace std;

// 辅助函数：删除旧的数据库文件，确保测试环境干净
void CleanUp() {
    remove("test_db.db");
}

void BasicTest() {
    cout << "--- 正在运行基础测试 (Basic Test) ---" << endl;
    CleanUp();

    DiskManager* disk_manager = new DiskManager("test_db.db");
    // 缓冲池大小设为 10 个页
    BufferPoolManager* bpm = new BufferPoolManager(10, disk_manager);
    BPlusTree* tree = new BPlusTree(bpm);

    // 1. 测试单条数据插入与查找
    Rid rid1 = { 10, 1 };
    tree->Insert(100, rid1);

    Rid result;
    bool found = tree->Search(100, &result);
    assert(found == true);
    assert(result.page_id == 10 && result.slot_num == 1);
    cout << "单条数据插入与查找: 通过" << endl;

    // 2. 测试查找不存在的键
    found = tree->Search(999, &result);
    assert(found == false);
    cout << "不存在的键查找: 通过" << endl;

    delete tree;
    delete bpm;
    delete disk_manager;
    cout << "基础测试全部通过！\n" << endl;
}

void SplitAndScaleTest() {
    cout << "--- 正在运行分裂与压力测试 (Split and Scale Test) ---" << endl;
    CleanUp();

    DiskManager* disk_manager = new DiskManager("test_db.db");
    BufferPoolManager* bpm = new BufferPoolManager(50, disk_manager);
    BPlusTree* tree = new BPlusTree(bpm);

    // 插入 500 条数据，由于 PAGE_SIZE 是 4096，这必然会触发叶子节点和内部节点的分裂
    const int num_entries = 500;
    cout << "正在插入 " << num_entries << " 条数据以触发 B+ 树分裂..." << endl;

    for (int i = 1; i <= num_entries; ++i) {
        Rid rid = { i, i % 10 };
        bool success = tree->Insert(i, rid);
        if (!success) {
            cout << "插入失败，Key: " << i << endl;
        }
    }

    // 验证所有数据是否都能找回
    cout << "正在验证数据一致性..." << endl;
    for (int i = 1; i <= num_entries; ++i) {
        Rid result;
        bool found = tree->Search(i, &result);
        if (!found || result.page_id != i || result.slot_num != (i % 10)) {
            cout << "验证失败! Key: " << i << ", Found: " << found << endl;
            assert(false);
        }
    }
    cout << "500 条数据一致性校验: 通过" << endl;

    // 打印树结构（如果你的 Print 函数已实现）
    cout << "当前树结构示意图:" << endl;
    tree->Print();

    delete tree;
    delete bpm;
    delete disk_manager;
    cout << "分裂与压力测试全部通过！" << endl;
}

int main() {
    try {
        BasicTest();
        SplitAndScaleTest();
        cout << "所有测试用例运行完毕！" << endl;
    }
    catch (const exception& e) {
        cerr << "测试过程中发生异常: " << e.what() << endl;
        return 1;
    }
    return 0;
}