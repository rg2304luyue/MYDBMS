#include "DiskManager.h"
#include <iostream>
using namespace std;

int main() {
    DiskManager dm("test_db.db");

    const char* my_data = "Hello, C++ DBMS! This is Lu Yue's first page.";
    char buffer[PAGE_SIZE] = { 0 };
    memcpy(buffer, my_data, strlen(my_data));

    // 测试写入第 0 页
    dm.write_page(0, buffer);
    cout << "Successfully wrote to Page 0." << endl;

    // 测试读取第 0 页
    char read_buffer[PAGE_SIZE] = { 0 };
    dm.read_page(0, read_buffer);
    cout << "Read data: " << read_buffer << endl;

    return 0;
}