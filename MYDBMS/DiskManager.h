#pragma once
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
using namespace std;

static constexpr int PAGE_SIZE = 4096; // 4KB一页

class DiskManager {
public:
	// 构造函数
	DiskManager(const string& db_file);
	~DiskManager();

	// 写入指定页的数据
	void write_page(int page_id, const char* data);

	// 读取指定页的数据
	void read_page(int page_id, char* data);

	// 获取当前文件总页数
	int get_total_pages();

private:
		string file_name; // 数据库文件名
		fstream db_io_; // 文件流对象

		// 根据page_id计算文件中的偏移位置
		size_t page_offset(int page_id) {
			return static_cast<size_t>(page_id) * PAGE_SIZE;
		}
};