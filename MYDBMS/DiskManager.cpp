#include "DiskManager.h"
using namespace std;

DiskManager::DiskManager(const string& db_file) : file_name(db_file) {
	// 以读写 + 二进制模式打开
	db_io_.open(db_file, ios::in | ios::out | ios::binary);

	// 如果文件不存在，则创建一个新文件
	if(!db_io_.is_open()) {
		db_io_.clear(); // 清除错误状态
		db_io_.open(db_file, ios::out | ios::binary | ios::trunc); // 创建新文件
		db_io_.close(); // 关闭新文件
		db_io_.open(db_file, ios::in | ios::out | ios::binary); // 重新以读写模式打开
	}
}

DiskManager::~DiskManager() {
	if(db_io_.is_open()) {
		db_io_.close();
	}
}

void DiskManager::write_page(int page_id, const char* data) {
	// 计算页的偏移位置
	size_t offset = static_cast<size_t> (page_id) * PAGE_SIZE;
	db_io_.seekp(offset); // 定位到页的起始位置
	db_io_.write(data, PAGE_SIZE); // 写入数据
	db_io_.flush(); // 确保数据写入磁盘
}

void DiskManager::read_page(int page_id, char* data) {
	// 计算页的偏移位置
	size_t offset = static_cast<size_t> (page_id) * PAGE_SIZE;
	db_io_.seekg(offset); // 定位到页的起始位置
	db_io_.read(data, PAGE_SIZE); // 读取数据

	// 如果读取的数据不足一页，填充剩余部分为0
	if (db_io_.gcount() < PAGE_SIZE) {
		memset(data + db_io_.gcount(), 0, PAGE_SIZE - db_io_.gcount());
	}
}