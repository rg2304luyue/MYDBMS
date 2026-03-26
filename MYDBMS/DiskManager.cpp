#include "DiskManager.h"
using namespace std;

DiskManager::DiskManager(const string& db_file) : file_name(db_file) {
	// 以读写 + 二进制模式打开
	db_io_.open(db_file, ios::in | ios::out | ios::binary);

	// 如果文件不存在，则创建一个新文件
	if(!db_io_.is_open()) {
		db_io_.open(db_file, ios::out | ios::binary); // 创建新文件
		db_io_.close(); // 关闭新文件
		db_io_.open(db_file, ios::in | ios::out | ios::binary); // 重新以读写模式打开
	}

	if (!db_io_.is_open()) {
		throw runtime_error("DiskManager: cannot open file " + db_file);
	}
}

DiskManager::~DiskManager() {
	if(db_io_.is_open()) {
		db_io_.close();
	}
}

void DiskManager::write_page(int page_id, const char* data) {
	// seekp = seek for put（写指针）
	db_io_.seekp(page_offset(page_id)); // 定位到页的起始位置
	db_io_.write(data, PAGE_SIZE); // 写入数据
	db_io_.flush(); // 确保数据写入磁盘

	// 检查写入是否成功
	if (db_io_.bad()) {
		throw runtime_error("DiskManager: write_page failed for page " + to_string(page_id));
	}
}

void DiskManager::read_page(int page_id, char* data) {
	// seekg = seek for get（读指针）
	db_io_.seekg(page_offset(page_id)); // 定位到页的起始位置
	db_io_.read(data, PAGE_SIZE); // 读取数据

	// 如果读取的数据不足一页，填充剩余部分为0
	int read_count = static_cast<int>(db_io_.gcount());
	if (read_count < PAGE_SIZE) {
		memset(data + read_count, 0, PAGE_SIZE - read_count);
	}

	// 读完后清除 eof 标志，否则下次 seekg 会失败
	db_io_.clear();
}

int DiskManager::get_total_pages() {
	// seekg 到文件末尾，tellg 返回当前字节位置，除以页大小就是页数
	db_io_.seekg(0, ios::end);
	size_t file_size = static_cast<size_t>(db_io_.tellg());
	db_io_.clear();
	// 整除，不足一页的尾部忽略（正常情况下文件大小一定是PAGE_SIZE的整数倍）
	return static_cast<int>(file_size / PAGE_SIZE);
}