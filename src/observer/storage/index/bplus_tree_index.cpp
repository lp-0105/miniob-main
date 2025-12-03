/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai.wyl on 2021/5/19.
//

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/db/db.h"

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

int BplusTreeIndex::total_key_length() const
{
  // 直接使用基类计算好的值
  return key_length_;
}

RC BplusTreeIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const std::vector<const FieldMeta *> &field_metas)
{
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s",
        file_name, index_meta.name());
    return RC::RECORD_OPENNED;
  }

  if (field_metas.empty()) {
    LOG_ERROR("Failed to create index, field_metas is empty.");
    return RC::INVALID_ARGUMENT;
  }

  Index::init(index_meta, field_metas);

  // 计算复合键总长度
  int key_length = total_key_length();
  
  // 使用第一个字段的类型作为主类型（实际比较时会按字段逐个比较）
  // 或者可以使用CHARS类型，因为我们把多字段当作一个字节数组
  AttrType attr_type = field_metas[0]->type();

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, attr_type, key_length);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, rc:%s",
        file_name, index_meta.name(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully create index, file_name:%s, index:%s, field_count:%d, key_length:%d",
    file_name, index_meta.name(), static_cast<int>(field_metas_.size()), key_length);
  return RC::SUCCESS;
}

RC BplusTreeIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const std::vector<const FieldMeta *> &field_metas)
{
  if (inited_) {
    LOG_WARN("Failed to open index due to the index has been opened before. file_name:%s, index:%s",
        file_name, index_meta.name());
    return RC::RECORD_OPENNED;
  }

  if (field_metas.empty()) {
    LOG_ERROR("Failed to open index, field_metas is empty.");
    return RC::INVALID_ARGUMENT;
  }

  Index::init(index_meta, field_metas);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, rc:%s",
        file_name, index_meta.name(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully open index, file_name:%s, index:%s, field_count:%d",
    file_name, index_meta.name(), static_cast<int>(field_metas_.size()));
  return RC::SUCCESS;
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s", index_meta_.name());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

// ⭐ 构造复合键
void BplusTreeIndex::make_key(const char *record, char *key) const
{
  int offset = 0;
  // ⭐ 改为使用引用（因为现在存的是对象而不是指针）
  for (const FieldMeta &field : field_metas_) {
    memcpy(key + offset, record + field.offset(), field.len());
    offset += field.len();
  }
}

RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  // 构建复合键
  int key_length = total_key_length();
  char *key = new char[key_length];
  make_key(record, key);
  
  RC rc = index_handler_.insert_entry(key, rid);
  
  delete[] key;
  return rc;
}

RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  // 构建复合键
  int key_length = total_key_length();
  char *key = new char[key_length];
  make_key(record, key);
  
  RC rc = index_handler_.delete_entry(key, rid);
  
  delete[] key;
  return rc;
}

IndexScanner *BplusTreeIndex::create_scanner(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

RC BplusTreeIndexScanner::open(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}
