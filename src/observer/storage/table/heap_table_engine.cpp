/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/table/heap_table_engine.h"
#include "storage/record/heap_record_scanner.h"
#include "common/log/log.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/common/meta_util.h"
#include "storage/db/db.h"


HeapTableEngine::~HeapTableEngine()
{
  if (record_handler_ != nullptr) {
    delete record_handler_;
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
  }

  for (vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index;
  }
  indexes_.clear();

  LOG_INFO("Table has been closed: %s", table_meta_->name());
}
RC HeapTableEngine::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->insert_record(record.data(), table_meta_->record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                table_meta_->name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                table_meta_->name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

RC HeapTableEngine::insert_chunk(const Chunk& chunk)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->insert_chunk(chunk, table_meta_->record_size());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert chunk failed. table name=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }

  // TODO: insert chunk support update index
  return rc;
}

RC HeapTableEngine::visit_record(const RID &rid, function<bool(Record &)> visitor)
{
  return record_handler_->visit_record(rid, visitor);
}

RC HeapTableEngine::get_record(const RID &rid, Record &record)
{
  RC rc = record_handler_->get_record(rid, record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to visit record. rid=%s, table=%s, rc=%s", rid.to_string().c_str(), table_meta_->name(), strrc(rc));
    return rc;
  }

  return rc;
}

RC HeapTableEngine::delete_record(const Record &record)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record.data(), &record.rid());
    ASSERT(RC::SUCCESS == rc, 
           "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
           table_meta_->name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
  }
  rc = record_handler_->delete_record(&record.rid());
  return rc;
}

RC HeapTableEngine::insert_record_with_trx(Record &record, Trx *trx)
{
  RC rc = RC::SUCCESS;
  
  // 1. 先插入记录到文件
  rc = record_handler_->insert_record(record.data(), table_meta_->record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to insert record. table name=%s, rc=%s", table_meta_->name(), strrc(rc));
    return rc;
  }
  
  // 2. 插入索引条目
  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to insert record into indexes. table name=%s, rid=%s, rc=%s",
              table_meta_->name(), record.rid().to_string().c_str(), strrc(rc));
    // 回滚：删除已插入的记录
    RC rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback record insertion after index failure. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), record.rid().to_string().c_str(), strrc(rc2));
    }
    return rc;
  }
  
  // 3. 事务处理
  if (trx != nullptr) {
    rc = trx->insert_record(table_, record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to insert record in transaction. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), record.rid().to_string().c_str(), strrc(rc));
      // 回滚整个操作
      RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false);
      RC rc3 = record_handler_->delete_record(&record.rid());
      if (rc2 != RC::SUCCESS || rc3 != RC::SUCCESS) {
        LOG_ERROR("Failed to rollback transaction insertion");
      }
      return rc;
    }
  }
  
  return RC::SUCCESS;
}

RC HeapTableEngine::delete_record_with_trx(const Record &record, Trx *trx)
{
  RC rc = RC::SUCCESS;
  
  // 1. 先删除索引条目
  rc = delete_entry_of_indexes(record.data(), record.rid(), false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete record from indexes. table name=%s, rid=%s, rc=%s",
              table_meta_->name(), record.rid().to_string().c_str(), strrc(rc));
    return rc;
  }
  
  // 2. 删除记录
  rc = record_handler_->delete_record(&record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete record. table name=%s, rid=%s, rc=%s",
              table_meta_->name(), record.rid().to_string().c_str(), strrc(rc));
    // 回滚：重新插入索引条目
    RC rc2 = insert_entry_of_indexes(record.data(), record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index deletion after record deletion failure. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), record.rid().to_string().c_str(), strrc(rc2));
    }
    return rc;
  }
  
  // 3. 事务处理
  if (trx != nullptr) {
    // 创建一个非const的Record对象用于事务处理
    Record non_const_record;
    non_const_record.set_rid(record.rid());
    // 复制记录数据
    char *data = new char[table_meta_->record_size()];
    memcpy(data, record.data(), table_meta_->record_size());
    non_const_record.set_data(data, table_meta_->record_size());
    
    rc = trx->delete_record(table_, non_const_record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to delete record in transaction. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), record.rid().to_string().c_str(), strrc(rc));
      // 回滚整个操作
      RC rc2 = insert_entry_of_indexes(record.data(), record.rid());
      RID new_rid;
      RC rc3 = record_handler_->insert_record(record.data(), table_meta_->record_size(), &new_rid);
      if (rc2 != RC::SUCCESS || rc3 != RC::SUCCESS) {
        LOG_ERROR("Failed to rollback transaction deletion");
      }
      delete[] data;
      return rc;
    }
    delete[] data;
  }
  
  return RC::SUCCESS;
}

RC HeapTableEngine::update_record_with_trx(const Record &old_record, const Record &new_record, Trx *trx)
{
  RC rc = RC::SUCCESS;
  
  // 1. 首先删除旧记录在索引中的条目
  rc = delete_entry_of_indexes(old_record.data(), old_record.rid(), false /*error_on_not_exists*/);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete old record from indexes. table name=%s, rid=%s, rc=%s",
              table_meta_->name(), old_record.rid().to_string().c_str(), strrc(rc));
    return rc;
  }
  
  // 2. 使用visit_record更新记录数据
  rc = record_handler_->visit_record(old_record.rid(), [&](Record &record) {
    // 复制新记录数据到当前记录
    memcpy(record.data(), new_record.data(), record.len());
    return true; // 返回true表示记录被修改
  });
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to update record. table name=%s, rid=%s, rc=%s",
              table_meta_->name(), old_record.rid().to_string().c_str(), strrc(rc));
    
    // 回滚：重新插入旧记录到索引
    RC rc2 = insert_entry_of_indexes(old_record.data(), old_record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index entries after update failure. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), old_record.rid().to_string().c_str(), strrc(rc2));
    }
    return rc;
  }
  
  // 3. 插入新记录到索引
  rc = insert_entry_of_indexes(new_record.data(), new_record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to insert new record into indexes. table name=%s, rid=%s, rc=%s",
              table_meta_->name(), new_record.rid().to_string().c_str(), strrc(rc));
    
    // 回滚：恢复旧记录数据和索引
    RC rc2 = record_handler_->visit_record(new_record.rid(), [&](Record &record) {
      memcpy(record.data(), old_record.data(), record.len());
      return true;
    });
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback record data after index update failure. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), new_record.rid().to_string().c_str(), strrc(rc2));
    }
    
    RC rc3 = insert_entry_of_indexes(old_record.data(), old_record.rid());
    if (rc3 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index entries after index update failure. table name=%s, rid=%s, rc=%s",
                table_meta_->name(), old_record.rid().to_string().c_str(), strrc(rc3));
    }
    return rc;
  }
  
  return RC::SUCCESS;
}

RC HeapTableEngine::get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)
{
  scanner = new HeapRecordScanner(table_, *data_buffer_pool_, trx, db_->log_handler(), mode, nullptr);
  RC rc = scanner->open_scan();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC HeapTableEngine::get_chunk_scanner(ChunkFileScanner &scanner, Trx *trx, ReadWriteMode mode)
{
  RC rc = scanner.open_scan_chunk(table_, *data_buffer_pool_, db_->log_handler(), mode);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC HeapTableEngine::create_index(Trx *trx, const std::vector<const FieldMeta *> &field_metas, const char *index_name)
{
  // 1. 参数检查
  if (field_metas.empty() || common::is_blank(index_name)) {
    LOG_INFO("Invalid arguments, field_metas empty or index_name is blank");
    return RC::INVALID_ARGUMENT;
  }

  // 2. 检查是否已存在同名索引
  if (table_meta_->find_index_by_name(index_name) != nullptr) {
    LOG_WARN("Index already exists. table=%s, index_name=%s", table_meta_->name(), index_name);
    return RC::SCHEMA_INDEX_EXIST;
  }

  // 3. 检查是否已存在相同字段组合的索引
  std::vector<std::string> field_names;
  for (const FieldMeta *field : field_metas) {
    field_names.push_back(field->name());
  }
  if (table_meta_->find_index_by_fields(field_names) != nullptr) {
    LOG_WARN("Index with same fields already exists. table=%s", table_meta_->name());
    return RC::SCHEMA_INDEX_EXIST;
  }

  // 4. 创建索引元数据
  IndexMeta new_index_meta;
  RC rc = new_index_meta.init(index_name, field_metas);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to init index meta. table=%s, index_name=%s, rc=%s",
             table_meta_->name(), index_name, strrc(rc));
    return rc;
  }

  // 5. 创建索引文件
  std::string index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_name);
  
  BplusTreeIndex *index = new BplusTreeIndex();
  rc = index->create(table_, index_file.c_str(), new_index_meta, field_metas);
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to create bplus tree index. table=%s, index_name=%s, rc=%s",
             table_meta_->name(), index_name, strrc(rc));
    delete index;
    return rc;
  }

  // 遍历当前的所有数据，插入这个索引
  RecordScanner *scanner = nullptr;
  rc = get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s", 
             table_meta_->name(), index_name, strrc(rc));
    return rc;
  }

  Record record;
  while (OB_SUCC(rc = scanner->next(record))) {
    rc = index->insert_entry(record.data(), &record.rid());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
               table_meta_->name(), index_name, strrc(rc));
      return rc;
    }
  }
  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  } else {
    LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
             table_meta_->name(), index_name, strrc(rc));
    return rc;
  }
  scanner->close_scan();
  delete scanner;
  LOG_INFO("inserted all records into new index. table=%s, index=%s", table_meta_->name(), index_name);

  indexes_.push_back(index);

  /// 接下来将这个索引放到表的元数据中
  TableMeta new_table_meta(*table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, table_meta_->name(), rc, strrc(rc));
    return rc;
  }

  /// 内存中有一份元数据，磁盘文件也有一份元数据。修改磁盘文件时，先创建一个临时文件，写入完成后再rename为正式文件
  /// 这样可以防止文件内容不完整
  // 创建元数据临时文件
  string  tmp_file = table_meta_file(db_->path().c_str(), table_meta_->name()) + ".tmp";
  fstream fs;
  fs.open(tmp_file, ios_base::out | ios_base::binary | ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;  // 创建索引中途出错，要做还原操作
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  // 覆盖原始元数据文件
  string meta_file = table_meta_file(db_->path().c_str(), table_meta_->name());

  int ret = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). "
              "system error=%d:%s",
              tmp_file.c_str(), meta_file.c_str(), index_name, table_meta_->name(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }

  table_meta_->swap(new_table_meta);

  LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, table_meta_->name());
  return rc;
}

RC HeapTableEngine::insert_entry_of_indexes(const char *record, const RID &rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC HeapTableEngine::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

RC HeapTableEngine::sync()
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
          table_meta_->name(),
          index->index_meta().name(),
          rc,
          strrc(rc));
      return rc;
    }
  }

  rc = data_buffer_pool_->flush_all_pages();
  LOG_INFO("Sync table over. table=%s", table_meta_->name());
  return rc;
}

Index *HeapTableEngine::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}
Index *HeapTableEngine::find_index_by_field(const char *field_name) const
{
  const IndexMeta *index_meta = table_meta_->find_index_by_field(field_name);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

Index *HeapTableEngine::find_index_by_fields(const std::vector<std::string> &field_names) const
{
  const IndexMeta *index_meta = table_meta_->find_index_by_fields(field_names);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

RC HeapTableEngine::init()
{
  string data_file = table_data_file(db_->path().c_str(), table_meta_->name());

  BufferPoolManager &bpm = db_->buffer_pool_manager();
  RC                 rc  = bpm.open_file(db_->log_handler(), data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s", data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler(table_meta_->storage_format());

  rc = record_handler_->init(*data_buffer_pool_, db_->log_handler(), table_meta_, table_->lob_handler_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%s", strrc(rc));
    delete record_handler_;
    record_handler_ = nullptr;
    return rc;
  }

  return rc;
}

RC HeapTableEngine::open()
{
  RC rc = RC::SUCCESS;
  init();
  const int index_num = table_meta_->index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_->index(i);
    
    // ⭐ 修改为支持多字段：获取索引的所有字段
    std::vector<const FieldMeta *> field_metas;
    const std::vector<std::string> &field_names = index_meta->fields();
    
    for (const std::string &field_name : field_names) {
      const FieldMeta *field_meta = table_meta_->field(field_name.c_str());
      if (field_meta == nullptr) {
        LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                  table_meta_->name(), index_meta->name(), field_name.c_str());
        // skip cleanup
        //  do all cleanup action in destructive Table function
        return RC::INTERNAL;
      }
      field_metas.push_back(field_meta);
    }

    BplusTreeIndex *index      = new BplusTreeIndex();
    string          index_file = table_index_file(db_->path().c_str(), table_meta_->name(), index_meta->name());

    rc = index->open(table_, index_file.c_str(), *index_meta, field_metas);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                table_meta_->name(), index_meta->name(), index_file.c_str(), strrc(rc));
      // skip cleanup
      //  do all cleanup action in destructive Table function.
      return rc;
    }
    indexes_.push_back(index);
  }
  return rc;
}
