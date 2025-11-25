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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/filter_stmt.h"

UpdateStmt::UpdateStmt(Table *table, const char *attribute_name, Value *values, int value_amount, FilterStmt *filter_stmt)
    : table_(table), attribute_name_(attribute_name), values_(values), value_amount_(value_amount), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  const char *table_name = update.relation_name.c_str();
  if (nullptr == table_name || strlen(table_name) == 0) {
    LOG_WARN("invalid argument. table name is null or empty");
    return RC::INVALID_ARGUMENT;
  }

  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 检查字段是否存在
  const char *attribute_name = update.attribute_name.c_str();
  if (nullptr == attribute_name || strlen(attribute_name) == 0) {
    LOG_WARN("invalid argument. attribute name is null or empty");
    return RC::INVALID_ARGUMENT;
  }

  const FieldMeta *field_meta = table->table_meta().field(attribute_name);
  if (nullptr == field_meta) {
    LOG_WARN("no such field. table=%s, field_name=%s", table_name, attribute_name);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // 检查值的类型是否匹配字段类型
  const Value &value = update.value;
  if (value.attr_type() != field_meta->type()) {
    // 允许一定的类型转换，比如字符串可以转换为整数或浮点数
    if (value.attr_type() == AttrType::CHARS && 
        (field_meta->type() == AttrType::INTS || field_meta->type() == AttrType::FLOATS)) {
      // 可以尝试转换
    } else if (value.attr_type() == AttrType::INTS && field_meta->type() == AttrType::FLOATS) {
      // 整数可以转换为浮点数
    } else if (value.attr_type() == AttrType::FLOATS && field_meta->type() == AttrType::INTS) {
      // 浮点数可以转换为整数（可能丢失精度）
    } else {
      LOG_WARN("type mismatch. field_type=%d, value_type=%d", field_meta->type(), value.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  // 创建过滤语句
  FilterStmt *filter_stmt = nullptr;
  if (!update.conditions.empty()) {
    RC rc = FilterStmt::create(db, table, nullptr, update.conditions.data(), 
                              static_cast<int>(update.conditions.size()), filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
  }

  // 创建UpdateStmt对象
  Value *values = new Value[1];
  values[0] = value;

  UpdateStmt *update_stmt = new UpdateStmt();
  update_stmt->table_ = table;
  update_stmt->attribute_name_ = strdup(attribute_name);
  update_stmt->values_ = values;
  update_stmt->value_amount_ = 1;
  update_stmt->filter_stmt_ = filter_stmt;

  stmt = update_stmt;
  return RC::SUCCESS;
}
