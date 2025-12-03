/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "common/type/date_type.h"

using namespace std;

InsertStmt::InsertStmt(Table *table, const vector<Value> &values)
    : table_(table), values_(values)
{}

RC InsertStmt::create(Db *db, const InsertSqlNode &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  const int        value_num  = static_cast<int>(inserts.values.size());
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // 类型检查和转换
  vector<Value> converted_values;
  converted_values.reserve(value_num);
  
  const int sys_field_num = table_meta.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    if (nullptr == field_meta) {
      LOG_WARN("field meta is null. index=%d", i);
      return RC::INTERNAL;
    }
    
    AttrType field_type = field_meta->type();
    AttrType value_type = inserts.values[i].attr_type();
    
    // 处理 DATE 类型
    if (field_type == AttrType::DATES) {
      if (value_type == AttrType::CHARS) {
        // 字符串转日期
        const char *date_str = inserts.values[i].data();
        Value date_value;
        RC rc = DataType::type_instance(AttrType::DATES)->set_value_from_str(date_value, string(date_str));
        if (rc != RC::SUCCESS) {
          LOG_WARN("invalid date format or invalid date: %s", date_str);
          return RC::INVALID_ARGUMENT;
        }
        converted_values.push_back(date_value);
      } else if (value_type == AttrType::DATES) {
        // 已经是 DATE 类型
        converted_values.push_back(inserts.values[i]);
      } else {
        LOG_WARN("field type mismatch. field=%s, field_type=%d, value_type=%d",
            field_meta->name(), static_cast<int>(field_type), static_cast<int>(value_type));
        return RC::INVALID_ARGUMENT;
      }
    } else {
      // 其他类型直接使用
      converted_values.push_back(inserts.values[i]);
    }
  }

  // everything alright
  stmt = new InsertStmt(table, converted_values);
  return RC::SUCCESS;
}
