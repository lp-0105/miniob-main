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
#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/lang/unordered_map.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/filter_stmt.h"

UpdateStmt::UpdateStmt(Table *table, const char *attribute_name, Value *values, int value_amount, FilterStmt *filter_stmt)
    : table_(table), attribute_name_(attribute_name), values_(values), value_amount_(value_amount), filter_stmt_(filter_stmt)
{}

UpdateStmt::UpdateStmt(Table *table, const char *attribute_name, unique_ptr<Expression> expression, FilterStmt *filter_stmt)
    : table_(table), attribute_name_(attribute_name), expression_(std::move(expression)), filter_stmt_(filter_stmt)
{}

UpdateStmt::UpdateStmt(Table *table, vector<string> attribute_names, vector<Value> values, FilterStmt *filter_stmt)
    : table_(table), attribute_names_(attribute_names), values_list_(values), filter_stmt_(filter_stmt)
{}

UpdateStmt::UpdateStmt(Table *table, vector<string> attribute_names, vector<unique_ptr<Expression>> expressions, FilterStmt *filter_stmt)
    : table_(table), attribute_names_(attribute_names), expressions_(std::move(expressions)), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
  
  if (nullptr != values_) {
    delete[] values_;
    values_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt)
{
  const char *table_name = update_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", 
             db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // create filter statement
  FilterStmt *filter_stmt = nullptr;
  unordered_map<string, Table *> table_map;
  table_map[table->name()] = table;
  
  RC rc = FilterStmt::create(db, table, &table_map, 
                            update_sql.conditions.empty() ? nullptr : &update_sql.conditions[0], 
                            static_cast<int>(update_sql.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // 检查是否为多字段更新
  if (!update_sql.attribute_names.empty()) {
    // 多字段更新
    vector<string> attribute_names = update_sql.attribute_names;
    
    // 检查所有字段是否存在
    for (const auto &attr_name : attribute_names) {
      const FieldMeta *field_meta = table->table_meta().field(attr_name.c_str());
      if (nullptr == field_meta) {
        LOG_WARN("no such field. table=%s, field=%s", table_name, attr_name.c_str());
        return RC::SCHEMA_FIELD_NOT_EXIST;
      }
    }
    
    // 检查是否为表达式更新
    if (!update_sql.expressions.empty()) {
      // 表达式更新
      vector<unique_ptr<Expression>> expressions;
      for (auto &expr : update_sql.expressions) {
        if (expr) {
          expressions.push_back(std::move(const_cast<unique_ptr<Expression>&>(expr)));
        } else {
          LOG_WARN("null expression in multi-field update");
          return RC::INVALID_ARGUMENT;
        }
      }
      stmt = new UpdateStmt(table, attribute_names, std::move(expressions), filter_stmt);
    } else if (!update_sql.values.empty()) {
      // 常量值更新
      vector<Value> values = update_sql.values;
      stmt = new UpdateStmt(table, attribute_names, values, filter_stmt);
    } else {
      // 从attribute_names中提取字段名和值/表达式
      // 这里需要根据解析器的实现来处理
      LOG_WARN("invalid multi-field update. no values or expressions provided");
      return RC::INVALID_ARGUMENT;
    }
  } else {
    // 单字段更新（兼容现有代码）
    // check whether the attribute exists
    const FieldMeta *field_meta = table->table_meta().field(update_sql.attribute_name.c_str());
    if (nullptr == field_meta) {
      LOG_WARN("no such field. table=%s, field=%s", table_name, update_sql.attribute_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    // create update statement
    if (update_sql.is_expression) {
      // 表达式更新
      if (update_sql.expression) {
        stmt = new UpdateStmt(table, update_sql.attribute_name.c_str(), 
                              std::move(const_cast<UpdateSqlNode&>(update_sql).expression), 
                              filter_stmt);
      } else {
        LOG_WARN("null expression in single-field update");
        return RC::INVALID_ARGUMENT;
      }
    } else {
      // 常量值更新
      Value *values = new Value[1];
      values[0] = update_sql.value;
      stmt = new UpdateStmt(table, update_sql.attribute_name.c_str(), values, 1, filter_stmt);
    }
  }

  return rc;
}