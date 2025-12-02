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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"
#include "sql/operator/join_physical_operator.h"

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }

  for (FilterStmt *filter_stmt : join_filter_stmts_) {
    if (nullptr != filter_stmt) {
      delete filter_stmt;
    }
  }
  join_filter_stmts_.clear();
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
  }

  // collect join tables
  vector<JoinTable> join_tables;
  for (const auto &join_table_sql : select_sql.join_tables) {
    const char *table_name = join_table_sql.table_name.c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. join table name is null");
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    // tables.push_back(table);  // ⭐ 注释掉这行！JOIN表不要加到tables中
    table_map.insert({table_name, table});

    JoinTable join_table;
    join_table.table = table;

    // process join conditions
    for (const auto &join_condition_sql : join_table_sql.join_conditions) {
      
      JoinCondition join_condition;
      join_condition.comp = join_condition_sql.comp;

      // 处理左操作数
      if (join_condition_sql.left_is_attr) {
        // 左操作数是字段
        join_condition.left_is_attr = 1;
        join_condition.left_table = join_condition_sql.left_relation;
        join_condition.left_field = join_condition_sql.left_attribute;
        
        // 验证左表是否存在
        auto left_table_it = table_map.find(join_condition_sql.left_relation);
        if (left_table_it == table_map.end()) {
          LOG_WARN("left table not found in join condition: %s", join_condition_sql.left_relation.c_str());
          return RC::SCHEMA_TABLE_NOT_EXIST;
        }
      } else {
        // 左操作数是常量值
        join_condition.left_is_attr = 0;
        join_condition.left_value = join_condition_sql.left_value;
      }

      // 处理右操作数
      if (join_condition_sql.right_is_attr) {
        // 右操作数是字段
        join_condition.right_is_attr = 1;
        join_condition.right_table = join_condition_sql.right_relation;
        join_condition.right_field = join_condition_sql.right_attribute;
        
        // 验证右表是否存在
        auto right_table_it = table_map.find(join_condition_sql.right_relation);
        if (right_table_it == table_map.end()) {
          LOG_WARN("right table not found in join condition: %s", join_condition_sql.right_relation.c_str());
          return RC::SCHEMA_TABLE_NOT_EXIST;
        }
      } else {
        // 右操作数是常量值
        join_condition.right_is_attr = 0;
        join_condition.right_value = join_condition_sql.right_value;
      }

      join_table.join_conditions.push_back(join_condition);
    }

    join_tables.push_back(join_table);
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);
  
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // 将JOIN条件合并到FilterStmt中
  // 遍历所有JOIN表，将JOIN条件转换为ConditionSqlNode并添加到FilterStmt中
  vector<ConditionSqlNode> all_conditions(select_sql.conditions);
  
  for (const auto &join_table_sql : select_sql.join_tables) {
    for (const auto &join_condition_sql : join_table_sql.join_conditions) {
      ConditionSqlNode condition;
      condition.comp = join_condition_sql.comp;
      
      // 处理左操作数 - 使用 left_is_attr 标志位判断
      if (join_condition_sql.left_is_attr) {
        condition.left_is_attr = 1;
        condition.left_attr.relation_name = join_condition_sql.left_relation;
        condition.left_attr.attribute_name = join_condition_sql.left_attribute;
      } else {
        condition.left_is_attr = 0;
        condition.left_value = join_condition_sql.left_value;
      }
      
      // 处理右操作数 - 使用 right_is_attr 标志位判断
      if (join_condition_sql.right_is_attr) {
        condition.right_is_attr = 1;
        condition.right_attr.relation_name = join_condition_sql.right_relation;
        condition.right_attr.attribute_name = join_condition_sql.right_attribute;
      } else {
        condition.right_is_attr = 0;
        condition.right_value = join_condition_sql.right_value;
      }
      
      all_conditions.push_back(condition);
    }
  }
  
  // 重新创建包含JOIN条件的FilterStmt
  if (filter_stmt != nullptr) {
    delete filter_stmt;
    filter_stmt = nullptr;
  }
  
  rc = FilterStmt::create(db,
      default_table,
      &table_map,
      all_conditions.data(),
      static_cast<int>(all_conditions.size()),
      filter_stmt);
  
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt with join conditions");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->join_tables_.swap(join_tables);
  stmt = select_stmt;
  return RC::SUCCESS;
}
