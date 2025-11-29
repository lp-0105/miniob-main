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

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"
#include "common/lang/vector.h"
#include "common/lang/memory.h"
#include "common/lang/string.h"

class Table;
class FilterStmt;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  UpdateStmt(Table *table, const char *attribute_name, Value *values, int value_amount, FilterStmt *filter_stmt);
  UpdateStmt(Table *table, const char *attribute_name, unique_ptr<Expression> expression, FilterStmt *filter_stmt);
  // 多字段更新构造函数
  UpdateStmt(Table *table, vector<string> attribute_names, vector<Value> values, FilterStmt *filter_stmt);
  UpdateStmt(Table *table, vector<string> attribute_names, vector<unique_ptr<Expression>> expressions, FilterStmt *filter_stmt);
  ~UpdateStmt() override;

public:
  StmtType type() const override { return StmtType::UPDATE; }
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  Table *table() const { return table_; }
  const char *attribute_name() const { return attribute_name_; }
  Value *values() const { return values_; }
  int value_amount() const { return value_amount_; }
  Expression* expression() const { return expression_.get(); }
  FilterStmt *filter_stmt() const { return filter_stmt_; }
  
  // 多字段更新相关方法
  int field_count() const { return static_cast<int>(attribute_names_.size()); }
  const vector<string> &attribute_names() const { return attribute_names_; }
  const vector<Value> &values_list() const { return values_list_; }
  const vector<unique_ptr<Expression>> &expressions_list() const { return expressions_; }

private:
  Table *table_        = nullptr;
  const char *attribute_name_ = nullptr;  // 保留以兼容现有代码
  Value *values_       = nullptr;         // 保留以兼容现有代码
  int    value_amount_ = 0;               // 保留以兼容现有代码
  unique_ptr<Expression> expression_ = nullptr;  // 保留以兼容现有代码
  
  // 多字段更新相关成员
  vector<string> attribute_names_;   // 要更新的字段名列表
  vector<Value> values_list_;        // 要更新的值列表
  vector<unique_ptr<Expression>> expressions_;  // 表达式更新时的表达式列表
  
  FilterStmt *filter_stmt_ = nullptr;
};