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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"
#include "sql/operator/join_physical_operator.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */

/**
 * @brief 表示一个JOIN表
 * @ingroup Statement
 */
struct JoinTable
{
  Table *table;                          ///< 表
  vector<::JoinCondition> join_conditions; ///< JOIN条件列表
};

class SelectStmt : public Stmt
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }

public:
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt);

public:
  const vector<Table *> &tables() const { return tables_; }
  FilterStmt            *filter_stmt() const { return filter_stmt_; }
  const vector<FilterStmt *> &join_filter_stmts() const { return join_filter_stmts_; }
  const vector<JoinTable> &join_tables() const { return join_tables_; }

  vector<unique_ptr<Expression>> &query_expressions() { return query_expressions_; }
  vector<unique_ptr<Expression>> &group_by() { return group_by_; }

private:
  vector<unique_ptr<Expression>> query_expressions_;
  vector<Table *>                tables_;
  FilterStmt                    *filter_stmt_ = nullptr;
  vector<FilterStmt *>           join_filter_stmts_;
  vector<unique_ptr<Expression>> group_by_;
  vector<JoinTable>              join_tables_; ///< JOIN表列表
};
