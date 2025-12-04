/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/expr/expression.h"
#include <stdio.h>

FilterStmt::~FilterStmt()
{
  for (FilterUnit *unit : filter_units_) {
    delete unit;
  }
  filter_units_.clear();
}

RC FilterStmt::create(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;

  FilterStmt *tmp_stmt = new FilterStmt();
  
  for (int i = 0; i < condition_num; i++) {
    FilterUnit *filter_unit = nullptr;
    rc = create_filter_unit(db, default_table, tables, conditions[i], filter_unit);
    
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      return rc;
    }
    
    tmp_stmt->filter_units_.push_back(filter_unit);
  }

  stmt = tmp_stmt;
  return RC::SUCCESS;
}

// 递归绑定表达式中的字段
static Expression* bind_expression(Expression *expr, Table *default_table, unordered_map<string, Table *> *tables)
{
  if (expr == nullptr) {
    return nullptr;
  }
  
  // 如果是未绑定的字段表达式，需要绑定到实际的表和字段
  if (expr->type() == ExprType::UNBOUND_FIELD) {
    UnboundFieldExpr *unbound_expr = static_cast<UnboundFieldExpr *>(expr);
    const char *table_name = unbound_expr->table_name();
    const char *field_name = unbound_expr->field_name();
    
    Table *table = nullptr;
    if (table_name == nullptr || strlen(table_name) == 0) {
      // 没有指定表名，使用默认表
      table = default_table;
    } else if (tables != nullptr) {
      auto iter = tables->find(table_name);
      if (iter != tables->end()) {
        table = iter->second;
      }
    }
    
    if (table == nullptr) {
      LOG_WARN("No such table: %s", table_name ? table_name : "default");
      return nullptr;
    }
    
    const FieldMeta *field_meta = table->table_meta().field(field_name);
    if (field_meta == nullptr) {
      LOG_WARN("No such field: %s in table %s", field_name, table->name());
      return nullptr;
    }
    
    // 创建绑定后的 FieldExpr
    return new FieldExpr(table, field_meta);
  }
  
  // 如果是算术表达式，需要递归绑定子表达式
  if (expr->type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(expr);
    
    Expression *new_left = nullptr;
    Expression *new_right = nullptr;
    
    if (arith_expr->left()) {
      new_left = bind_expression(arith_expr->left().get(), default_table, tables);
      if (new_left == nullptr) {
        return nullptr;  // 绑定失败
      }
    }
    
    if (arith_expr->right()) {
      new_right = bind_expression(arith_expr->right().get(), default_table, tables);
      if (new_right == nullptr) {
        if (new_left) delete new_left;
        return nullptr;  // 绑定失败
      }
    }
    
    // 创建新的算术表达式
    unique_ptr<Expression> left_ptr(new_left);
    unique_ptr<Expression> right_ptr(new_right);
    
    ArithmeticExpr *new_expr = new ArithmeticExpr(arith_expr->arithmetic_type(), 
                                                   std::move(left_ptr), 
                                                   std::move(right_ptr));
    new_expr->set_name(arith_expr->name());
    return new_expr;
  }
  
  // 其他类型的表达式（如 ValueExpr）直接复制
  return expr->copy().release();
}

RC FilterStmt::create_filter_unit(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    const ConditionSqlNode &condition, FilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;
  
  Expression *left_expr = bind_expression(condition.left_expr.get(), default_table, tables);
  Expression *right_expr = bind_expression(condition.right_expr.get(), default_table, tables);
  
  if (left_expr == nullptr || right_expr == nullptr) {
    if (left_expr) delete left_expr;
    if (right_expr) delete right_expr;
    LOG_WARN("Failed to bind expression in filter condition");
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }
  
  CompOp comp = condition.comp;
  
  filter_unit = new FilterUnit;
  filter_unit->set_comp(comp);
  filter_unit->set_left(left_expr);
  filter_unit->set_right(right_expr);
  
  return rc;
}
