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
// Created by Wangyunlai on 2024/5/31.
//

#pragma once

#include "common/lang/vector.h"
#include "sql/expr/tuple.h"
#include "sql/expr/expression.h"
#include "common/value.h"
#include "common/sys/rc.h"

template <typename ExprPointerType>
class ExpressionTuple : public Tuple
{
public:
  ExpressionTuple(const vector<ExprPointerType> &expressions) : expressions_(expressions) {}
  virtual ~ExpressionTuple() = default;

  void set_tuple(const Tuple *tuple) { child_tuple_ = tuple; }

  int cell_num() const override { return static_cast<int>(expressions_.size()); }

  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }

    const ExprPointerType &expression = expressions_[index];
    return get_value(expression, cell);
  }

  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }

    const ExprPointerType &expression = expressions_[index];
    spec                              = TupleCellSpec(expression->name());
    return RC::SUCCESS;
  }

  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = child_tuple_->find_cell(spec, cell);
      if (OB_SUCC(rc)) {
        return rc;
      }
    }

    rc = RC::NOTFOUND;
    for (const ExprPointerType &expression : expressions_) {
      // 检查完整的字段规范匹配：表名+字段名或别名
      if (expression->type() == ExprType::FIELD) {
        // 如果是字段表达式，检查表名和字段名是否匹配
        Expression *base_expr = get_expression_pointer(expression);
        FieldExpr *field_expr = static_cast<FieldExpr *>(base_expr);
        const Field &field = field_expr->field();
        
        // 检查表名和字段名是否匹配
        if ((spec.table_name() == nullptr || 0 == strcmp(spec.table_name(), field.table_name())) &&
            0 == strcmp(spec.field_name(), field.field_name())) {
          rc = get_value(expression, cell);
          break;
        }
      }
      
      // 检查别名是否匹配
      if (spec.alias() != nullptr && 0 == strcmp(spec.alias(), expression->name())) {
        rc = get_value(expression, cell);
        break;
      }
    }

    return rc;
  }

private:
  RC get_value(const ExprPointerType &expression, Value &value) const
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = expression->get_value(*child_tuple_, value);
    } else {
      rc = expression->try_get_value(value);
    }
    return rc;
  }

  // 辅助函数，用于处理原始指针和智能指针
  Expression *get_expression_pointer(const Expression *expr) const
  {
    return const_cast<Expression *>(expr);
  }

  template<typename T>
  Expression *get_expression_pointer(const std::unique_ptr<T> &expr) const
  {
    return expr.get();
  }

private:
  const vector<ExprPointerType> &expressions_;
  const Tuple                   *child_tuple_ = nullptr;
};
