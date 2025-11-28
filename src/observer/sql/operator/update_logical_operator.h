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
// Created by Longda on 2021/4/13.
//

#pragma once

#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"
#include "sql/expr/expression.h"

class Table;

/**
 * @brief 更新逻辑算子
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount);
  UpdateLogicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table *table() const { return table_; }
  const char *attribute_name() const { return attribute_name_; }
  Value *values() const { return values_; }
  int value_amount() const { return value_amount_; }
  Expression* expression() const { return expression_.get(); }

private:  
  Table *table_ = nullptr;
  const char *attribute_name_ = nullptr;
  Value *values_ = nullptr;
  int value_amount_ = 0;
  unique_ptr<Expression> expression_ = nullptr;  // 表达式更新时的表达式
};