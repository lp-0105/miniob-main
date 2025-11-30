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
#include <vector>
#include <string>

class Table;

/**
 * @brief 更新逻辑算子
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  // 单字段更新构造函数（保持向后兼容）
  UpdateLogicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount);
  UpdateLogicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression);
  
  // 多字段更新构造函数
  UpdateLogicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                       const std::vector<Value*> &values_list, const std::vector<int> &value_amounts);
  UpdateLogicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                       std::vector<unique_ptr<Expression>> &&expressions);
  
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table *table() const { return table_; }
  
  // 单字段更新接口（保持向后兼容）
  const char *attribute_name() const { return attribute_name_; }
  Value *values() const { return values_; }
  int value_amount() const { return value_amount_; }
  Expression* expression() const { return expression_.get(); }
  
  // 多字段更新接口
  const std::vector<std::string> &attribute_names() const { return attribute_names_; }
  const std::vector<Value*> &values_list() const { return values_list_; }
  const std::vector<int> &value_amounts() const { return value_amounts_; }
  const std::vector<unique_ptr<Expression>> &expressions() const { return expressions_; }
  
  // 判断是否为多字段更新
  bool is_multi_field() const { return is_multi_field_; }

private:  
  Table *table_ = nullptr;
  
  // 单字段更新成员（保持向后兼容）
  const char *attribute_name_ = nullptr;
  Value *values_ = nullptr;
  int value_amount_ = 0;
  unique_ptr<Expression> expression_ = nullptr;  // 表达式更新时的表达式
  
  // 多字段更新成员
  std::vector<std::string> attribute_names_;
  std::vector<Value*> values_list_;
  std::vector<int> value_amounts_;
  std::vector<unique_ptr<Expression>> expressions_;
  
  bool is_multi_field_ = false;
};