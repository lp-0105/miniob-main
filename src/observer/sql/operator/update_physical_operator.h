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

#include "sql/operator/physical_operator.h"
#include "storage/field/field.h"
#include "sql/expr/expression.h"
#include <vector>
#include <string>

class Table;
class Trx;

/**
 * @brief 更新物理算子
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  // 单字段更新构造函数（保持向后兼容）
  UpdatePhysicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount);
  UpdatePhysicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression);
  
  // 多字段更新构造函数
  UpdatePhysicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                         const std::vector<Value*> &values_list, const std::vector<int> &value_amounts);
  UpdatePhysicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                         std::vector<unique_ptr<Expression>> &&expressions);
  
  virtual ~UpdatePhysicalOperator() = default;

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;
  
  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

private:
  Table *table_ = nullptr;
  
  // 单字段更新成员（保持向后兼容）
  const char *attribute_name_ = nullptr;
  Value *values_ = nullptr;
  int value_amount_ = 0;
  unique_ptr<Expression> expression_ = nullptr;  // 表达式更新时的表达式
  int field_index_ = -1;  // 要更新的字段在表中的索引
  
  // 多字段更新成员
  std::vector<std::string> attribute_names_;
  std::vector<Value*> values_list_;
  std::vector<int> value_amounts_;
  std::vector<unique_ptr<Expression>> expressions_;
  std::vector<int> field_indexes_;
  
  bool is_multi_field_ = false;
  Trx *trx_ = nullptr;
};