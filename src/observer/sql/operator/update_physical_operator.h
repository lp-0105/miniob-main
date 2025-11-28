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

class Table;
class Trx;

/**
 * @brief 更新物理算子
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount);
  UpdatePhysicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression);
  virtual ~UpdatePhysicalOperator() = default;

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;
  
  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

private:
  Table *table_ = nullptr;
  const char *attribute_name_ = nullptr;
  Value *values_ = nullptr;
  int value_amount_ = 0;
  unique_ptr<Expression> expression_ = nullptr;  // 表达式更新时的表达式
  Trx *trx_ = nullptr;
  int field_index_ = -1;  // 要更新的字段在表中的索引
};