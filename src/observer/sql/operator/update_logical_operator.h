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
// Created by Assistant on 2025/01/25.
//

#pragma once

#include "sql/operator/logical_operator.h"
#include "storage/table/table.h"
#include "sql/operator/update_unit.h"
#include <vector>
#include <memory>

/**
 * @brief 更新逻辑操作符
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<UpdateUnit> update_units, std::unique_ptr<Expression> condition);
  ~UpdateLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }

  Table *table() const { return table_; }
  const std::vector<UpdateUnit> &update_units() const { return update_units_; }
  Expression *condition() const { return condition_.get(); }

private:
  Table *table_ = nullptr;
  std::vector<UpdateUnit> update_units_;
  std::unique_ptr<Expression> condition_;
};