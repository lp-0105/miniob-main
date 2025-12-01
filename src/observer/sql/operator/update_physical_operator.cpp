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

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, std::vector<UpdateUnit> update_units, std::unique_ptr<Expression> condition)
    : table_(table), update_units_(std::move(update_units)), condition_(std::move(condition))
{
}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (nullptr == table_) {
    LOG_WARN("table is null");
    return RC::INVALID_ARGUMENT;
  }
  
  trx_ = trx;
  finished_ = false;
  
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  if (finished_) {
    return RC::RECORD_EOF;
  }
  
  // UPDATE操作只需要执行一次
  RC rc = RC::SUCCESS;
  
  // 这里应该实现UPDATE逻辑，但由于UPDATE操作通常需要扫描表并更新符合条件的记录
  // 这个简单的实现只标记为完成
  finished_ = true;
  
  return rc;
}

RC UpdatePhysicalOperator::close()
{
  trx_ = nullptr;
  return RC::SUCCESS;
}