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
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/executor/delete_executor.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include "storage/table/table.h"
#include "storage/record/record_scanner.h"
#include "storage/common/condition_filter.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "common/lang/string.h"
#include "common/lang/vector.h"

RC DeleteExecutor::execute(SQLStageEvent *sql_event)
{
  if (nullptr == sql_event) {
    LOG_WARN("invalid argument. sql_event is null");
    return RC::INVALID_ARGUMENT;
  }

  SessionEvent *session_event = sql_event->session_event();
  if (nullptr == session_event) {
    LOG_WARN("invalid argument. session_event is null");
    return RC::INVALID_ARGUMENT;
  }

  Stmt *stmt = sql_event->stmt();
  if (nullptr == stmt) {
    LOG_WARN("invalid argument. stmt is null");
    return RC::INVALID_ARGUMENT;
  }

  if (stmt->type() != StmtType::DELETE) {
    LOG_WARN("invalid argument. stmt type is not delete");
    return RC::INVALID_ARGUMENT;
  }

  DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);
  Table *table = delete_stmt->table();
  if (nullptr == table) {
    LOG_WARN("invalid argument. table is null");
    return RC::INVALID_ARGUMENT;
  }

  // 获取事务
  Trx *trx = session_event->session()->current_trx();
  if (nullptr == trx) {
    LOG_WARN("invalid argument. trx is null");
    return RC::INVALID_ARGUMENT;
  }

  // 创建过滤表达式
  FilterStmt *filter_stmt = delete_stmt->filter_stmt();
  CompositeConditionFilter condition_filter;
  
  // 从FilterStmt获取过滤条件
  std::vector<ConditionSqlNode> conditions;
  if (filter_stmt != nullptr) {
    for (const FilterUnit *unit : filter_stmt->filter_units()) {
      ConditionSqlNode condition;
      
      // 设置左操作数
      const FilterObj &left = unit->left();
      if (left.is_attr) {
        condition.left_is_attr = 1;
        condition.left_attr.relation_name = left.field.table()->name();
        condition.left_attr.attribute_name = left.field.field_name();
      } else {
        condition.left_is_attr = 0;
        condition.left_value = left.value;
      }
      
      // 设置右操作数
      const FilterObj &right = unit->right();
      if (right.is_attr) {
        condition.right_is_attr = 1;
        condition.right_attr.relation_name = right.field.table()->name();
        condition.right_attr.attribute_name = right.field.field_name();
      } else {
        condition.right_is_attr = 0;
        condition.right_value = right.value;
      }
      
      // 设置比较操作符
      condition.comp = unit->comp();
      
      conditions.push_back(condition);
    }
  }
  
  RC rc = condition_filter.init(*table, conditions.data(), static_cast<int>(conditions.size()));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init condition filter");
    return rc;
  }

  // 扫描表中的记录
  RecordScanner *scanner = nullptr;
  rc = table->get_record_scanner(scanner, trx, ReadWriteMode::READ_WRITE);
  if (rc != RC::SUCCESS || nullptr == scanner) {
    LOG_WARN("failed to get record scanner");
    return rc;
  }

  Record record;
  int deleted_count = 0;
  while (OB_SUCC(rc = scanner->next(record))) {
    // 检查记录是否满足过滤条件
    bool filter_result = condition_filter.filter(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to filter record");
      scanner->close_scan();
      delete scanner;
      return rc;
    }

    if (!filter_result) {
      continue; // 不满足条件，跳过
    }

    // 使用delete_record_with_trx方法删除记录
    rc = table->delete_record_with_trx(record, trx);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record");
      scanner->close_scan();
      delete scanner;
      return rc;
    }

    deleted_count++;
  }

  scanner->close_scan();
  delete scanner;
  // 返回成功
  return RC::SUCCESS;
}