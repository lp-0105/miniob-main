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

#include "sql/executor/update_executor.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include "storage/table/table.h"
#include "storage/record/record_scanner.h"
#include "storage/common/condition_filter.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "common/lang/string.h"
#include "common/lang/vector.h"

RC UpdateExecutor::execute(SQLStageEvent *sql_event)
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

  if (stmt->type() != StmtType::UPDATE) {
    LOG_WARN("invalid argument. stmt type is not update");
    return RC::INVALID_ARGUMENT;
  }

  UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
  Table *table = update_stmt->table();
  if (nullptr == table) {
    LOG_WARN("invalid argument. table is null");
    return RC::INVALID_ARGUMENT;
  }

  const char *attribute_name = update_stmt->attribute_name();
  const FieldMeta *field_meta = table->table_meta().field(attribute_name);
  if (nullptr == field_meta) {
    LOG_WARN("no such field. table=%s, field=%s", table->name(), attribute_name);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // 获取事务
  Trx *trx = session_event->session()->current_trx();
  if (nullptr == trx) {
    LOG_WARN("invalid argument. trx is null");
    return RC::INVALID_ARGUMENT;
  }

  // 创建过滤表达式
  FilterStmt *filter_stmt = update_stmt->filter_stmt();
  CompositeConditionFilter condition_filter;
  
  // 从FilterStmt获取过滤条件
  std::vector<ConditionSqlNode> conditions;
  if (filter_stmt != nullptr) {
    for (const FilterUnit *unit : filter_stmt->filter_units()) {
      ConditionSqlNode condition;
      
      // 设置左操作数表达式
      condition.left_expr = unique_ptr<Expression>(unit->left()->copy().release());
      
      // 设置右操作数表达式
      condition.right_expr = unique_ptr<Expression>(unit->right()->copy().release());
      
      // 设置比较操作符
      condition.comp = unit->comp();
      
      conditions.push_back(std::move(condition));
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
  int updated_count = 0;
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

    // 获取要更新的值
    const Value &value = update_stmt->values()[0];
    // 创建新的记录
    Record new_record;
    char *new_data = (char *)malloc(record.len());
    if (nullptr == new_data) {
      LOG_WARN("failed to allocate memory for new record data");
      scanner->close_scan();
      delete scanner;
      return RC::NOMEM;
    }
    memcpy(new_data, record.data(), record.len());
    new_record.set_data_owner(new_data, record.len());
    new_record.set_rid(record.rid());
    
    // 直接更新新记录中的字段值
    size_t copy_len = field_meta->len();
    const size_t data_len = value.length();
    if (field_meta->type() == AttrType::CHARS) {
      if (copy_len > data_len) {
        copy_len = data_len + 1;
      }
    }
    memcpy(new_data + field_meta->offset(), value.data(), copy_len);
    
    // 使用update_record_with_trx方法更新记录
    rc = table->update_record_with_trx(record, new_record, trx);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record");
      scanner->close_scan();
      delete scanner;
      return rc;
    }
    // 注意：不要手动删除new_data，因为Record对象通过set_data_owner已经获得了所有权
    // 当Record对象析构时，会自动释放内存

    updated_count++;
  }

  scanner->close_scan();
  delete scanner;
  // 返回成功
  return RC::SUCCESS;
}