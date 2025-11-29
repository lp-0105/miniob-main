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

    // 创建新的记录
    Record new_record;
    char *new_data = (char*)malloc(record.len());  // 使用malloc而不是new[]
    if (nullptr == new_data) {
      LOG_WARN("failed to allocate memory. size=%d", record.len());
      scanner->close_scan();
      delete scanner;
      return RC::NOMEM;
    }
    memcpy(new_data, record.data(), record.len());
    new_record.set_data_owner(new_data, record.len());
    new_record.set_rid(record.rid());
    
    // 检查是否为多字段更新
    if (update_stmt->field_count() > 0) {
      // 多字段更新
      const vector<string> &attribute_names = update_stmt->attribute_names();
      const vector<Value> &values_list = update_stmt->values_list();
      const vector<unique_ptr<Expression>> &expressions_list = update_stmt->expressions_list();
      
      // 检查是否为表达式更新
      bool is_expression_update = !expressions_list.empty();
      
      // 逐个更新每个字段
      for (int i = 0; i < update_stmt->field_count(); i++) {
        const char *attribute_name = attribute_names[i].c_str();
        const FieldMeta *field_meta = table->table_meta().field(attribute_name);
        if (nullptr == field_meta) {
          LOG_WARN("no such field. table=%s, field=%s", table->name(), attribute_name);
          scanner->close_scan();
          delete scanner;
          return RC::SCHEMA_FIELD_NOT_EXIST;
        }
        
        // 获取要更新的值
        Value value;
        if (is_expression_update) {
          // 表达式更新 - 需要计算表达式值
          // 在计算表达式前先保存当前记录的数据，避免锁冲突
          char *record_data_copy = (char*)malloc(record.len());
          if (nullptr == record_data_copy) {
            LOG_WARN("failed to allocate memory for record copy");
            scanner->close_scan();
            delete scanner;
            return RC::NOMEM;
          }
          memcpy(record_data_copy, record.data(), record.len());
          
          // 使用RAII确保内存释放
          auto record_copy_deleter = [&record_data_copy]() {
            if (record_data_copy) {
              free(record_data_copy);
            }
          };
          
          // 创建记录副本用于表达式计算
          Record record_copy;
          record_copy.set_data_owner(record_data_copy, record.len());
          record_copy.set_rid(record.rid());
          
          // 创建表达式计算上下文
          std::vector<unique_ptr<Expression>> expressions;
          expressions.push_back(unique_ptr<Expression>(expressions_list[i]->copy()));
          
          // 创建元组和计算上下文
          ProjectTuple project_tuple;
          project_tuple.set_expressions(std::move(expressions));
          
          // 创建记录元组
          RowTuple row_tuple;
          const vector<FieldMeta> *fields = table->table_meta().field_metas();
          row_tuple.set_schema(table, fields);
          row_tuple.set_record(&record_copy);
          
          // 设置计算上下文
          project_tuple.set_tuple(&row_tuple);
          
          // 计算表达式值
          rc = project_tuple.cell_at(0, value);
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to evaluate expression");
            record_copy_deleter(); // 释放内存
            scanner->close_scan();
            delete scanner;
            return rc;
          }
          
          // 释放记录副本内存
          record_copy_deleter();
        } else {
          // 常量值更新
          value = values_list[i];
        }
        
        // 更新新记录中的字段值
        size_t copy_len = field_meta->len();
        const size_t data_len = value.length();
        if (field_meta->type() == AttrType::CHARS) {
          // 修复CHAR类型字符串截断问题
          if (data_len < field_meta->len()) {
            copy_len = data_len;
            // 确保字符串以null结尾
            memset(new_data + field_meta->offset() + data_len, 0, field_meta->len() - data_len);
          } else {
            copy_len = field_meta->len() - 1;  // 保留一个字节给null终止符
            // 确保字符串以null结尾
            new_data[field_meta->offset() + copy_len] = '\0';
          }
        }
        memcpy(new_data + field_meta->offset(), value.data(), copy_len);
      }
    } else {
      // 单字段更新（兼容现有代码）
      const char *attribute_name = update_stmt->attribute_name();
      const FieldMeta *field_meta = table->table_meta().field(attribute_name);
      if (nullptr == field_meta) {
        LOG_WARN("no such field. table=%s, field=%s", table->name(), attribute_name);
        scanner->close_scan();
        delete scanner;
        return RC::SCHEMA_FIELD_NOT_EXIST;
      }
      
      // 获取要更新的值
      Value value;
      if (update_stmt->expression() != nullptr) {
        // 表达式更新 - 需要计算表达式值
        // 在计算表达式前先保存当前记录的数据，避免锁冲突
        char *record_data_copy = (char*)malloc(record.len());
        if (nullptr == record_data_copy) {
          LOG_WARN("failed to allocate memory for record copy");
          scanner->close_scan();
          delete scanner;
          return RC::NOMEM;
        }
        memcpy(record_data_copy, record.data(), record.len());
        
        // 使用RAII确保内存释放
        auto record_copy_deleter = [&record_data_copy]() {
          if (record_data_copy) {
            free(record_data_copy);
          }
        };
        
        // 创建记录副本用于表达式计算
        Record record_copy;
        record_copy.set_data_owner(record_data_copy, record.len());
        record_copy.set_rid(record.rid());
        
        // 创建表达式计算上下文
        std::vector<unique_ptr<Expression>> expressions;
        expressions.push_back(unique_ptr<Expression>(update_stmt->expression()->copy()));
        
        // 创建元组和计算上下文
        ProjectTuple project_tuple;
        project_tuple.set_expressions(std::move(expressions));
        
        // 创建记录元组
        RowTuple row_tuple;
        const vector<FieldMeta> *fields = table->table_meta().field_metas();
        row_tuple.set_schema(table, fields);
        row_tuple.set_record(&record_copy);
        
        // 设置计算上下文
        project_tuple.set_tuple(&row_tuple);
        
        // 计算表达式值
        rc = project_tuple.cell_at(0, value);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to evaluate expression");
          record_copy_deleter(); // 释放内存
          scanner->close_scan();
          delete scanner;
          return rc;
        }
        
        // 释放记录副本内存
        record_copy_deleter();
      } else {
        // 常量值更新
        value = update_stmt->values()[0];
      }
      
      // 更新新记录中的字段值
      size_t copy_len = field_meta->len();
      const size_t data_len = value.length();
      if (field_meta->type() == AttrType::CHARS) {
        // 修复CHAR类型字符串截断问题
        if (data_len < field_meta->len()) {
          copy_len = data_len;
          // 确保字符串以null结尾
          memset(new_data + field_meta->offset() + data_len, 0, field_meta->len() - data_len);
        } else {
          copy_len = field_meta->len() - 1;  // 保留一个字节给null终止符
          // 确保字符串以null结尾
          new_data[field_meta->offset() + copy_len] = '\0';
        }
      }
      memcpy(new_data + field_meta->offset(), value.data(), copy_len);
    }
    
    // 使用update_record_with_trx方法更新记录
    rc = table->update_record_with_trx(trx, record, new_record);
    // Don't delete new_data here - it will be deleted by new_record's destructor
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record");
      scanner->close_scan();
      delete scanner;
      return rc;
    }

    updated_count++;
  }

  scanner->close_scan();
  delete scanner;
  // 返回成功
  return RC::SUCCESS;
}