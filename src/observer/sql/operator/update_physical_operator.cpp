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

#include "sql/operator/update_physical_operator.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/stmt/update_stmt.h"
#include "session/session.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  trx_ = trx;
  
  if (children_.size() != 1) {
    LOG_WARN("update operator must have 1 child");
    return RC::INTERNAL;
  }

  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  // 获取字段索引
  if (is_multi_field_) {
    // 多字段更新
    field_indexes_.resize(attribute_names_.size());
    for (size_t i = 0; i < attribute_names_.size(); i++) {
      const FieldMeta *field_meta = table_->table_meta().field(attribute_names_[i].c_str());
      if (nullptr == field_meta) {
        LOG_WARN("no such field in table: %s.%s", table_->name(), attribute_names_[i].c_str());
        return RC::SCHEMA_FIELD_NOT_EXIST;
      }
      // 存储字段索引 - 通过遍历字段找到索引
      int field_index = -1;
      for (int j = 0; j < table_->table_meta().field_num(); j++) {
        const FieldMeta *temp_field = table_->table_meta().field(j);
        if (temp_field != nullptr && strcmp(temp_field->name(), attribute_names_[i].c_str()) == 0) {
          field_index = j;
          break;
        }
      }
      if (field_index == -1) {
        LOG_WARN("failed to find field index for: %s.%s", table_->name(), attribute_names_[i].c_str());
        return RC::SCHEMA_FIELD_NOT_EXIST;
      }
      field_indexes_[i] = field_index;
    }
  } else {
    // 单字段更新
    const FieldMeta *field_meta = table_->table_meta().field(attribute_name_);
    if (nullptr == field_meta) {
      LOG_WARN("no such field in table: %s.%s", table_->name(), attribute_name_);
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    // 存储字段索引 - 通过遍历字段找到索引
    int field_index = -1;
    for (int j = 0; j < table_->table_meta().field_num(); j++) {
      const FieldMeta *temp_field = table_->table_meta().field(j);
      if (temp_field != nullptr && strcmp(temp_field->name(), attribute_name_) == 0) {
        field_index = j;
        break;
      }
    }
    if (field_index == -1) {
      LOG_WARN("failed to find field index for: %s.%s", table_->name(), attribute_name_);
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    field_index_ = field_index;
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  
  // 获取子算子的记录
  while (RC::SUCCESS == (rc = children_[0]->next())) {
    Tuple *tuple = children_[0]->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current tuple");
      return RC::INTERNAL;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();

    // 使用成员变量中的事务
    Trx *trx = trx_;
    
    if (is_multi_field_) {
      // 多字段更新
      // 创建一个新的记录，包含所有字段的原始数据
      Record new_record;
      new_record.set_rid(record.rid());
      char *new_data = static_cast<char*>(malloc(record.len()));
      if (nullptr == new_data) {
        LOG_WARN("failed to allocate memory for new record");
        return RC::NOMEM;
      }
      memcpy(new_data, record.data(), record.len());
      new_record.set_data(new_data, record.len());
      
      // 逐个更新字段
      for (size_t i = 0; i < attribute_names_.size(); i++) {
          // 直接通过字段名获取字段元数据，而不是使用可能不正确的索引
          const FieldMeta *field_meta = table_->table_meta().field(attribute_names_[i].c_str());
          if (nullptr == field_meta) {
            LOG_WARN("failed to get field meta for %s", attribute_names_[i].c_str());
            free(new_record.data());
            return RC::SCHEMA_FIELD_NOT_EXIST;
          }
        
        if (!expressions_.empty()) {
          // 表达式更新
          // 创建一个记录副本，用于表达式计算，避免锁冲突
          Record expr_record;
          expr_record.set_rid(record.rid());
          char *expr_data = static_cast<char*>(malloc(record.len()));
          if (nullptr == expr_data) {
            LOG_WARN("failed to allocate memory for expression record");
            free(new_record.data());
            return RC::NOMEM;
          }
          memcpy(expr_data, record.data(), record.len());
          expr_record.set_data(expr_data, record.len());
    
          RowTuple expr_tuple;
          expr_tuple.set_record(&expr_record);
          const vector<FieldMeta> *fields = table_->table_meta().field_metas();
          expr_tuple.set_schema(table_, fields);
          
          // 计算表达式值
          Value expr_value;
          RC rc = expressions_[i]->get_value(expr_tuple, expr_value);
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to get expression value");
            free(expr_record.data());
            free(new_record.data());
            return rc;
          }
          
          // 使用Record的set_field方法更新字段
          size_t copy_len = field_meta->len();
          const size_t data_len = expr_value.length();
          if (field_meta->type() == AttrType::CHARS) {
            // 修复CHAR类型字符串截断问题
            if (data_len < static_cast<size_t>(field_meta->len())) {
              copy_len = data_len;
              // 确保字符串以null结尾
              memset(static_cast<char*>(new_record.data()) + field_meta->offset() + data_len, 0, field_meta->len() - data_len);
            } else {
              copy_len = field_meta->len() - 1;  // 保留一个字节给null终止符
              // 确保字符串以null结尾
              static_cast<char*>(new_record.data())[field_meta->offset() + copy_len] = '\0';
            }
            // 直接使用expr_value.data()而不是get_string().c_str()，避免临时对象问题
            const char *src_data = expr_value.data();
            if (src_data != nullptr) {
              memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), src_data, copy_len);
            } else {
              // 如果数据为空，填充零
              memset(static_cast<char*>(new_record.data()) + field_meta->offset(), 0, copy_len);
            }
          } else {
            // 非字符串类型，直接复制数据
            memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), expr_value.data(), copy_len);
          }
          
          free(expr_record.data());
        } else {
          // 常量值更新
          // 使用Record的set_field方法更新字段
          size_t copy_len = field_meta->len();
          
          // 检查Value指针是否为空
          if (values_list_[i] == nullptr) {
            LOG_WARN("null Value pointer for field %s", attribute_names_[i].c_str());
            // 如果Value指针为空，填充零
            memset(static_cast<char*>(new_record.data()) + field_meta->offset(), 0, copy_len);
            continue;
          }
          
          const size_t data_len = values_list_[i][0].length();
          
          // 检查数据指针是否为空
          const char *src_data = values_list_[i][0].data();
          if (src_data == nullptr) {
            LOG_WARN("null data pointer for field %s", attribute_names_[i].c_str());
            // 如果数据为空，填充零
            memset(static_cast<char*>(new_record.data()) + field_meta->offset(), 0, copy_len);
            continue;
          }
          
          if (field_meta->type() == AttrType::CHARS) {
            // 修复CHAR类型字符串截断问题
            if (data_len < static_cast<size_t>(field_meta->len())) {
              copy_len = data_len;
              // 确保字符串以null结尾
              memset(static_cast<char*>(new_record.data()) + field_meta->offset() + data_len, 0, field_meta->len() - data_len);
            } else {
              copy_len = field_meta->len() - 1;  // 保留一个字节给null终止符
              // 确保字符串以null结尾
              static_cast<char*>(new_record.data())[field_meta->offset() + copy_len] = '\0';
            }
            // 直接使用values_list_[i][0].data()而不是get_string().c_str()，避免临时对象问题
            memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), src_data, copy_len);
          } else {
            // 非字符串类型，直接复制数据
            memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), src_data, copy_len);
          }
        }
      }
      
      // 一次性更新整个记录
      rc = table_->update_record_with_trx(trx, record, new_record);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to update record: %s", strrc(rc));
        free(new_record.data());
        return rc;
      }
      
      // 释放内存
      free(new_record.data());
    } else {
      // 单字段更新
      // 创建新的记录
      Record new_record;
      new_record.set_rid(record.rid());
      char *new_data = static_cast<char*>(malloc(record.len()));
      if (nullptr == new_data) {
        LOG_WARN("failed to allocate memory for new record");
        return RC::NOMEM;
      }
      memcpy(new_data, record.data(), record.len());
      new_record.set_data(new_data, record.len());

      // 获取字段元数据
      const FieldMeta *field_meta = table_->table_meta().field(field_index_);

      if (expression_) {
        // 表达式更新
        // 创建一个记录副本，用于表达式计算，避免锁冲突
        Record expr_record;
        expr_record.set_rid(record.rid());
        char *expr_data = static_cast<char*>(malloc(record.len()));
        if (nullptr == expr_data) {
          LOG_WARN("failed to allocate memory for expression record");
          free(new_record.data());
          return RC::NOMEM;
        }
        memcpy(expr_data, record.data(), record.len());
        expr_record.set_data(expr_data, record.len());
  
        RowTuple expr_tuple;
        expr_tuple.set_record(&expr_record);
        const vector<FieldMeta> *fields = table_->table_meta().field_metas();
        expr_tuple.set_schema(table_, fields);
        
        // 获取表达式值
        Value expr_value;
        rc = expression_->get_value(expr_tuple, expr_value);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to get expression value");
          free(expr_record.data());
          free(new_record.data());
          return rc;
        }

        // 使用Record的set_field方法更新字段
        size_t copy_len = field_meta->len();
        const size_t data_len = expr_value.length();
        if (field_meta->type() == AttrType::CHARS) {
          // 修复CHAR类型字符串截断问题
          if (data_len < static_cast<size_t>(field_meta->len())) {
            copy_len = data_len;
            // 确保字符串以null结尾
            memset(static_cast<char*>(new_record.data()) + field_meta->offset() + data_len, 0, field_meta->len() - data_len);
          } else {
            copy_len = field_meta->len() - 1;  // 保留一个字节给null终止符
            // 确保字符串以null结尾
            static_cast<char*>(new_record.data())[field_meta->offset() + copy_len] = '\0';
          }
          // 直接使用expr_value.data()而不是get_string().c_str()，避免临时对象问题
          const char *src_data = expr_value.data();
          if (src_data != nullptr) {
            memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), src_data, copy_len);
          } else {
            // 如果数据为空，填充零
            memset(static_cast<char*>(new_record.data()) + field_meta->offset(), 0, copy_len);
          }
        } else {
          // 非字符串类型，直接复制数据
          memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), expr_value.data(), copy_len);
        }

        free(expr_record.data());
      } else {
          // 常量值更新
          // 使用Record的set_field方法更新字段
          size_t copy_len = field_meta->len();
          const size_t data_len = values_[0].length();
          
          // 检查数据指针是否为空
          const char *src_data = values_[0].data();
          if (src_data == nullptr) {
            LOG_WARN("null data pointer for field %s", attribute_name_);
            // 如果数据为空，填充零
            memset(static_cast<char*>(new_record.data()) + field_meta->offset(), 0, copy_len);
          } else {
            if (field_meta->type() == AttrType::CHARS) {
              // 修复CHAR类型字符串截断问题
              if (data_len < static_cast<size_t>(field_meta->len())) {
                copy_len = data_len;
                // 确保字符串以null结尾
                memset(static_cast<char*>(new_record.data()) + field_meta->offset() + data_len, 0, field_meta->len() - data_len);
              } else {
                copy_len = field_meta->len() - 1;  // 保留一个字节给null终止符
                // 确保字符串以null结尾
                static_cast<char*>(new_record.data())[field_meta->offset() + copy_len] = '\0';
              }
              // 直接使用values_[0].data()而不是get_string().c_str()，避免临时对象问题
              memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), src_data, copy_len);
            } else {
              // 非字符串类型，直接复制数据
              memcpy(static_cast<char*>(new_record.data()) + field_meta->offset(), src_data, copy_len);
            }
          }
        }

        // 更新记录
        rc = table_->update_record_with_trx(trx, record, new_record);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to update record: %s", strrc(rc));
          free(new_record.data());
          return rc;
        }

        free(new_record.data());
    }
  }

  return rc;
}

RC UpdatePhysicalOperator::close()
{
  RC rc = children_[0]->close();
  return rc;
}

Tuple *UpdatePhysicalOperator::current_tuple()
{
  return nullptr;
}

// 单字段更新构造函数（保持向后兼容）
UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount)
    : table_(table), attribute_name_(attribute_name), values_(values), value_amount_(value_amount), is_multi_field_(false)
{
}

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression)
    : table_(table), attribute_name_(attribute_name), expression_(std::move(expression)), is_multi_field_(false)
{
}

// 多字段更新构造函数
UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                                                const std::vector<Value*> &values_list, const std::vector<int> &value_amounts)
    : table_(table), attribute_names_(attribute_names), values_list_(values_list), value_amounts_(value_amounts), is_multi_field_(true)
{
}

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                                                std::vector<unique_ptr<Expression>> &&expressions)
    : table_(table), attribute_names_(attribute_names), expressions_(std::move(expressions)), is_multi_field_(true)
{
}