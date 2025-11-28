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
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/value.h"
#include "sql/expr/expression.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("update operator must have 1 child");
    return RC::INTERNAL;
  }

  trx_ = trx;
  
  // 查找要更新的字段在表中的索引
  const TableMeta &table_meta = table_->table_meta();
  const FieldMeta *field_meta = table_meta.field(attribute_name_);
  if (nullptr == field_meta) {
    LOG_WARN("no such field. field=%s.%s", table_->name(), attribute_name_);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }
  
  // 计算字段索引
  const vector<FieldMeta> *fields = table_meta.field_metas();
  for (size_t i = 0; i < fields->size(); i++) {
    if (strcmp((*fields)[i].name(), field_meta->name()) == 0) {
      field_index_ = i;
      break;
    }
  }
  
  // 确保找到了字段索引
  if (field_index_ < 0) {
    LOG_WARN("failed to get field index. field=%s.%s", table_->name(), attribute_name_);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // 打开子算子
  return children_[0]->open(trx);
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  
  // 从子算子获取要更新的记录
  while (RC::SUCCESS == (rc = children_[0]->next())) {
    Tuple *tuple = children_[0]->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current tuple");
      return RC::INTERNAL;
    }
    
    // 获取记录ID
    RowTuple *row_tuple = dynamic_cast<RowTuple*>(tuple);
    if (row_tuple == nullptr) {
      LOG_WARN("failed to get row tuple");
      return RC::INTERNAL;
    }
    Record old_record = row_tuple->record();
    
    // 创建新记录
    Record new_record;
    char *new_data = (char*)malloc(old_record.len());
    if (nullptr == new_data) {
      LOG_WARN("failed to allocate memory for new record");
      return RC::NOMEM;
    }
    memcpy(new_data, old_record.data(), old_record.len());
    new_record.set_data_owner(new_data, old_record.len());
    new_record.set_rid(old_record.rid());
    
    // 准备新的值
    Value new_value;
    if (expression_ != nullptr) {
      // 表达式更新
      rc = expression_->get_value(*tuple, new_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to evaluate expression");
        // 不需要手动释放new_data，new_record析构时会自动释放
        return rc;
      }
    } else {
      // 常量值更新
      if (value_amount_ != 1) {
        LOG_WARN("update value amount should be 1");
        // 不需要手动释放new_data，new_record析构时会自动释放
        return RC::INVALID_ARGUMENT;
      }
      new_value = values_[0];
    }
    
    // 检查类型是否匹配
    const FieldMeta *field_meta = table_->table_meta().field(attribute_name_);
    if (field_meta->type() != new_value.attr_type()) {
      // 尝试类型转换
      Value converted_value;
      if (Value::cast_to(new_value, field_meta->type(), converted_value) != RC::SUCCESS) {
        LOG_WARN("type mismatch. field type=%d, value type=%d", 
                field_meta->type(), new_value.attr_type());
        // 不需要手动释放new_data，new_record析构时会自动释放
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      new_value = converted_value;
    }
    
    // 更新记录中的字段
    memcpy(new_record.data() + field_meta->offset(), new_value.data(), field_meta->len());
    
    // 更新记录
    rc = table_->update_record_with_trx(trx_, old_record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record. rc=%s", strrc(rc));
      // 不需要手动释放new_data，new_record析构时会自动释放
      return rc;
    }
    
    // 成功更新后，new_record的析构函数会自动释放new_data
  }
  
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *UpdatePhysicalOperator::current_tuple()
{
  return nullptr;
}

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount)
  : table_(table), attribute_name_(attribute_name), values_(values), value_amount_(value_amount)
{
}

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression)
  : table_(table), attribute_name_(attribute_name), expression_(std::move(expression))
{
}