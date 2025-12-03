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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAMES("field_names");

RC IndexMeta::init(const char *name, const std::vector<const FieldMeta *> &fields)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }
  
  if (fields.empty()) {
    LOG_ERROR("Failed to init index, fields is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  fields_.clear();
  for (const FieldMeta *field : fields) {
    if (field == nullptr) {
      LOG_ERROR("Failed to init index, field is null.");
      return RC::INVALID_ARGUMENT;
    }
    fields_.push_back(field->name());
  }
  
  return RC::SUCCESS;
}

const char *IndexMeta::field(int index) const
{
  if (index < 0 || index >= static_cast<int>(fields_.size())) {
    return nullptr;
  }
  return fields_[index].c_str();
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  Json::Value field_names(Json::arrayValue);
  for (const auto &field : fields_) {
    field_names.append(field);
  }
  json_value[FIELD_FIELD_NAMES] = field_names;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value = json_value[FIELD_NAME];
  const Json::Value &fields_value = json_value[FIELD_FIELD_NAMES];
  
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string.");
    return RC::INTERNAL;
  }

  if (!fields_value.isArray() || fields_value.empty()) {
    // 兼容旧格式：尝试读取单字段
    const Json::Value &field_value = json_value["field_name"];
    if (field_value.isString()) {
      const FieldMeta *field = table.field(field_value.asCString());
      if (field == nullptr) {
        LOG_ERROR("Deserialize index [%s]: no such field: %s",
                  name_value.asCString(), field_value.asCString());
        return RC::SCHEMA_FIELD_MISSING;
      }
      std::vector<const FieldMeta *> fields = {field};
      return index.init(name_value.asCString(), fields);
    }
    LOG_ERROR("Index fields is not valid.");
    return RC::INTERNAL;
  }

  std::vector<const FieldMeta *> fields;
  for (int i = 0; i < static_cast<int>(fields_value.size()); i++) {
    const Json::Value &field_name = fields_value[i];
    if (!field_name.isString()) {
      LOG_ERROR("Field name is not a string at index %d", i);
      return RC::INTERNAL;
    }
    
    const FieldMeta *field = table.field(field_name.asCString());
    if (field == nullptr) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s",
                name_value.asCString(), field_name.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(field);
  }

  return index.init(name_value.asCString(), fields);
}

void IndexMeta::desc(std::ostream &os) const 
{ 
  os << "index name=" << name_ << ", fields=[";
  for (size_t i = 0; i < fields_.size(); i++) {
    if (i > 0) os << ", ";
    os << fields_[i];
  }
  os << "]";
}