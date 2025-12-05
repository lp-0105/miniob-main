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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}
 
RC CountAggregator::accumulate(const Value &value)
{
  count_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value& result)
{
  result = Value(count_);
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  
  // 安全地获取数值
  switch (value.attr_type()) {
    case AttrType::INTS:
      sum_ += static_cast<float>(value.get_int());
      break;
    case AttrType::FLOATS:
      sum_ += value.get_float();
      break;
    default:
      return RC::SUCCESS;  // 忽略其他类型
  }
  count_++;
  has_value_ = true;
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result)
{
  if (!has_value_ || count_ == 0) {
    result.set_null();
  } else {
    result.set_float(sum_ / static_cast<float>(count_));
  }
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  if (!has_value_) {
    max_value_ = value;
    has_value_ = true;
  } else if (value.compare(max_value_) > 0) {
    max_value_ = value;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result)
{
  if (!has_value_) {
    result.set_null();
  } else {
    result = max_value_;
  }
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  if (!has_value_) {
    min_value_ = value;
    has_value_ = true;
  } else if (value.compare(min_value_) < 0) {
    min_value_ = value;
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result)
{
  if (!has_value_) {
    result.set_null();
  } else {
    result = min_value_;
  }
  return RC::SUCCESS;
}
