/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include <cstdlib>  // for atoi, atof
#include <cstring>  // for strcmp

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS, "left type is not CHARS");
  
  // 如果右边也是字符串，直接比较
  if (right.attr_type() == AttrType::CHARS) {
    return common::compare_string(
        (void *)left.value_.pointer_value_, left.length_,
        (void *)right.value_.pointer_value_, right.length_);
  }
  
  // ⭐ 如果右边是整数，将字符串转换为整数进行比较
  if (right.attr_type() == AttrType::INTS) {
    int left_int = atoi(left.value_.pointer_value_);  // 字符串转整数
    int right_int = right.get_int();
    return left_int - right_int;
  }
  
  // ⭐ 如果右边是浮点数，将字符串转换为浮点数进行比较
  if (right.attr_type() == AttrType::FLOATS) {
    float left_float = (float)atof(left.value_.pointer_value_);
    float right_float = right.get_float();
    if (left_float < right_float) return -1;
    if (left_float > right_float) return 1;
    return 0;
  }
  
  // 其他类型，转为字符串比较
  return strcmp(left.value_.pointer_value_, right.to_string().c_str());
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}