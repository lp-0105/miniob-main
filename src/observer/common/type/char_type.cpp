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
#include "common/type/date_type.h"
#include "common/value.h"
#include <cstdlib>  // for atoi, atof
#include <cstring>  // for strcmp
#include <climits>  // 添加这个头文件

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS, "left type is not CHARS");
  
  // 处理与 DATES 类型的比较
  if (right.attr_type() == AttrType::DATES) {
    return -DataType::type_instance(AttrType::DATES)->compare(right, left);
  }
  
  // 如果右边也是 CHARS
  if (right.attr_type() == AttrType::CHARS) {
    int ly, lm, ld, ry, rm, rd;
    
    std::string left_str = left.get_string();
    std::string right_str = right.get_string();
    
    // 检查是否看起来像日期格式 (YYYY-M-D 或 YYYY-MM-DD)
    auto looks_like_date = [](const std::string &s) { 
      int y, m, d;
      return sscanf(s.c_str(), "%d-%d-%d", &y, &m, &d) == 3;
    };
    
    bool left_looks_date = looks_like_date(left_str);
    bool right_looks_date = looks_like_date(right_str);
    
    bool left_valid = DateType::parse_date(left_str.c_str(), ly, lm, ld);
    bool right_valid = DateType::parse_date(right_str.c_str(), ry, rm, rd);
    
    // 如果看起来像日期但验证失败 = 无效日期
    if (left_looks_date && !left_valid) {
      return INT_MIN;  // 特殊值表示无效日期
    }
    if (right_looks_date && !right_valid) {
      return INT_MIN;  // 特殊值表示无效日期
    }
    
    // 两边都是有效日期，按日期数值比较
    if (left_valid && right_valid) {
      int left_val = DateType::date_to_int(ly, lm, ld);
      int right_val = DateType::date_to_int(ry, rm, rd);
      if (left_val < right_val) return -1;
      if (left_val > right_val) return 1;
      return 0;
    }
    
    // 普通字符串比较
    return common::compare_string(
        (void *)left.value_.pointer_value_, left.length_,
        (void *)right.value_.pointer_value_, right.length_);
  }
  
  // 其他类型处理
  if (right.attr_type() == AttrType::INTS) {
    int left_int = atoi(left.value_.pointer_value_);
    int right_int = right.get_int();
    return left_int - right_int;
  }
  
  if (right.attr_type() == AttrType::FLOATS) {
    float left_float = (float)atof(left.value_.pointer_value_);
    float right_float = right.get_float();
    if (left_float < right_float) return -1;
    if (left_float > right_float) return 1;
    return 0;
  }
  
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