/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/type/char_type.h"
#include "common/type/date_type.h"
#include "common/type/float_type.h"
#include "common/type/integer_type.h"
#include "common/type/data_type.h"
#include "common/type/vector_type.h"
#include "common/value.h"

// Todo: 实现新数据类型
// your code here

array<unique_ptr<DataType>, static_cast<int>(AttrType::MAXTYPE)> DataType::type_instances_ = {
    make_unique<DataType>(AttrType::UNDEFINED),
    make_unique<CharType>(),
    make_unique<IntegerType>(),
    make_unique<FloatType>(),
    make_unique<DateType>(),
    make_unique<VectorType>(),
    make_unique<DataType>(AttrType::BOOLEANS),
    make_unique<NullType>(), // ⭐ 使用专门的NullType处理NULL值
};

// NullType实现
int NullType::compare(const Value &left, const Value &right) const
{
  // NULL与任何值比较都返回-2（表示不可比较）
  // 根据SQL标准，NULL与任何值（包括NULL）比较都返回NULL
  return -2;
}

int NullType::compare(const Column &left, const Column &right, int left_idx, int right_idx) const
{
  return -2;
}

RC NullType::add(const Value &left, const Value &right, Value &result) const
{
  // NULL参与运算，结果设置为NULL
  result.set_null();
  return RC::SUCCESS;
}

RC NullType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_null();
  return RC::SUCCESS;
}

RC NullType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_null();
  return RC::SUCCESS;
}

RC NullType::divide(const Value &left, const Value &right, Value &result) const
{
  result.set_null();
  return RC::SUCCESS;
}

RC NullType::negative(const Value &val, Value &result) const
{
  result.set_null();
  return RC::SUCCESS;
}

RC NullType::cast_to(const Value &val, AttrType type, Value &result) const
{
  // NULL可以转换为任何类型，结果仍然是NULL
  result.set_null();
  return RC::SUCCESS;
}

RC NullType::to_string(const Value &val, string &result) const
{
  result = "NULL";
  return RC::SUCCESS;
}

RC NullType::set_value_from_str(Value &val, const string &data) const
{
  val.set_null();
  return RC::SUCCESS;
}