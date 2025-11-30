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

#include "sql/operator/update_logical_operator.h"

// 单字段更新构造函数（保持向后兼容）
UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const char *attribute_name, Value *values, int value_amount)
    : table_(table), attribute_name_(attribute_name), values_(values), value_amount_(value_amount), is_multi_field_(false)
{}

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const char *attribute_name, unique_ptr<Expression> expression)
    : table_(table), attribute_name_(attribute_name), expression_(std::move(expression)), is_multi_field_(false)
{}

// 多字段更新构造函数
UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                                             const std::vector<Value*> &values_list, const std::vector<int> &value_amounts)
    : table_(table), attribute_names_(attribute_names), values_list_(values_list), value_amounts_(value_amounts), is_multi_field_(true)
{}

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const std::vector<std::string> &attribute_names, 
                                             std::vector<unique_ptr<Expression>> &&expressions)
    : table_(table), attribute_names_(attribute_names), expressions_(std::move(expressions)), is_multi_field_(true)
{}