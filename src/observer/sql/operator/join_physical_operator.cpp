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
// Created by WangYunlai on 2022/12/30.
//

#include "sql/operator/join_physical_operator.h"
#include "sql/expr/expression.h"
#include "common/log/log.h"

NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator(const std::vector<JoinCondition> &join_conditions)
    : join_conditions_(join_conditions) {}

RC NestedLoopJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("nlj operator should have 2 children");
    return RC::INTERNAL;
  }

  RC rc         = RC::SUCCESS;
  left_         = children_[0].get();
  right_        = children_[1].get();
  right_closed_ = true;
  round_done_   = true;

  rc   = left_->open(trx);
  trx_ = trx;
  return rc;
}

RC NestedLoopJoinPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  
  while (true) {
    bool left_need_step = (left_tuple_ == nullptr);
    
    if (round_done_) {
      left_need_step = true;
    } else {
      rc = right_next();
      if (rc != RC::SUCCESS) {
        if (rc == RC::RECORD_EOF) {
          left_need_step = true;
        } else {
          return rc;
        }
      } else {
        // 检查JOIN条件
        if (join_conditions_.empty() || check_join_conditions()) {
          return rc;  // 满足JOIN条件，返回结果
        }
        // 不满足JOIN条件，继续查找下一个匹配
        continue;
      }
    }

    if (left_need_step) {
      rc = left_next();
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }

    rc = right_next();
    if (rc != RC::SUCCESS) {
      if (rc == RC::RECORD_EOF) {
        continue;  // 右表遍历结束，继续左表的下一条记录
      }
      return rc;
    }

    // 检查JOIN条件
    if (join_conditions_.empty() || check_join_conditions()) {
      return rc;  // 满足JOIN条件，返回结果
    }
    // 不满足JOIN条件，继续查找下一个匹配
  }
}

RC NestedLoopJoinPhysicalOperator::close()
{
  RC rc = left_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));
  }

  if (!right_closed_) {
    rc = right_->close();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
    } else {
      right_closed_ = true;
    }
  }
  return rc;
}

Tuple *NestedLoopJoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

bool NestedLoopJoinPhysicalOperator::check_join_conditions()
{
  if (join_conditions_.empty()) {
    return true; // 没有JOIN条件，直接返回true
  }

  for (const auto &condition : join_conditions_) {
    Value left_value, right_value;
    
    // 获取左值
    if (condition.left_is_attr) {
      // 左值是字段
      RC rc = left_tuple_->find_cell(TupleCellSpec(condition.left_table.c_str(), condition.left_field.c_str()), left_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Failed to find left field %s.%s", condition.left_table.c_str(), condition.left_field.c_str());
        return false;
      }
    } else {
      // 左值是常量
      left_value = condition.left_value;
    }
    
    // 获取右值
    if (condition.right_is_attr) {
      // 右值是字段
      RC rc = right_tuple_->find_cell(TupleCellSpec(condition.right_table.c_str(), condition.right_field.c_str()), right_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Failed to find right field %s.%s", condition.right_table.c_str(), condition.right_field.c_str());
        return false;
      }
    } else {
      // 右值是常量
      right_value = condition.right_value;
    }
    
    // 比较两个值
    int compare_result = left_value.compare(right_value);
    if (compare_result == -2) { // 比较失败
      LOG_WARN("Failed to compare values");
      return false;
    }
    
    // 根据比较操作符检查条件
    bool condition_met = false;
    switch (condition.comp) {
      case EQUAL_TO:
        condition_met = (compare_result == 0);
        break;
      case LESS_EQUAL:
        condition_met = (compare_result <= 0);
        break;
      case NOT_EQUAL:
        condition_met = (compare_result != 0);
        break;
      case LESS_THAN:
        condition_met = (compare_result < 0);
        break;
      case GREAT_EQUAL:
        condition_met = (compare_result >= 0);
        break;
      case GREAT_THAN:
        condition_met = (compare_result > 0);
        break;
      default:
        LOG_WARN("Unsupported comparison operator: %d", condition.comp);
        return false;
    }
    
    if (!condition_met) {
      return false; // 有一个条件不满足，整个JOIN条件不满足
    }
  }
  
  return true; // 所有条件都满足
}

RC NestedLoopJoinPhysicalOperator::left_next()
{
  RC rc = RC::SUCCESS;
  rc    = left_->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  left_tuple_ = left_->current_tuple();
  joined_tuple_.set_left(left_tuple_);
  return rc;
}

RC NestedLoopJoinPhysicalOperator::right_next()
{
  RC rc = RC::SUCCESS;
  if (round_done_) {
    if (!right_closed_) {
      rc = right_->close();

      right_closed_ = true;
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }

    rc = right_->open(trx_);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    right_closed_ = false;

    round_done_ = false;
  }

  rc = right_->next();
  if (rc != RC::SUCCESS) {
    if (rc == RC::RECORD_EOF) {
      // 遇到EOF时立即关闭右表扫描器，以便下次从头开始扫描
      if (!right_closed_) {
        RC close_rc = right_->close();
        if (close_rc != RC::SUCCESS) {
          LOG_WARN("failed to close right oper. rc=%s", strrc(close_rc));
        } else {
          right_closed_ = true;
        }
      }
      round_done_ = true;
    }
    return rc;
  }

  right_tuple_ = right_->current_tuple();
  joined_tuple_.set_right(right_tuple_);
  return rc;
}
