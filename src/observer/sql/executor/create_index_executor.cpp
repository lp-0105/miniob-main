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
// Created by Wangyunlai on 2023/4/25.
//

#include "sql/executor/create_index_executor.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/create_index_stmt.h"
#include "storage/table/table.h"

RC CreateIndexExecutor::execute(SQLStageEvent *sql_event)
{
  CreateIndexStmt *stmt = static_cast<CreateIndexStmt *>(sql_event->stmt());
  
  Table *table = stmt->table();
  const std::vector<std::string> &attr_names = stmt->attribute_names();
  const char *index_name = stmt->index_name().c_str();
  
  // 1. 获取所有字段的FieldMeta 
  std::vector<const FieldMeta *> field_metas;
  for (const std::string &attr_name : attr_names) {
    const FieldMeta *field_meta = table->table_meta().field(attr_name.c_str());
    if (field_meta == nullptr) {
      LOG_WARN("Field not found. table=%s, field=%s", table->name(), attr_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    field_metas.push_back(field_meta);
  }
  
  // 2. 调用表的创建索引方法（支持多字段）
  Trx *trx = sql_event->session_event()->session()->current_trx();
  RC rc = table->create_index(trx, field_metas, index_name);
  
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to create index. table=%s, index=%s, rc=%s",
             table->name(), index_name, strrc(rc));
  }
  
  return rc;
}