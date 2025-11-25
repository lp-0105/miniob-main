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
// Created by Wangyunlai on 2023/6/14.
//

#include "sql/executor/drop_table_executor.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "sql/stmt/drop_table_stmt.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"

RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
  if (nullptr == sql_event) {
    LOG_WARN("invalid argument. sql_event is null");
    return RC::INVALID_ARGUMENT;
  }

  SessionEvent *session_event = sql_event->session_event();
  if (nullptr == session_event) {
    LOG_WARN("invalid argument. session_event is null");
    return RC::INVALID_ARGUMENT;
  }

  Stmt *stmt = sql_event->stmt();
  if (nullptr == stmt) {
    LOG_WARN("invalid argument. stmt is null");
    return RC::INVALID_ARGUMENT;
  }

  if (stmt->type() != StmtType::DROP_TABLE) {
    LOG_WARN("invalid argument. stmt type is not drop table");
    return RC::INVALID_ARGUMENT;
  }

  DropTableStmt *drop_table_stmt = static_cast<DropTableStmt *>(stmt);
  const string &table_name = drop_table_stmt->table_name();
  if (table_name.empty()) {
    LOG_WARN("invalid argument. table name is empty");
    return RC::INVALID_ARGUMENT;
  }

  // 获取当前数据库
  Session *session = session_event->session();
  if (nullptr == session) {
    LOG_WARN("invalid argument. session is null");
    return RC::INVALID_ARGUMENT;
  }

  const char *db_name = session->get_current_db_name();
  if (nullptr == db_name) {
    LOG_WARN("invalid argument. db name is null");
    return RC::INVALID_ARGUMENT;
  }

  // 获取数据库对象
  Db *db = session->get_current_db();
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // 执行删除表操作
  // 由于Db类没有实现drop_table方法，我们返回一个未实现错误
  // 在实际应用中，需要实现表删除功能
  LOG_WARN("drop table is not implemented yet. table name=%s", table_name);
  return RC::UNIMPLEMENTED;
}