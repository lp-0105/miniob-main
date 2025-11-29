/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/trx/lsm_mvcc_trx.h"
#include "common/lang/mutex.h"
#include "common/log/log.h"

RC LsmMvccTrxKit::init() { return RC::SUCCESS; }

const vector<FieldMeta> *LsmMvccTrxKit::trx_fields() const { return nullptr; }

Trx *LsmMvccTrxKit::create_trx(LogHandler &) { return new LsmMvccTrx(lsm_); }

Trx *LsmMvccTrxKit::create_trx(LogHandler &, int32_t /*trx_id*/) { return nullptr; }

void LsmMvccTrxKit::destroy_trx(Trx *trx) { delete trx; }

void LsmMvccTrxKit::all_trxes(vector<Trx *> &trxes) { return; }

/** oblsm 自身的日志回放是足够的，这里其实是空实现 */
LogReplayer *LsmMvccTrxKit::create_log_replayer(Db &, LogHandler &) { return new LsmMvccTrxLogReplayer; }

////////////////////////////////////////////////////////////////////////////////

RC LsmMvccTrx::insert_record(Table *table, Record &record)
{
   return table->insert_record_with_trx(record, this);
}

RC LsmMvccTrx::delete_record(Table *table, Record &record)
{
  return table->delete_record_with_trx(record, this);
}

RC LsmMvccTrx::update_record(Table *table, Record &old_record, Record &new_record)
{
  return table->update_record_with_trx(this, old_record, new_record);
}
/**
 * 在 index scan 中使用的，需要适配 index scan
 */
RC LsmMvccTrx::visit_record(Table *table, Record &record, ReadWriteMode mode) 
{
  // 对于LSM存储引擎，由于MVCC机制由底层LSM引擎处理，
  // 这里不需要额外的锁管理，直接返回成功
  // LSM引擎的MVCC机制会自动处理并发控制
  return RC::SUCCESS;
}

RC LsmMvccTrx::start_if_need()
{
  if (trx_ != nullptr) {
    return RC::SUCCESS;
  }
  trx_ = lsm_->begin_transaction();
  return RC::SUCCESS;
}

RC LsmMvccTrx::commit()
{
  if (trx_ == nullptr) {
    return RC::SUCCESS;
  }
  return trx_->commit();
}

RC LsmMvccTrx::rollback()
{
  return trx_->rollback();
}

/**
 * 实际没有使用
 */
RC LsmMvccTrx::redo(Db *, const LogEntry &) { return RC::SUCCESS; }

RC LsmMvccTrx::check_intra_transaction_lock(PageNum page_num, ReadWriteMode mode)
{
  lock_mutex_.lock();

  auto it = intra_transaction_locks_.find(page_num);
  if (it == intra_transaction_locks_.end()) {
    // 没有获取过锁，可以获取任何模式的锁
    lock_mutex_.unlock();
    return RC::SUCCESS;
  }

  LockInfo &lock_info = it->second;
  if (lock_info.mode == mode) {
    // 已经获取了相同模式的锁，可以重复获取
    lock_mutex_.unlock();
    return RC::SUCCESS;
  }

  if (lock_info.mode == ReadWriteMode::READ_ONLY && mode == ReadWriteMode::READ_WRITE) {
    // 读锁升级为写锁，同一事务内允许
    lock_info.mode = mode;
    lock_mutex_.unlock();
    return RC::SUCCESS;
  }

  // 写锁降级为读锁，同一事务内允许
  if (lock_info.mode == ReadWriteMode::READ_WRITE && mode == ReadWriteMode::READ_ONLY) {
    lock_info.mode = mode;
    lock_mutex_.unlock();
    return RC::SUCCESS;
  }

  lock_mutex_.unlock();
  return RC::LOCKED_CONCURRENCY_CONFLICT;
}

void LsmMvccTrx::record_intra_transaction_lock(PageNum page_num, ReadWriteMode mode)
{
  lock_mutex_.lock();

  auto it = intra_transaction_locks_.find(page_num);
  if (it == intra_transaction_locks_.end()) {
    // 第一次获取该页面的锁
    intra_transaction_locks_[page_num] = LockInfo{mode};
  } else {
    // 更新锁模式：如果当前请求的是写锁，则升级为写锁
    if (mode == ReadWriteMode::READ_WRITE) {
      it->second.mode = ReadWriteMode::READ_WRITE;
    }
    // 如果当前请求的是读锁，但已经持有写锁，则保持写锁不变
  }
  
  lock_mutex_.unlock();
}
