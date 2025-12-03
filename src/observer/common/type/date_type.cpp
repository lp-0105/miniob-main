#include "common/type/date_type.h"
#include "common/value.h"
#include "common/log/log.h"
#include "storage/common/column.h"
#include <cstdio>

bool DateType::is_valid_date(int year, int month, int day)
{
  if (year < 1 || year > 9999) return false;
  if (month < 1 || month > 12) return false;
  if (day < 1) return false;

  int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  
  bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
  if (is_leap) {
    days_in_month[2] = 29;
  }
  
  if (day > days_in_month[month]) return false;
  return true;
}

int DateType::date_to_int(int year, int month, int day)
{
  return year * 10000 + month * 100 + day;
}

void DateType::int_to_date(int date_int, int &year, int &month, int &day)
{
  year  = date_int / 10000;
  month = (date_int % 10000) / 100;
  day   = date_int % 100;
}

bool DateType::parse_date(const char *str, int &year, int &month, int &day)
{
  if (str == nullptr) return false;
  int ret = sscanf(str, "%d-%d-%d", &year, &month, &day);
  if (ret != 3) return false;
  return is_valid_date(year, month, day);
}

int DateType::compare(const Value &left, const Value &right) const
{
  int left_val = 0, right_val = 0;
  
  // 处理左值
  if (left.attr_type() == AttrType::DATES) {
    left_val = left.get_int();
  } else if (left.attr_type() == AttrType::CHARS) {
    int year, month, day;
    std::string str = left.get_string();
    if (DateType::parse_date(str.c_str(), year, month, day)) {
      left_val = DateType::date_to_int(year, month, day);
    } else {
      return -1;
    }
  } else {
    left_val = left.get_int();
  }
  
  // 处理右值
  if (right.attr_type() == AttrType::DATES) {
    right_val = right.get_int();
  } else if (right.attr_type() == AttrType::CHARS) {
    int year, month, day;
    std::string str = right.get_string();
    if (DateType::parse_date(str.c_str(), year, month, day)) {
      right_val = DateType::date_to_int(year, month, day);
    } else {
      return 1;
    }
  } else {
    right_val = right.get_int();
  }
  
  int result = 0;
  if (left_val < right_val) result = -1;
  else if (left_val > right_val) result = 1;
  
  return result;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  if (type == AttrType::DATES) {
    result = val;
    return RC::SUCCESS;
  }
  if (type == AttrType::CHARS) {
    string str;
    RC rc = to_string(val, str);
    if (rc != RC::SUCCESS) return rc;
    result.set_string(str.c_str(), static_cast<int>(str.length()));
    return RC::SUCCESS;
  }
  return RC::UNSUPPORTED;
}

RC DateType::to_string(const Value &val, string &result) const
{
  int date_int = val.get_int();
  int year, month, day;
  int_to_date(date_int, year, month, day);
  char buf[16];
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
  result = buf;
  return RC::SUCCESS;
}

int DateType::compare(const Column &left, const Column &right, int left_idx, int right_idx) const
{
  const char *left_data = left.data() + left_idx * left.attr_len();
  const char *right_data = right.data() + right_idx * right.attr_len();
  
  int left_val = *reinterpret_cast<const int *>(left_data);
  int right_val = *reinterpret_cast<const int *>(right_data);
  
  if (left_val < right_val) return -1;
  if (left_val > right_val) return 1;
  return 0;
}

RC DateType::add(const Value &left, const Value &right, Value &result) const
{
  LOG_WARN("date addition not supported");
  return RC::UNSUPPORTED;
}

RC DateType::subtract(const Value &left, const Value &right, Value &result) const
{
  LOG_WARN("date subtraction not supported");
  return RC::UNSUPPORTED;
}

RC DateType::multiply(const Value &left, const Value &right, Value &result) const
{
  LOG_WARN("date multiplication not supported");
  return RC::UNSUPPORTED;
}

RC DateType::divide(const Value &left, const Value &right, Value &result) const
{
  LOG_WARN("date division not supported");
  return RC::UNSUPPORTED;
}

RC DateType::negative(const Value &val, Value &result) const
{
  LOG_WARN("date negation not supported");
  return RC::UNSUPPORTED;
}

int DateType::cast_cost(AttrType type)
{
  if (type == AttrType::DATES) return 0;
  if (type == AttrType::CHARS) return 1;
  return INT32_MAX;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  int year, month, day;
  if (!DateType::parse_date(data.c_str(), year, month, day)) {
    return RC::INVALID_ARGUMENT;
  }
  
  int date_int = DateType::date_to_int(year, month, day);
  val.set_type(AttrType::DATES);
  val.set_data(reinterpret_cast<char *>(&date_int), sizeof(date_int));
  return RC::SUCCESS;
}