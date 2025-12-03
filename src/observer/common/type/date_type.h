#pragma once

#include "common/type/data_type.h"

class DateType : public DataType
{
public:
  DateType() : DataType(AttrType::DATES) {}
  virtual ~DateType() = default;

  int compare(const Value &left, const Value &right) const override;
  int compare(const Column &left, const Column &right, int left_idx, int right_idx) const override;
  
  RC add(const Value &left, const Value &right, Value &result) const override;
  RC subtract(const Value &left, const Value &right, Value &result) const override;
  RC multiply(const Value &left, const Value &right, Value &result) const override;
  RC divide(const Value &left, const Value &right, Value &result) const override;
  RC negative(const Value &val, Value &result) const override;
  
  RC cast_to(const Value &val, AttrType type, Value &result) const override;
  RC to_string(const Value &val, string &result) const override;
  int cast_cost(AttrType type) override;
  RC set_value_from_str(Value &val, const string &data) const override;

  static bool is_valid_date(int year, int month, int day);
  static int  date_to_int(int year, int month, int day);
  static void int_to_date(int date_int, int &year, int &month, int &day);
  static bool parse_date(const char *str, int &year, int &month, int &day);
};