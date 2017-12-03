#pragma once

#include <vector>

#include "base_attribute_vector.hpp"

namespace opossum {

template <typename uintX_t>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(const size_t size) : BaseAttributeVector(), _values(size) {}

  ValueID get(const size_t i) const override { return static_cast<ValueID>(_values.at(i)); }

  // inserts the value_id at a given position
  void set(const size_t i, const ValueID value_id) override { _values.at(i) = static_cast<uintX_t>(value_id); }

  // returns the number of values
  size_t size() const override { return _values.size(); }

  // returns the width of the values in bytes
  AttributeVectorWidth width() const override { return sizeof(uintX_t); }

  std::vector<uintX_t>& values() { return _values; }
  const std::vector<uintX_t>& values() const { return _values; }

 private:
  std::vector<uintX_t> _values;
};

}  // namespace opossum
