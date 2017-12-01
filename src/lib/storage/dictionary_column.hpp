#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "value_column.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseColumn {
 public:
  /**
   * Creates a Dictionary column from a given value column.
   */
  explicit DictionaryColumn(const std::shared_ptr<BaseColumn>& base_column);

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionaryColumn in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override;

  // return the value at a certain position.
  const T get(const size_t i) const;

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override;

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const;

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const;

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const;

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(const T value) const;

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const;

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(const T value) const;

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const;

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const;

  // return the number of entries
  size_t size() const override;

 protected:
  std::shared_ptr<BaseAttributeVector> _create_fitted_vector(const size_t size);

  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

template <typename T>
DictionaryColumn<T>::DictionaryColumn(const std::shared_ptr<BaseColumn>& base_column)
    : _dictionary(nullptr), _attribute_vector(nullptr) {
  DebugAssert(base_column, "No base column provided");

  const auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(base_column);

  Assert(value_column, "Bad column type provided");

  const auto& values = value_column->values();
  _dictionary = std::make_shared<std::vector<T>>(values.begin(), values.end());
  std::sort(_dictionary->begin(), _dictionary->end());

  const auto unique_end = std::unique(_dictionary->begin(), _dictionary->end());
  const auto new_size = std::distance(_dictionary->begin(), unique_end);

  _dictionary->resize(new_size);
  _attribute_vector = _create_fitted_vector(values.size());

  for (auto row = 0u; row < values.size(); ++row) {
    const auto dictionary_entry = std::lower_bound(_dictionary->begin(), _dictionary->end(), values[row]);
    DebugAssert(*dictionary_entry == values[row], "Value was not found in dictionary just created");
    _attribute_vector->set(row, static_cast<ValueID>(dictionary_entry - _dictionary->begin()));
  }
}

template <typename T>
const AllTypeVariant DictionaryColumn<T>::operator[](const size_t i) const {
  PerformanceWarning("operator[] used");
  auto value_id = _attribute_vector->get(i);
  return _dictionary->at(value_id);
}

template <typename T>
const T DictionaryColumn<T>::get(const size_t i) const {
  auto value_id = _attribute_vector->get(i);
  return _dictionary->at(value_id);
}

template <typename T>
void DictionaryColumn<T>::append(const AllTypeVariant&) {
  throw std::logic_error("Dictionary columns are immutable!");
}

template <typename T>
std::shared_ptr<const std::vector<T>> DictionaryColumn<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const BaseAttributeVector> DictionaryColumn<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const T& DictionaryColumn<T>::value_by_value_id(ValueID value_id) const {
  return _dictionary->at(value_id);
}

template <typename T>
ValueID DictionaryColumn<T>::lower_bound(const T value) const {
  DebugAssert(_dictionary, "Dictionary pointer is empty");
  auto lower_bound_it = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
  if (lower_bound_it == _dictionary->end()) return INVALID_VALUE_ID;
  return ValueID(lower_bound_it - _dictionary->begin());
}

template <typename T>
ValueID DictionaryColumn<T>::lower_bound(const AllTypeVariant& value) const {
  auto casted_value = type_cast<T>(value);
  return lower_bound(casted_value);
}

template <typename T>
ValueID DictionaryColumn<T>::upper_bound(const T value) const {
  DebugAssert(_dictionary, "Dictionary pointer is empty");
  auto upper_bound_it = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
  if (upper_bound_it == _dictionary->end()) return INVALID_VALUE_ID;
  return ValueID(upper_bound_it - _dictionary->begin());
}

template <typename T>
ValueID DictionaryColumn<T>::upper_bound(const AllTypeVariant& value) const {
  auto casted_value = type_cast<T>(value);
  return upper_bound(type_cast<T>(casted_value));
}

template <typename T>
size_t DictionaryColumn<T>::unique_values_count() const {
  return _dictionary->size();
}

template <typename T>
size_t DictionaryColumn<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
std::shared_ptr<BaseAttributeVector> DictionaryColumn<T>::_create_fitted_vector(const size_t size) {
  auto unique_count = dictionary()->size();

  if (unique_count < std::numeric_limits<uint8_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint8_t>>(size);
  } else if (unique_count < std::numeric_limits<uint16_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint16_t>>(size);
  }

  Assert(unique_count < std::numeric_limits<uint32_t>::max(), "Too many unique values (> 2^32)");
  return std::make_shared<FittedAttributeVector<uint32_t>>(size);
}

}  // namespace opossum
