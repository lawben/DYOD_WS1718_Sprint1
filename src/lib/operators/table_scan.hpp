#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <unordered_set>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan() {}

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  const std::shared_ptr<const AbstractOperator> _in_operator;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  class AbstractTableScanImpl {
   public:
    virtual std::shared_ptr<const Table> execute() = 0;
  };

  template <typename T>
  class TableScanImpl : public AbstractTableScanImpl {
   public:
    explicit TableScanImpl(const TableScan& parent) : _parent(parent) {}

    std::shared_ptr<const Table> execute() override;

   protected:
    template <typename TComparator>
    void _handle_dictionary_column(const TComparator compare, const DictionaryColumn<T>& column, const ChunkID chunk_id,
                                   const T search_value, PosList& positions);
    template <typename TComparator>
    void _handle_value_column(const TComparator compare, const ValueColumn<T>& column, const ChunkID chunk_id,
                              const T search_value, PosList& positions);
    template <typename TComparator>
    void _handle_reference_column(const TComparator compare, const ReferenceColumn& column, const ChunkID chunk_id,
                                  const T search_value, PosList& positions);

    const TableScan& _parent;
  };
};

template <typename T>
std::shared_ptr<const Table> TableScan::TableScanImpl<T>::execute() {
  if (_parent._search_value.type() != typeid(T)) {
    throw std::logic_error("Unknown column data type provided");
  }

  auto search_value = type_cast<T>(_parent._search_value);
  const auto& table = _parent._in_operator->get_output();
  auto result_positions = std::make_shared<PosList>();

  std::function<bool(const T&, const T&)> compare;
  switch (_parent._scan_type) {
    case ScanType::OpEquals:
      compare = [](const T& value, const T& search_value) { return value == search_value; };
      break;
    case ScanType::OpNotEquals:
      compare = [](const T& value, const T& search_value) { return value != search_value; };
      break;
    case ScanType::OpLessThan:
      compare = [](const T& value, const T& search_value) { return value < search_value; };
      break;
    case ScanType::OpLessThanEquals:
      compare = [](const T& value, const T& search_value) { return value <= search_value; };
      break;
    case ScanType::OpGreaterThan:
      compare = [](const T& value, const T& search_value) { return value > search_value; };
      break;
    case ScanType::OpGreaterThanEquals:
      compare = [](const T& value, const T& search_value) { return value >= search_value; };
      break;
    default:
      throw std::logic_error("Invalid scan type");
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto& column = chunk.get_column(_parent._column_id);

    auto dictionary_column = std::dynamic_pointer_cast<const DictionaryColumn<T>>(column);
    if (dictionary_column) {
      _handle_dictionary_column(compare, *dictionary_column, chunk_id, search_value, *result_positions);
      continue;
    }

    auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column);
    if (value_column) {
      _handle_value_column(compare, *value_column, chunk_id, search_value, *result_positions);
      continue;
    }

    auto reference_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
    if (reference_column) {
      _handle_reference_column(compare, *reference_column, chunk_id, search_value, *result_positions);
      continue;
    }

    throw std::logic_error("Unknown column type provided");
  }

  auto result_table = std::make_shared<Table>();
  Chunk result_chunk;

  for (auto column_id = ColumnID{0}; column_id < table->col_count(); ++column_id) {
    result_table->add_column_definition(table->column_name(column_id), table->column_type(column_id));
    result_chunk.add_column(std::make_shared<ReferenceColumn>(table, column_id, result_positions));
  }

  result_table->emplace_chunk(std::move(result_chunk));

  return result_table;
}

template <typename T>
template <typename TComparator>
void TableScan::TableScanImpl<T>::_handle_dictionary_column(const TComparator compare,
                                                            const DictionaryColumn<T>& column, const ChunkID chunk_id,
                                                            const T search_value, PosList& positions) {
  DebugAssert(column.dictionary(), "Dictionary column's dictionary is not set");
  const auto& dictionary_values = *column.dictionary();

  std::unordered_set<std::size_t> selected_dictionary_keys;
  selected_dictionary_keys.reserve(dictionary_values.size());

  for (auto index = 0ul; index < dictionary_values.size(); ++index) {
    if (compare(dictionary_values[index], search_value)) selected_dictionary_keys.emplace(index);
  }

  DebugAssert(column.attribute_vector(), "Dictionary column's attribute vector is not set");
  const auto& attribute_vector = *column.attribute_vector();
  for (auto index = ChunkOffset{0}; index < column.size(); ++index) {
    if (selected_dictionary_keys.find(attribute_vector.get(index)) != selected_dictionary_keys.end())
      positions.emplace_back(RowID{chunk_id, index});
  }
}

template <typename T>
template <typename TComparator>
void TableScan::TableScanImpl<T>::_handle_value_column(const TComparator compare, const ValueColumn<T>& column,
                                                       const ChunkID chunk_id, const T search_value,
                                                       PosList& positions) {
  const auto& values = column.values();

  for (auto index = ChunkOffset{0}; index < column.size(); ++index) {
    if (compare(values[index], search_value)) positions.emplace_back(RowID{chunk_id, index});
  }
}

template <typename T>
template <typename TComparator>
void TableScan::TableScanImpl<T>::_handle_reference_column(const TComparator compare, const ReferenceColumn& column,
                                                           const ChunkID chunk_id, const T search_value,
                                                           PosList& positions) {
  DebugAssert(column.pos_list(), "Reference column's pos list is not set");
  const auto& original_positions = *column.pos_list();

  DebugAssert(column.referenced_table(), "Reference column's referenced table is not set");
  const auto& original_table = *column.referenced_table();

  auto original_positions_it = original_positions.cbegin();
  for (auto original_chunk = ChunkID{0}; original_chunk < original_table.chunk_count(); ++original_chunk) {
    if (original_positions_it == original_positions.cend())
      break;

    if (original_positions_it->chunk_id > original_chunk)
      continue;

    const auto& original_column = original_table.get_chunk(original_chunk).get_column(column.referenced_column_id());

    auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(original_column);
    if (value_column) {
      for(;original_positions_it->chunk_id == original_chunk
          && original_positions_it != original_positions.cend(); ++original_positions_it) {
        if (compare(value_column->values().at(original_positions_it->chunk_offset), search_value))
          positions.emplace_back(*original_positions_it);
      }
      continue;
    }

    auto dictionary_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(original_column);
    if (dictionary_column) {
      for(;original_positions_it->chunk_id == original_chunk
        && original_positions_it != original_positions.cend(); ++original_positions_it) {
        if (compare(dictionary_column->get(original_positions_it->chunk_offset), search_value))
          positions.emplace_back(*original_positions_it);
      }
      continue;
    }

    throw std::logic_error("Unknown column type");
  }
}

}  // namespace opossum
