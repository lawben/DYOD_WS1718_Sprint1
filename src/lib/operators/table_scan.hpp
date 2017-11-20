#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

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

  return nullptr;
}

template <typename T>
template <typename TComparator>
void TableScan::TableScanImpl<T>::_handle_dictionary_column(const TComparator compare,
                                                            const DictionaryColumn<T>& column, const ChunkID chunk_id,
                                                            const T search_value, PosList& positions) {}

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
                                                           PosList& positions) {}

}  // namespace opossum
