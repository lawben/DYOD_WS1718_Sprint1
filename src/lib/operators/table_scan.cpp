#include "table_scan.hpp"

#include <memory>
#include <utility>

#include "resolve_type.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class AbstractTableScanImpl {
 public:
  virtual std::shared_ptr<const Table> execute(const std::shared_ptr<const AbstractOperator> in_operator) = 0;
};

struct TableScanContext {
  const BaseAttributeVector& attribute_vector;
  const ValueID search_value;
  const bool contains_value;
  const ChunkID chunk_id;
};

template <typename T>
class TableScanImpl : public AbstractTableScanImpl {
 public:
  explicit TableScanImpl(const TableScan& parent) : _parent(parent) {}

  std::shared_ptr<const Table> execute(const std::shared_ptr<const AbstractOperator> in_operator) override;

 protected:
  const TableScan& _parent;

  void _handle_dictionary_column_for_scan_type_equals(const TableScanContext& context, PosList& positions);
  void _handle_dictionary_column_for_scan_type_not_equals(const TableScanContext& context, PosList& positions);
  void _handle_dictionary_column_for_scan_type_greater_than(const TableScanContext& context, PosList& positions);
  void _handle_dictionary_column_for_scan_type_greater_than_equals(const TableScanContext& context, PosList& positions);
  void _handle_dictionary_column_for_scan_type_less_than(const TableScanContext& context, PosList& positions);
  void _handle_dictionary_column_for_scan_type_less_than_equals(const TableScanContext& context, PosList& positions);

  template <typename TComparator>
  void _handle_value_column(const TComparator compare, const ValueColumn<T>& column, const ChunkID chunk_id,
                            const T search_value, PosList& positions);

  template <typename TComparator>
  void _handle_reference_column(const TComparator compare, const ReferenceColumn& column, const ChunkID chunk_id,
                                const T search_value, PosList& positions);
};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : _in_operator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto& table = _in_operator->get_output();
  auto column_type = table->column_type(_column_id);

  auto table_scan = make_unique_by_column_type<AbstractTableScanImpl, TableScanImpl>(column_type, *this);
  return table_scan->execute(_in_operator);
}

template <typename E>
constexpr typename std::underlying_type<E>::type to_underlying(E e) noexcept {
  return static_cast<int>(e);
}

template <typename T>
std::shared_ptr<const Table> TableScanImpl<T>::execute(const std::shared_ptr<const AbstractOperator> in_operator) {
  if (_parent.search_value().type() != typeid(T)) {
    throw std::logic_error("Unknown column data type provided");
  }

  auto search_value = type_cast<T>(_parent.search_value());
  const auto& table = in_operator->get_output();
  auto result_positions = std::make_shared<PosList>();

  std::function<bool(const T&, const T&)> compare;
  switch (_parent.scan_type()) {
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

  auto ref_table = table;
  auto table_changed = false;

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto& column = chunk.get_column(_parent.column_id());

    auto dictionary_column = std::dynamic_pointer_cast<const DictionaryColumn<T>>(column);
    if (dictionary_column) {
      DebugAssert(dictionary_column->dictionary(), "Dictionary column's dictionary is not set");
      const auto& dictionary_values = *(dictionary_column->dictionary());

      auto value_id = dictionary_column->lower_bound(search_value);
      auto contains_value = std::binary_search(dictionary_values.begin(), dictionary_values.end(), search_value);

      DebugAssert(dictionary_column->attribute_vector(), "Dictionary column's attribute vector is not set");
      const auto& attribute_vector = *dictionary_column->attribute_vector();

      TableScanContext context = {
          /* .attribute_vector = */ attribute_vector,
          /* .search_value = */ value_id,
          /* .contains_value = */ contains_value,
          /* .chunk_id = */ chunk_id,
      };

      switch (_parent.scan_type()) {
        case ScanType::OpEquals:
          _handle_dictionary_column_for_scan_type_equals(context, *result_positions);
          break;
        case ScanType::OpNotEquals:
          _handle_dictionary_column_for_scan_type_not_equals(context, *result_positions);
          break;
        case ScanType::OpLessThan:
          _handle_dictionary_column_for_scan_type_less_than(context, *result_positions);
          break;
        case ScanType::OpLessThanEquals:
          _handle_dictionary_column_for_scan_type_less_than_equals(context, *result_positions);
          break;
        case ScanType::OpGreaterThan:
          _handle_dictionary_column_for_scan_type_greater_than(context, *result_positions);
          break;
        case ScanType::OpGreaterThanEquals:
          _handle_dictionary_column_for_scan_type_greater_than_equals(context, *result_positions);
          break;
      }

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
      if (!table_changed) {
        // Make sure we only create one new shared_ptr
        ref_table = reference_column->referenced_table();
        table_changed = true;
      }
      continue;
    }

    throw std::logic_error("Unknown column type provided");
  }

  auto result_table = std::make_shared<Table>();
  Chunk result_chunk;

  for (auto column_id = ColumnID{0}; column_id < table->col_count(); ++column_id) {
    result_table->add_column_definition(table->column_name(column_id), table->column_type(column_id));
    result_chunk.add_column(std::make_shared<ReferenceColumn>(ref_table, column_id, result_positions));
  }

  result_table->emplace_chunk(std::move(result_chunk));

  return result_table;
}

template <typename T>
void TableScanImpl<T>::_handle_dictionary_column_for_scan_type_equals(const TableScanContext& context,
                                                                      PosList& positions) {
  if (!context.contains_value) return;

  for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
    if (context.attribute_vector.get(index) == context.search_value)
      positions.emplace_back(RowID{context.chunk_id, index});
  }
}

template <typename T>
void TableScanImpl<T>::_handle_dictionary_column_for_scan_type_not_equals(const TableScanContext& context,
                                                                          PosList& positions) {
  if (!context.contains_value) {
    // Insert everything
    for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
      positions.emplace_back(RowID{context.chunk_id, index});
    }
  } else {
    for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
      if (context.attribute_vector.get(index) != context.search_value)
        positions.emplace_back(RowID{context.chunk_id, index});
    }
  }
}

template <typename T>
void TableScanImpl<T>::_handle_dictionary_column_for_scan_type_greater_than(const TableScanContext& context,
                                                                            PosList& positions) {
  if (!context.contains_value) return _handle_dictionary_column_for_scan_type_greater_than_equals(context, positions);

  for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
    if (context.attribute_vector.get(index) > context.search_value)
      positions.emplace_back(RowID{context.chunk_id, index});
  }
}

template <typename T>
void TableScanImpl<T>::_handle_dictionary_column_for_scan_type_greater_than_equals(const TableScanContext& context,
                                                                                   PosList& positions) {
  for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
    if (context.attribute_vector.get(index) >= context.search_value)
      positions.emplace_back(RowID{context.chunk_id, index});
  }
}

template <typename T>
void TableScanImpl<T>::_handle_dictionary_column_for_scan_type_less_than(const TableScanContext& context,
                                                                         PosList& positions) {
  for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
    if (context.attribute_vector.get(index) < context.search_value)
      positions.emplace_back(RowID{context.chunk_id, index});
  }
}

template <typename T>
void TableScanImpl<T>::_handle_dictionary_column_for_scan_type_less_than_equals(const TableScanContext& context,
                                                                                PosList& positions) {
  if (!context.contains_value) return _handle_dictionary_column_for_scan_type_less_than(context, positions);

  for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
    if (context.attribute_vector.get(index) <= context.search_value)
      positions.emplace_back(RowID{context.chunk_id, index});
  }
}

template <typename T>
template <typename TComparator>
void TableScanImpl<T>::_handle_value_column(const TComparator compare, const ValueColumn<T>& column,
                                            const ChunkID chunk_id, const T search_value, PosList& positions) {
  const auto& values = column.values();

  for (auto index = ChunkOffset{0}; index < column.size(); ++index) {
    if (compare(values[index], search_value)) positions.emplace_back(RowID{chunk_id, index});
  }
}

template <typename T>
template <typename TComparator>
void TableScanImpl<T>::_handle_reference_column(const TComparator compare, const ReferenceColumn& column,
                                                const ChunkID chunk_id, const T search_value, PosList& positions) {
  DebugAssert(column.pos_list(), "Reference column's pos list is not set");
  const auto& original_positions = *column.pos_list();

  DebugAssert(column.referenced_table(), "Reference column's referenced table is not set");
  const auto& original_table = *column.referenced_table();

  auto original_positions_it = original_positions.cbegin();
  for (auto original_chunk = ChunkID{0}; original_chunk < original_table.chunk_count(); ++original_chunk) {
    if (original_positions_it == original_positions.cend()) break;

    if (original_positions_it->chunk_id > original_chunk) continue;

    const auto& original_column = original_table.get_chunk(original_chunk).get_column(column.referenced_column_id());

    auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(original_column);
    if (value_column) {
      for (; original_positions_it->chunk_id == original_chunk && original_positions_it != original_positions.cend();
           ++original_positions_it) {
        if (compare(value_column->values().at(original_positions_it->chunk_offset), search_value))
          positions.emplace_back(*original_positions_it);
      }
      continue;
    }

    auto dictionary_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(original_column);
    if (dictionary_column) {
      for (; original_positions_it->chunk_id == original_chunk && original_positions_it != original_positions.cend();
           ++original_positions_it) {
        if (compare(dictionary_column->get(original_positions_it->chunk_offset), search_value))
          positions.emplace_back(*original_positions_it);
      }
      continue;
    }

    throw std::logic_error("Unknown column type");
  }
}

}  // namespace opossum
