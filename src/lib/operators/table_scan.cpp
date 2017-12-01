#include "table_scan.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/fitted_attribute_vector.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * The table scan is implemented on different column types and different operator types in a two-step process.
 *
 * Firstly, the TableScanDispatcher determines the incoming column types and extracts the raw search values and
 * attribute vectors.
 *
 * In the second step, the actual table scan implementation invokes the correct comparison operator and optionally
 * takes the provided position list into account when scanning the attribute vector.
 */

// Helper struct used to handle the original column pointed to by a ReferenceColumn.
struct PosListContext {
  PosList::const_iterator& position_iterator;
  const PosList::const_iterator& position_end;
};

// Abstract class needed for templated subclasses
class AbstractTableScanDispatcher {
 public:
  virtual std::shared_ptr<const Table> execute(const std::shared_ptr<const AbstractOperator> in_operator) = 0;
};

/**
 * Determines the incoming column types and extracts the raw search values (and attribute vectors).
 * Then it passes the relevant information for each column type to the TableScanImpl, which performs the actual scan.
 */
template <typename T>
class TableScanDispatcher : public AbstractTableScanDispatcher {
 public:
  explicit TableScanDispatcher(const TableScan& parent) : _parent(parent) {}

  std::shared_ptr<const Table> execute(const std::shared_ptr<const AbstractOperator> in_operator) override;

 protected:
  const TableScan& _parent;

  void _execute_scan_on_value_column(const ValueColumn<T>& value_column, const ScanType scan_type,
                                     const ChunkID chunk_id, const T search_value, PosList& positions,
                                     std::optional<PosListContext> pos_list_context = std::nullopt);
  void _execute_scan_on_dictionary_column(const DictionaryColumn<T>& dictionary_column, const ScanType scan_type,
                                          const ChunkID chunk_id, const T search_value, PosList& positions,
                                          std::optional<PosListContext> pos_list_context = std::nullopt);
  void _execute_scan_on_reference_column(const ReferenceColumn& reference_column, const ScanType scan_type,
                                         const T search_value, PosList& positions);
};

/**
 * Invokes the correct comparison operator and optionally takes the provided position list into account when scanning
 * the attribute vector. All results are written to the argument 'positons'.
 */
template <typename T>
class TableScanImpl {
 public:
  void perform_scan(const ScanType scan_type, const ChunkID chunk_id, const std::vector<T>& attribute_vector,
                    const T& search_value, PosList& positions, const bool contains_value,
                    std::optional<PosListContext> pos_list_context);

 protected:
  // Helper struct to reduce the number of arguments arguments in the comparison methods.
  struct TableScanContext {
    const std::vector<T>& attribute_vector;
    const T search_value;
    const bool contains_value;
    const ChunkID chunk_id;
    std::optional<PosListContext> pos_list_context;
  };

  // Methods where we need to catch edge cases or can optimise.
  void _execute_equals_scan(const TableScanContext& context, PosList& positions);
  void _execute_not_equal_scan(const TableScanContext& context, PosList& positions);
  void _execute_greater_than_scan(const TableScanContext& context, PosList& positions);
  void _execute_less_than_equals_scan(const TableScanContext& context, PosList& positions);

  // Decides whether to call __compare_referenced_column or _compare_column depending on if the vector is referenced.
  template <typename Comparator>
  void _execute_compare_scan(const TableScanContext& context, PosList& positions, Comparator compare);

  // Compare all values in a vector referenced by a ReferenceColumn to context.search_value with compare
  template <typename Comparator>
  void _compare_referenced_column(const TableScanContext& context, PosList& positions, Comparator compare);

  // Compare all values in a vector to context.search_value with compare
  template <typename Comparator>
  void _compare_column(const TableScanContext& context, PosList& positions, Comparator compare);
};

template <typename T>
std::shared_ptr<const Table> TableScanDispatcher<T>::execute(
    const std::shared_ptr<const AbstractOperator> in_operator) {
  if (_parent.search_value().type() != typeid(T)) {
    throw std::logic_error("Unknown column data type provided");
  }

  const auto search_value = type_cast<T>(_parent.search_value());
  const auto scan_type = _parent.scan_type();

  const auto& table = in_operator->get_output();
  auto result_positions = std::make_shared<PosList>();

  // ref_table changes when dealing with a ReferenceColmn, as we want to point to the original table
  auto ref_table = table;
  auto table_changed = false;

  // For each Chunk, check whether it is a Value-, Dictionary-, or ReferenceColumn
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto& column = chunk.get_column(_parent.column_id());

    auto dictionary_column = std::dynamic_pointer_cast<const DictionaryColumn<T>>(column);
    if (dictionary_column) {
      _execute_scan_on_dictionary_column(*dictionary_column, scan_type, chunk_id, search_value, *result_positions);
      continue;
    }

    auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column);
    if (value_column) {
      _execute_scan_on_value_column(*value_column, scan_type, chunk_id, search_value, *result_positions);
      continue;
    }

    auto reference_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
    if (reference_column) {
      _execute_scan_on_reference_column(*reference_column, scan_type, search_value, *result_positions);

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

  // Fill the result table
  for (auto column_id = ColumnID{0}; column_id < table->col_count(); ++column_id) {
    result_table->add_column_definition(table->column_name(column_id), table->column_type(column_id));
    result_chunk.add_column(std::make_shared<ReferenceColumn>(ref_table, column_id, result_positions));
  }

  result_table->emplace_chunk(std::move(result_chunk));
  return result_table;
}

template <typename T>
void TableScanDispatcher<T>::_execute_scan_on_reference_column(const ReferenceColumn& reference_column,
                                                               const ScanType scan_type, const T search_value,
                                                               PosList& positions) {
  DebugAssert(reference_column.pos_list(), "Reference column's pos list is not set");
  const auto& original_positions = *reference_column.pos_list();

  if (original_positions.empty()) return;

  DebugAssert(reference_column.referenced_table(), "Reference column's referenced table is not set");
  const auto& original_table = *reference_column.referenced_table();

  // We assume that the chunk_ids of positions are always in order.
  auto positions_it = original_positions.cbegin();
  for (auto chunk_id = ChunkID{0}; chunk_id < original_table.chunk_count(); ++chunk_id) {
    const auto& original_column =
        original_table.get_chunk(chunk_id).get_column(reference_column.referenced_column_id());
    PosListContext pos_list_context = {positions_it, original_positions.cend()};

    // If the ReferenceColumn points to a ValueColumn, perform the ValueColumn scan on the original with a PosList to
    // determine which values are relevant for the scan.
    const auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(original_column);
    if (value_column) {
      _execute_scan_on_value_column(*value_column, scan_type, chunk_id, search_value, positions, pos_list_context);
      continue;
    }

    // If the ReferenceColumn points to a DictionaryColumn, perform the DictionaryColumn scan on the original with a
    // PosList to determine which values are relevant for the scan.
    const auto dictionary_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(original_column);
    if (dictionary_column) {
      _execute_scan_on_dictionary_column(*dictionary_column, scan_type, chunk_id, search_value, positions,
                                         pos_list_context);
      continue;
    }

    throw std::logic_error("Unknown column type");
  }
}

template <typename T>
void TableScanDispatcher<T>::_execute_scan_on_value_column(const ValueColumn<T>& value_column, const ScanType scan_type,
                                                           const ChunkID chunk_id, const T search_value,
                                                           PosList& positions,
                                                           std::optional<PosListContext> pos_list_context) {
  TableScanImpl<T> scan;
  scan.perform_scan(scan_type, chunk_id, value_column.values(), search_value, positions, true, pos_list_context);
}

template <typename T>
void TableScanDispatcher<T>::_execute_scan_on_dictionary_column(const DictionaryColumn<T>& dictionary_column,
                                                                const ScanType scan_type, const ChunkID chunk_id,
                                                                const T search_value, PosList& positions,
                                                                std::optional<PosListContext> pos_list_context) {
  const auto lower_bound = dictionary_column.lower_bound(search_value);

  DebugAssert(dictionary_column.dictionary(), "Dictionary column's dictionary is not set");
  const auto& dictionary_values = *dictionary_column.dictionary();

  const auto contains_value = lower_bound != INVALID_VALUE_ID && dictionary_values[lower_bound] == search_value;

  // Determine which size attribute vector we have and run the scan on the correct type to avoid virtual calls for
  // each value access to BaseAtributeVector.
  const auto small_attribute_vector =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint8_t>>(dictionary_column.attribute_vector());
  if (small_attribute_vector) {
    TableScanImpl<uint8_t> scan;
    scan.perform_scan(scan_type, chunk_id, small_attribute_vector->values(), static_cast<uint8_t>(lower_bound),
                      positions, contains_value, pos_list_context);
    return;
  }

  const auto medium_attribute_vector =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint16_t>>(dictionary_column.attribute_vector());
  if (medium_attribute_vector) {
    TableScanImpl<uint16_t> scan;
    scan.perform_scan(scan_type, chunk_id, medium_attribute_vector->values(), static_cast<uint16_t>(lower_bound),
                      positions, contains_value, pos_list_context);
    return;
  }

  const auto large_attribute_vector =
      std::dynamic_pointer_cast<const FittedAttributeVector<uint32_t>>(dictionary_column.attribute_vector());
  if (large_attribute_vector) {
    TableScanImpl<uint32_t> scan;
    scan.perform_scan(scan_type, chunk_id, large_attribute_vector->values(), lower_bound, positions, contains_value,
                      pos_list_context);
    return;
  }

  throw std::logic_error("Unknown column type provided");
}

template <typename T>
void TableScanImpl<T>::perform_scan(const ScanType scan_type, const ChunkID chunk_id,
                                    const std::vector<T>& attribute_vector, const T& search_value, PosList& positions,
                                    const bool contains_value, std::optional<PosListContext> pos_list_context) {
  TableScanContext context = {attribute_vector, search_value, contains_value, chunk_id, pos_list_context};

  // Some special methods are called if there are edge cases or optimisations possible.
  // Else, default back to comapring with a lamdba.
  switch (scan_type) {
    case ScanType::OpEquals:
      return _execute_equals_scan(context, positions);
    case ScanType::OpNotEquals:
      return _execute_not_equal_scan(context, positions);
    case ScanType::OpLessThan: {
      auto lt = [](const T& val, const T& search_val) { return val < search_val; };
      return _execute_compare_scan(context, positions, lt);
    }
    case ScanType::OpLessThanEquals:
      return _execute_less_than_equals_scan(context, positions);
    case ScanType::OpGreaterThan:
      return _execute_greater_than_scan(context, positions);
    case ScanType::OpGreaterThanEquals: {
      auto geq = [](const T& val, const T& search_val) { return val >= search_val; };
      return _execute_compare_scan(context, positions, geq);
    }
    default:
      throw std::logic_error("Unknown ScanType provided.");
  }
}

template <typename T>
void TableScanImpl<T>::_execute_equals_scan(const TableScanContext& context, PosList& positions) {
  // No value will match, so we can exit early
  if (!context.contains_value) return;

  // Compare all values
  auto compare = [](const T& val, const T& search_val) { return val == search_val; };
  return _execute_compare_scan(context, positions, compare);
}

template <typename T>
void TableScanImpl<T>::_execute_not_equal_scan(const TableScanContext& context, PosList& positions) {
  if (!context.contains_value) {
    // All values match, so we don't need to compare and can insert all values directly
    if (context.pos_list_context) {
      // Insert all previously selected positions from referencing ReferenceColumn
      auto& pos_it = context.pos_list_context.value().position_iterator;
      const auto& pos_end = context.pos_list_context.value().position_end;
      for (; pos_it != pos_end && pos_it->chunk_id == context.chunk_id; ++pos_it) {
        const auto& offset = pos_it->chunk_offset;
        positions.emplace_back(RowID{context.chunk_id, offset});
      }
    } else {
      // Insert everything
      for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
        positions.emplace_back(RowID{context.chunk_id, index});
      }
    }
  } else {
    // Value is in vector, so we need to compare each value
    auto compare = [](const T& val, const T& search_val) { return val != search_val; };
    _execute_compare_scan(context, positions, compare);
  }
}

template <typename T>
void TableScanImpl<T>::_execute_greater_than_scan(const TableScanContext& context, PosList& positions) {
  if (!context.contains_value) {
    // We are using lower bound to find the value, so if it is not contained, we need all values that are greater or
    // equal to the first larger value in the vector.
    // Make a copy of the context with contains_value = true
    TableScanContext greater_than_equals_context = {context.attribute_vector, context.search_value, true,
                                                    context.chunk_id, context.pos_list_context};
    auto compare = [](const T& val, const T& search_val) { return val >= search_val; };
    return _execute_compare_scan(greater_than_equals_context, positions, compare);
  }

  // Compare all values
  auto compare = [](const T& val, const T& search_val) { return val > search_val; };
  return _execute_compare_scan(context, positions, compare);
}

template <typename T>
void TableScanImpl<T>::_execute_less_than_equals_scan(const TableScanContext& context, PosList& positions) {
  if (!context.contains_value) {
    // We are using lower bound to find the value, so if it is not contained, we need all values that are less than
    // the first larger value in the vector.
    // Make a copy of the context with contains_value = true
    TableScanContext less_than_context = {context.attribute_vector, context.search_value, true, context.chunk_id,
                                          context.pos_list_context};
    auto compare = [](const T& val, const T& search_val) { return val < search_val; };
    return _execute_compare_scan(less_than_context, positions, compare);
  }

  // Compare all values
  auto compare = [](const T& val, const T& search_val) { return val <= search_val; };
  return _execute_compare_scan(context, positions, compare);
}

template <typename T>
template <typename Comparator>
void TableScanImpl<T>::_execute_compare_scan(const TableScanContext& context, PosList& positions, Comparator compare) {
  if (context.pos_list_context) {
    _compare_referenced_column(context, positions, compare);
  } else {
    _compare_column(context, positions, compare);
  }
}

template <typename T>
template <typename Comparator>
void TableScanImpl<T>::_compare_referenced_column(const TableScanImpl::TableScanContext& context, PosList& positions,
                                                  Comparator compare) {
  // Only look at values that were referenced
  auto& pos_it = context.pos_list_context.value().position_iterator;
  const auto& pos_end = context.pos_list_context.value().position_end;
  for (; pos_it != pos_end && pos_it->chunk_id == context.chunk_id; ++pos_it) {
    const auto& offset = pos_it->chunk_offset;
    if (compare(context.attribute_vector[offset], context.search_value))
      positions.emplace_back(RowID{context.chunk_id, offset});
  }
}

template <typename T>
template <typename Comparator>
void TableScanImpl<T>::_compare_column(const TableScanImpl::TableScanContext& context, PosList& positions,
                                       Comparator compare) {
  // Look at all values
  for (auto index = ChunkOffset{0}; index < context.attribute_vector.size(); ++index) {
    if (compare(context.attribute_vector[index], context.search_value))
      positions.emplace_back(RowID{context.chunk_id, index});
  }
}

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : _in_operator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto& table = _in_operator->get_output();
  auto column_type = table->column_type(_column_id);

  auto table_scan = make_unique_by_column_type<AbstractTableScanDispatcher, TableScanDispatcher>(column_type, *this);
  return table_scan->execute(_in_operator);
}

}  // namespace opossum
