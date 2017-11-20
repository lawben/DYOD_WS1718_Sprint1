#include "table_scan.hpp"

#include <memory>

#include "resolve_type.hpp"

namespace opossum {

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
  return table_scan->execute();
}

}  // namespace opossum
