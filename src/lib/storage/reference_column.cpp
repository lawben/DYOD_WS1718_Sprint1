#include "reference_column.hpp"

#include <memory>

#include "chunk.hpp"
#include "table.hpp"

namespace opossum {

ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table,
                                 const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _position_list(pos) {}

const AllTypeVariant ReferenceColumn::operator[](const size_t i) const {
  auto row_id = _position_list->at(i);
  const auto& chunk = _referenced_table->get_chunk(row_id.chunk_id);
  if (row_id.chunk_offset >= chunk.size()) throw std::logic_error("Invalid chunk offset");
  return (*chunk.get_column(_referenced_column_id))[row_id.chunk_offset];
}

size_t ReferenceColumn::size() const { return _position_list->size(); }

const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const { return _position_list; }

const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const { return _referenced_table; }

ColumnID ReferenceColumn::referenced_column_id() const { return _referenced_column_id; }

}  // namespace opossum
