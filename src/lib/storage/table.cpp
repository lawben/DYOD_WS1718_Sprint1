#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_column.hpp"
#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunks(), _column_names(), _column_types(), _max_chunk_size(chunk_size) {
  create_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // The purpose of this method is to alter the column definition
  // *without* touching the existing columns/chunks.
  // As such, it can produce a situation where the column definition no longer
  // matches the existing columns contained in the chunks.

  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(row_count() == 0, "cannot add columns to a non-empty table");
  add_column_definition(name, type);
  for (auto& chunk : _chunks) {
    chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_max_chunk_size > 0 && _chunks.back().size() >= _max_chunk_size) {
    create_new_chunk();
  }

  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  Chunk new_chunk;
  for (auto& column_type : _column_types) {
    new_chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(column_type));
  }
  _chunks.emplace_back(std::move(new_chunk));
}

uint16_t Table::col_count() const { return _chunks.front().col_count(); }

uint64_t Table::row_count() const {
  auto row_count = 0ul;
  for (const auto& chunk : _chunks) {
    row_count += chunk.size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const {
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto it = std::find(_column_names.begin(), _column_names.end(), column_name);

  if (it != _column_names.end()) return ColumnID{it - _column_names.begin()};

  throw std::runtime_error("Column not found");
}

uint32_t Table::chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  return _chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return _chunks.at(chunk_id);
}

void Table::compress_chunk(ChunkID chunk_id) {
  const auto& uncompressed_chunk = this->get_chunk(chunk_id);
  auto compressed_chunk = std::make_shared<Chunk>();

  for (ColumnID column_id{0}; column_id < uncompressed_chunk.col_count(); ++column_id) {
    compressed_chunk->add_column(make_shared_by_column_type<BaseColumn, DictionaryColumn>(
        this->column_type(column_id), uncompressed_chunk.get_column(column_id)));
  }

  this->_chunks[chunk_id] = compressed_chunk;
}

}  // namespace opossum
