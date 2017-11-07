#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunks(), _column_names(), _column_types(), _max_chunk_size(chunk_size) {
  create_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  this->_column_names.push_back(name);
  this->_column_types.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  this->add_column_definition(name, type);
  for (auto& chunk : this->_chunks) {
    chunk->add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (this->_max_chunk_size > 0 && this->_chunks.back()->size() >= _max_chunk_size) {
    create_new_chunk();
  }

  this->_chunks.back()->append(values);
}

void Table::create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  for (auto& column_type : this->_column_types) {
    new_chunk->add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(column_type));
  }
  this->_chunks.push_back(new_chunk);
}

uint16_t Table::col_count() const { return this->_chunks.front()->col_count(); }

uint64_t Table::row_count() const {
  return this->_max_chunk_size * (this->chunk_count() - 1) + this->_chunks.back()->size();
}

ChunkID Table::chunk_count() const { return ChunkID{static_cast<uint32_t>(this->_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto it = std::find(this->_column_names.begin(), this->_column_names.end(), column_name);

  if (it != this->_column_names.end()) return ColumnID{it - this->_column_names.begin()};

  throw std::runtime_error("Column not found");
}

uint32_t Table::chunk_size() const { return this->_max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return this->_column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return this->_column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return this->_column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(this->chunk_count() > chunk_id && this->_chunks.at(chunk_id) != nullptr, "Invalid chunk id");
  return *this->_chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(this->chunk_count() > chunk_id && this->_chunks.at(chunk_id) != nullptr, "Invalid chunk id");
  return *this->_chunks.at(chunk_id);
}

void Table::compress_chunk(ChunkID chunk_id) { throw std::runtime_error("TODO"); }

}  // namespace opossum
