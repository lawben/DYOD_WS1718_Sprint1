#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_column(std::shared_ptr<BaseColumn> column) {
  this->_columns.push_back(column);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == this->col_count(), "invalid amount of values");

  // push back in all columns
  for (std::size_t i = 0; i < values.size(); i++) {
    this->get_column(ColumnID{i})->append(values.at(i));
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const {
  return this->_columns.at(column_id);
}

uint16_t Chunk::col_count() const {
  return static_cast<uint16_t>(this->_columns.size());
}

uint32_t Chunk::size() const {
  if (this->col_count() == 0)
    return 0;
  else
    return static_cast<uint32_t>(this->_columns.front()->size());
}

}  // namespace opossum
