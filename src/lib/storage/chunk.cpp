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
  _columns.emplace_back(column);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == col_count(), "invalid amount of values");

  // push back in all columns
  for (auto column_id = ColumnID{0}; column_id < values.size(); ++column_id) {
    get_column(column_id)->append(values[column_id]);
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const {
  return _columns.at(column_id);
}

uint16_t Chunk::col_count() const {
  return static_cast<uint16_t>(_columns.size());
}

uint32_t Chunk::size() const {
  if (col_count() == 0) {
    return 0;
  } else {
    DebugAssert(_columns.front() != nullptr, "invalid column pointer");
    return static_cast<uint32_t>(_columns.front()->size());
  }
}

}  // namespace opossum
