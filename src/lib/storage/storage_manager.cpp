#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  if (!_tables.insert(std::make_pair(name, table)).second) {
    throw std::logic_error("A table with the name " + name + " already exists");
  }
}

void StorageManager::drop_table(const std::string& name) {
  if (_tables.erase(name) == 0) {
    throw std::logic_error("A table with the name " + name + " doesn't exist");
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const {
  return _tables.find(name) != _tables.end();
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> result;
  for (const auto& table : _tables) {
    result.emplace_back(table.first);
  }
  return result;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& table : _tables) {
    out << "Name: " << table.first << std::endl
        << "# Columns: " << table.second->col_count() << std::endl
        << "# Rows: " << table.second->row_count() << std::endl
        << "# Chunks: " << table.second->chunk_count() << std::endl;
  }
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum
