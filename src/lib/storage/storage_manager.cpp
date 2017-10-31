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
  this->_tables.insert(std::make_pair(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  if (!this->has_table(name)) throw std::runtime_error("No such table");

  this->_tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return this->_tables.at(name); }

bool StorageManager::has_table(const std::string& name) const {
  return this->_tables.find(name) != this->_tables.end();
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> result;
  for (auto& table : this->_tables) {
    result.push_back(table.first);
  }
  return result;
}

void StorageManager::print(std::ostream& out) const {
  for (auto& table : this->_tables) {
    out << table.first << table.second->col_count() << table.second->row_count() << table.second->chunk_count()
        << std::endl;
  }
}

void StorageManager::reset() { get() = StorageManager(); }

}  // namespace opossum
