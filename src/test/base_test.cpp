#include "base_test.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "type_cast.hpp"

namespace opossum {

void BaseTest::EXPECT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive, bool strict_types) {
  EXPECT_TRUE(_table_equal(tleft, tright, order_sensitive, strict_types));
}

void BaseTest::ASSERT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive, bool strict_types) {
  ASSERT_TRUE(_table_equal(tleft, tright, order_sensitive, strict_types));
}

void BaseTest::EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                               bool order_sensitive, bool strict_types) {
  EXPECT_TABLE_EQ(*tleft, *tright, order_sensitive, strict_types);
}

void BaseTest::ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                               bool order_sensitive, bool strict_types) {
  ASSERT_TABLE_EQ(*tleft, *tright, order_sensitive, strict_types);
}

BaseTest::Matrix BaseTest::_table_to_matrix(const Table& t) {
  // initialize matrix with table sizes
  Matrix matrix(t.row_count(), std::vector<AllTypeVariant>(t.col_count()));

  // set values
  unsigned row_offset = 0;
  for (ChunkID chunk_id{0}; chunk_id < t.chunk_count(); chunk_id++) {
    const Chunk& chunk = t.get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk.size() == 0) continue;

    for (ColumnID col_id{0}; col_id < t.col_count(); ++col_id) {
      std::shared_ptr<BaseColumn> column = chunk.get_column(col_id);

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk.size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset][col_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk.size();
  }

  return matrix;
}

void BaseTest::_print_matrix(const BaseTest::Matrix& m) {
  std::cout << "-------------" << std::endl;
  for (unsigned row = 0; row < m.size(); row++) {
    for (ColumnID col{0}; col < m[row].size(); col++) {
      std::cout << std::setw(8) << m[row][col] << " ";
    }
    std::cout << std::endl;
  }
  std::cout << "-------------" << std::endl;
}

::testing::AssertionResult BaseTest::_table_equal(const Table& tleft, const Table& tright, bool order_sensitive,
                                                  bool strict_types) {
  Matrix left = _table_to_matrix(tleft);
  Matrix right = _table_to_matrix(tright);
  // compare schema of tables
  //  - column count
  if (tleft.col_count() != tright.col_count()) {
    _print_matrix(left);
    _print_matrix(right);
    return ::testing::AssertionFailure() << "Number of columns is different.";
  }

  //  - column names and types
  std::string left_col_type, right_col_type;
  for (ColumnID col_id{0}; col_id < tright.col_count(); ++col_id) {
    left_col_type = tleft.column_type(col_id);
    right_col_type = tright.column_type(col_id);
    // This is needed for the SQLiteTestrunner, since SQLite does not differentiate between float/double, and int/long.
    if (!strict_types) {
      if (left_col_type == "double") {
        left_col_type = "float";
      } else if (left_col_type == "long") {
        left_col_type = "int";
      }

      if (right_col_type == "double") {
        right_col_type = "float";
      } else if (right_col_type == "long") {
        right_col_type = "int";
      }
    }
    if (left_col_type != right_col_type || tleft.column_name(col_id) != tright.column_name(col_id)) {
      std::cout << "Column with ID " << col_id << " is different" << std::endl;
      std::cout << "Got: " << tleft.column_name(col_id) << " (" << tleft.column_type(col_id) << ")" << std::endl;
      std::cout << "Expected: " << tright.column_name(col_id) << " (" << tright.column_type(col_id) << ")" << std::endl;
      return ::testing::AssertionFailure() << "Table schema is different.";
    }
  }

  // compare content of tables
  //  - row count for fast failure
  if (tleft.row_count() != tright.row_count()) {
    _print_matrix(left);
    _print_matrix(right);
    std::cout << "Got: " << tleft.row_count() << " rows" << std::endl;
    std::cout << "Expected: " << tright.row_count() << " rows" << std::endl;
    return ::testing::AssertionFailure() << "Number of rows is different.";
  }

  // sort if order does not matter
  if (!order_sensitive) {
    std::sort(left.begin(), left.end());
    std::sort(right.begin(), right.end());
  }

  for (unsigned row = 0; row < left.size(); row++)
    for (ColumnID col{0}; col < left[row].size(); col++) {
      if (tleft.column_type(col) == "float") {
        auto left_val = type_cast<float>(left[row][col]);
        auto right_val = type_cast<float>(right[row][col]);

        if (strict_types) {
          EXPECT_EQ(tright.column_type(col), "float");
        } else {
          EXPECT_TRUE(tright.column_type(col) == "float" || tright.column_type(col) == "double");
        }
        EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
      } else if (tleft.column_type(col) == "double") {
        auto left_val = type_cast<double>(left[row][col]);
        auto right_val = type_cast<double>(right[row][col]);

        if (strict_types) {
          EXPECT_EQ(tright.column_type(col), "double");
        } else {
          EXPECT_TRUE(tright.column_type(col) == "float" || tright.column_type(col) == "double");
        }
        EXPECT_NEAR(left_val, right_val, 0.0001) << "Row/Col:" << row << "/" << col;
      } else {
        if (!strict_types && (tleft.column_type(col) == "int" || tleft.column_type(col) == "long")) {
          auto left_val = type_cast<int64_t>(left[row][col]);
          auto right_val = type_cast<int64_t>(right[row][col]);
          EXPECT_EQ(left_val, right_val) << "Row:" << row + 1 << " Col:" << col + 1;
        } else {
          EXPECT_EQ(left[row][col], right[row][col]) << "Row:" << row + 1 << " Col:" << col + 1;
        }
      }
    }

  return ::testing::AssertionSuccess();
}

BaseTest::~BaseTest() { StorageManager::reset(); }

}  // namespace opossum
