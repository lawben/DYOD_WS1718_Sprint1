#include <memory>

#include "storage/fitted_attribute_vector.hpp"

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"

namespace opossum {

class FittedAttributeVectorTest : public BaseTest {
 protected:
  void SetUp() override { vec = std::make_shared<FittedAttributeVector<uint8_t>>(10); }

  std::shared_ptr<FittedAttributeVector<uint8_t>> vec;
};

TEST_F(FittedAttributeVectorTest, GetSize) { EXPECT_EQ(vec->size(), 10ul); }

TEST_F(FittedAttributeVectorTest, GetValue) {
  vec->set(0, static_cast<ValueID>(10));
  EXPECT_EQ(vec->get(0), static_cast<ValueID>(10));
}

TEST_F(FittedAttributeVectorTest, AttributeVectorWidth) {
  EXPECT_EQ(vec->width(), 1);

  auto vec16 = std::make_shared<FittedAttributeVector<uint16_t>>(10);
  EXPECT_EQ(vec16->width(), 2);

  auto vec32 = std::make_shared<FittedAttributeVector<uint32_t>>(10);
  EXPECT_EQ(vec32->width(), 4);
}

}  // namespace opossum
