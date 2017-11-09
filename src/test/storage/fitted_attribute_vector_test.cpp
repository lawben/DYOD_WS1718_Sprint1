
#include <memory>
#include <storage/FittedAttributeVector.hpp>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"

namespace opossum {

class FittedAttributeVectorTest : public BaseTest {
protected:
  void SetUp() override {
    vec = std::make_shared<FittedAttributeVector<uint8_t >>(10);
  }

  std::shared_ptr<FittedAttributeVector<uint8_t>> vec;
};

TEST_F(FittedAttributeVectorTest, GetSize) {
  EXPECT_EQ(vec->size(), 10);
}

TEST_F(FittedAttributeVectorTest, GetValue) {
  vec->set(0, 10);
  EXPECT_EQ(vec->get(0), 10);
}

}