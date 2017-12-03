#include <random>

#include <benchmark/benchmark.h>

#include <storage/table.hpp>
#include <storage/value_column.hpp>
#include <operators/table_scan.hpp>
#include <operators/table_wrapper.hpp>


namespace opossum {

static std::shared_ptr<opossum::TableWrapper> create_table(size_t chunks, size_t rows_per_chunk) {
  auto table = std::make_shared<opossum::Table>(rows_per_chunk);
  table->add_column_definition("int_column", "int");

  for (auto chunk_id = opossum::ChunkID{0}; chunk_id < chunks; ++chunk_id) {
    // Create an int column

    auto int_column = std::make_shared<opossum::ValueColumn<int32_t>>();
    for (auto row = 0ul; row < rows_per_chunk; ++row) {
        int_column->append((int32_t)row);
    }

    opossum::Chunk chunk;
    chunk.add_column(std::move(int_column));
    table->emplace_chunk(std::move(chunk));

    // Dictionary-compress all but the last chunk
    if (chunk_id < chunks - 1)
      table->compress_chunk(chunk_id);
  }

  auto wrapper = std::make_shared<opossum::TableWrapper>(std::move(table));
  wrapper->execute();
  return wrapper;
}

static void BM_TableScan_IntColumn(benchmark::State &state) {
  auto table = create_table(state.range(0), state.range(1));

  while (state.KeepRunning()) {
    auto scan = std::make_shared<opossum::TableScan>(table, opossum::ColumnID{0},
                                                     opossum::ScanType::OpGreaterThanEquals, 
                                                     (int32_t)((state.range(0) *  state.range(1)) / 2));
    scan->execute();
  }
}

BENCHMARK(BM_TableScan_IntColumn)
       ->RangeMultiplier(10)
       ->Ranges({
                        {1,    100},      // Chunks
                        {1000, 10000000}  // Rows per Chunk
                })
       ->Unit(benchmark::kMicrosecond);

}  // namespace opossum

BENCHMARK_MAIN();
