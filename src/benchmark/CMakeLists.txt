set(
        HYRISE_BENCHMARK_SOURCES
        main.cpp
)

set(
        LIBRARIES
        benchmark
)

include_directories(../../third_party/benchmark/include/)

# Configure hyriseBenchmark
add_executable(hyriseBenchmark ${HYRISE_BENCHMARK_SOURCES})
target_link_libraries(hyriseBenchmark hyrise ${LIBRARIES})
