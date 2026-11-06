// Tables for the writer tests: one with every type the conformance vectors cover, and random
// ones for the property tests.
#pragma once

#include <cstdint>
#include <memory>
#include <random>

#include <arrow/api.h>

namespace tparquet::test {

/// `rows` rows of: every integer width (signed and unsigned), float16/32/64 with NaN payloads,
/// signed zeros, infinities and subnormals, decimal-valued doubles, timestamps (ms, us, ns;
/// with and without time zones), date32, time32, time64, durations, decimals (precision 9, 18
/// and 38), strings, binary, fixed-size binary, booleans, a dictionary, lists (with nulls and
/// empty lists), a nested struct with a list, and a map; most columns optional, with nulls.
std::shared_ptr<arrow::Table> EveryType(int64_t rows, uint64_t seed = 1);

/// A random table: 1 to `max_columns` columns of random types (numeric, temporal, strings,
/// lists, structs), `rows` rows, random nulls and values (smooth, decimal or arbitrary bits).
std::shared_ptr<arrow::Table> RandomTable(std::mt19937_64& rng, int64_t rows, int max_columns);

}  // namespace tparquet::test
