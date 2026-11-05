// tparquet-cpp-vectors: the C++ side of the cross-language acceptance (acceptance/cross).
//
//   tparquet-cpp-vectors check DIR        read every DIR/files/*.parquet with the fork and
//                                         compare it with DIR/expected/<name>.jsonl
//   tparquet-cpp-vectors write OUT SRC... rewrite every vector of each SRC through the C++
//                                         writer (policy pcodec), copying the expected rows
//
// `check` prints "<n> vectors match", or names the first differing line and exits 1, as
// acceptance/cross/vectors.py and gen-testdata --check do. Usage errors exit 2.
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "tparquet/tparquet.h"

namespace fs = std::filesystem;

namespace {

struct Mismatch : std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct Vector {
  std::string name;
  fs::path file, expected;
};

/// Every vector of `dir`; a file without expected rows is an error.
std::vector<Vector> Vectors(const fs::path& dir) {
  std::vector<Vector> out;
  std::error_code ec;
  for (fs::directory_iterator it(dir / "files", ec), end; !ec && it != end; it.increment(ec)) {
    if (it->path().extension() == ".parquet") {
      const std::string name = it->path().stem().string();
      out.push_back({name, it->path(), dir / "expected" / (name + ".jsonl")});
    }
  }
  if (out.empty()) throw Mismatch("no files/*.parquet in " + dir.string());
  std::sort(out.begin(), out.end(), [](const Vector& a, const Vector& b) { return a.name < b.name; });
  for (const auto& v : out) {
    if (!fs::is_regular_file(v.expected)) throw Mismatch(v.name + ": no expected/" + v.name + ".jsonl");
  }
  return out;
}

std::string Slurp(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  std::ostringstream s;
  s << in.rdbuf();
  if (!in) throw Mismatch("cannot read " + path.string());
  return s.str();
}

std::vector<std::string> Lines(const std::string& text) {
  std::vector<std::string> out;
  std::istringstream in(text);
  for (std::string l; std::getline(in, l);) out.push_back(l);
  return out;
}

template <typename T>
T OrThrow(arrow::Result<T> r, const std::string& name) {
  if (!r.ok()) throw Mismatch(name + ": " + r.status().ToString());
  return std::move(r).ValueOrDie();
}

void OrThrow(const arrow::Status& st, const std::string& name) {
  if (!st.ok()) throw Mismatch(name + ": " + st.ToString());
}

size_t Check(const fs::path& dir) {
  size_t n = 0;
  for (const auto& v : Vectors(dir)) {
    auto table = OrThrow(tparquet::ReadTable(v.file.string()), v.name);
    const auto got = OrThrow(tparquet::CanonicalJsonLines(*table), v.name);
    const auto want = Lines(Slurp(v.expected));
    for (size_t i = 0; i < std::max(got.size(), want.size()); ++i) {
      if (i >= got.size() || i >= want.size() || got[i] != want[i]) {
        throw Mismatch(v.name + ": line " + std::to_string(i + 1) + " differs from expected/" + v.name + ".jsonl");
      }
    }
    n++;
  }
  return n;
}

size_t Write(const fs::path& out, const std::vector<fs::path>& sources) {
  for (const char* sub : {"files", "expected"}) {
    fs::remove_all(out / sub);
    fs::create_directories(out / sub);
  }
  tparquet::WriteOptions options;
  options.policy = tparquet::Policy::kPcodec;
  size_t n = 0;
  for (const auto& src : sources) {
    for (const auto& v : Vectors(src)) {
      auto table = OrThrow(tparquet::ReadTable(v.file.string()), v.name);
      OrThrow(tparquet::WriteTable(*table, (out / "files" / (v.name + ".parquet")).string(), options), v.name);
      fs::copy_file(v.expected, out / "expected" / (v.name + ".jsonl"), fs::copy_options::overwrite_existing);
      n++;
    }
  }
  return n;
}

}  // namespace

int main(int argc, char** argv) {
  const std::vector<std::string> args(argv + 1, argv + argc);
  try {
    if (args.size() == 2 && args[0] == "check") {
      std::cout << Check(args[1]) << " vectors match\n";
      return 0;
    }
    if (args.size() >= 3 && args[0] == "write") {
      std::cout << Write(args[1], std::vector<fs::path>(args.begin() + 2, args.end())) << " vectors written\n";
      return 0;
    }
  } catch (const std::exception& e) {
    std::cerr << "tparquet-cpp-vectors: " << e.what() << "\n";
    return 1;
  }
  std::cerr << "usage: tparquet-cpp-vectors check DIR | tparquet-cpp-vectors write OUT SRC...\n";
  return 2;
}
