// tparquet-cpp-vectors, the acceptance hook: `check DIR` and `write OUT SRC...`, with the
// contract of acceptance/cross/vectors.py and gen-testdata --check.
#include <sys/wait.h>

#include <cstdlib>
#include <fstream>

#include "common.h"

namespace tparquet::test {
namespace {

struct Run {
  int code;
  std::string out, err;
};

std::string Quote(const std::string& s) {
  std::string q = "'";
  for (char c : s) q += c == '\'' ? std::string("'\\''") : std::string(1, c);
  return q + "'";
}

Run Cli(const std::vector<std::string>& args) {
  TempDir io;
  std::string cmd = Quote(TPARQUET_VECTORS_CLI);
  for (const auto& a : args) cmd += " " + Quote(a);
  cmd += " > " + Quote((io.path / "out").string()) + " 2> " + Quote((io.path / "err").string());
  const int status = std::system(cmd.c_str());
  return {WIFEXITED(status) ? WEXITSTATUS(status) : -1, ReadFile(io.path / "out"), ReadFile(io.path / "err")};
}

/// A copy of testdata's files and expected rows.
void CopyVectors(const std::filesystem::path& to) {
  std::filesystem::create_directories(to);
  for (const char* sub : {"files", "expected"}) {
    std::filesystem::copy(Testdata() / sub, to / sub, std::filesystem::copy_options::recursive);
  }
}

/// Changes line `line` (1-based) of `file` with `edit`.
void EditLine(const std::filesystem::path& file, int line, const std::function<void(std::string&)>& edit) {
  std::istringstream in(ReadFile(file));
  std::string out, l;
  for (int i = 1; std::getline(in, l); ++i) {
    if (i == line) edit(l);
    out += l + "\n";
  }
  WriteFile(file, out);
}

const std::string kVectors = std::to_string(VectorFiles().size()) + " vectors";

TEST(Cli, CheckAcceptsTestdata) {
  auto r = Cli({"check", Testdata().string()});
  EXPECT_EQ(r.code, 0) << r.err;
  EXPECT_EQ(r.out, kVectors + " match\n");
  EXPECT_EQ(r.err, "");
}

TEST(Cli, CheckRefusesATamperedRow) {
  TempDir dir;
  CopyVectors(dir.path);
  // as acceptance/cross/run.sh tampers: the first '1' of line 2 of temporal.jsonl becomes '2'
  EditLine(dir.path / "expected" / "temporal.jsonl", 2, [](std::string& l) { l[l.find('1')] = '2'; });
  auto r = Cli({"check", dir.path.string()});
  EXPECT_EQ(r.code, 1);
  EXPECT_EQ(r.out, "");
  EXPECT_EQ(r.err, "tparquet-cpp-vectors: temporal: line 2 differs from expected/temporal.jsonl\n");
}

TEST(Cli, CheckRefusesMissingAndExtraRows) {
  {
    TempDir dir;
    CopyVectors(dir.path);
    const auto f = dir.path / "expected" / "lists.jsonl";
    WriteFile(f, ReadFile(f) + "{\"ints\":null,\"levels\":null}\n");
    auto r = Cli({"check", dir.path.string()});
    EXPECT_EQ(r.code, 1);
    EXPECT_EQ(r.err, "tparquet-cpp-vectors: lists: line 1201 differs from expected/lists.jsonl\n");
  }
  {
    TempDir dir;
    CopyVectors(dir.path);
    const auto f = dir.path / "expected" / "mixed.jsonl";
    std::string rows = ReadFile(f);
    WriteFile(f, rows.substr(0, rows.rfind('\n', rows.size() - 2) + 1));  // drop the last row
    auto r = Cli({"check", dir.path.string()});
    EXPECT_EQ(r.code, 1);
    EXPECT_EQ(r.err, "tparquet-cpp-vectors: mixed: line 1500 differs from expected/mixed.jsonl\n");
  }
  {
    TempDir dir;
    CopyVectors(dir.path);
    std::filesystem::remove(dir.path / "expected" / "pages_v1.jsonl");
    auto r = Cli({"check", dir.path.string()});
    EXPECT_EQ(r.code, 1);
    EXPECT_EQ(r.err, "tparquet-cpp-vectors: pages_v1: no expected/pages_v1.jsonl\n");
  }
  {
    TempDir dir;
    std::filesystem::create_directories(dir.path / "files");
    auto r = Cli({"check", dir.path.string()});
    EXPECT_EQ(r.code, 1);
    EXPECT_EQ(r.err, "tparquet-cpp-vectors: no files/*.parquet in " + dir.path.string() + "\n");
  }
  {
    TempDir dir;
    CopyVectors(dir.path);
    WriteFile(dir.path / "files" / "broken.parquet", "not a parquet file");
    WriteFile(dir.path / "expected" / "broken.jsonl", "");
    auto r = Cli({"check", dir.path.string()});
    EXPECT_EQ(r.code, 1);
    EXPECT_EQ(r.err.rfind("tparquet-cpp-vectors: broken: ", 0), 0u) << r.err;
  }
}

TEST(Cli, WriteRewritesEveryVectorThroughTheWriter) {
  TempDir dir;
  const auto out = dir.path / "cpp";
  // a stale file in OUT is removed
  std::filesystem::create_directories(out / "files");
  WriteFile(out / "files" / "stale.parquet", "x");
  // two sources: testdata, and a copy of two of its vectors under other names
  const auto second = dir.path / "second";
  std::filesystem::create_directories(second / "files");
  std::filesystem::create_directories(second / "expected");
  for (const char* name : {"mixed", "lists"}) {
    std::filesystem::copy(Testdata() / "files" / (std::string(name) + ".parquet"),
                          second / "files" / ("again_" + std::string(name) + ".parquet"));
    std::filesystem::copy(Testdata() / "expected" / (std::string(name) + ".jsonl"),
                          second / "expected" / ("again_" + std::string(name) + ".jsonl"));
  }
  auto r = Cli({"write", out.string(), Testdata().string(), second.string()});
  EXPECT_EQ(r.code, 0) << r.err;
  const size_t n = VectorFiles().size() + 2;
  EXPECT_EQ(r.out, std::to_string(n) + " vectors written\n");
  EXPECT_FALSE(std::filesystem::exists(out / "files" / "stale.parquet"));
  ASSERT_EQ(VectorFiles(out).size(), n);
  for (const auto& f : VectorFiles(out)) {
    const std::string name = f.stem().string();
    const auto src = name.rfind("again_", 0) == 0 ? second : Testdata();
    EXPECT_EQ(ReadFile(out / "expected" / (name + ".jsonl")), ReadFile(src / "expected" / (name + ".jsonl")))
        << name << ": the expected rows are copied";
    // written by the C++ writer, PCODEC on every eligible column
    auto md = Metadata(f);
    EXPECT_NE(md->created_by().find("parquet-cpp-arrow"), std::string::npos) << name;
    bool eligible = false, all_pcodec = true;
    for (int rg = 0; rg < md->num_row_groups(); ++rg) {
      for (int c = 0; c < md->num_columns(); ++c) {
        auto t = md->schema()->Column(c)->physical_type();
        if (t == parquet::Type::INT32 || t == parquet::Type::INT64 || t == parquet::Type::FLOAT ||
            t == parquet::Type::DOUBLE) {
          eligible = true;
          all_pcodec &= md->RowGroup(rg)->ColumnChunk(c)->compression() == arrow::Compression::PCODEC;
        }
      }
    }
    EXPECT_TRUE(all_pcodec) << name;
    if (eligible && md->num_row_groups() > 0) {
      EXPECT_EQ(FooterValue(*md, kPcodecVersionKey), "1") << name;
    }
  }
  auto check = Cli({"check", out.string()});
  EXPECT_EQ(check.code, 0) << check.err;
  EXPECT_EQ(check.out, std::to_string(n) + " vectors match\n");
}

TEST(Cli, UsageErrors) {
  const std::string usage =
      "usage: tparquet-cpp-vectors check DIR | tparquet-cpp-vectors write OUT SRC...\n";
  for (const auto& args : std::vector<std::vector<std::string>>{
           {}, {"check"}, {"check", "a", "b"}, {"write", "out"}, {"frob", "x"}}) {
    auto r = Cli(args);
    EXPECT_EQ(r.code, 2);
    EXPECT_EQ(r.err, usage);
  }
  TempDir dir;
  auto r = Cli({"write", (dir.path / "o").string(), (dir.path / "missing").string()});
  EXPECT_EQ(r.code, 1);
  EXPECT_EQ(r.err, "tparquet-cpp-vectors: no files/*.parquet in " + (dir.path / "missing").string() + "\n");
}

}  // namespace
}  // namespace tparquet::test
