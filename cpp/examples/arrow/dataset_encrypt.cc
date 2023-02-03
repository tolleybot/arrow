#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <parquet/arrow/writer.h>
#include <iostream>

arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
  auto schema =
      arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
                     arrow::field("c", arrow::int64())});
  std::shared_ptr<arrow::Array> array_a;
  std::shared_ptr<arrow::Array> array_b;
  std::shared_ptr<arrow::Array> array_c;

  arrow::NumericBuilder<arrow::Int64Type> builder;
  auto status = builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  if (!status.ok()) {
    return nullptr;
  }
  status = builder.Finish(&array_a);
  if (!status.ok()) {
    return nullptr;
  }
  builder.Reset();
  status = builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
  if (!status.ok()) {
    return nullptr;
  }
  status = builder.Finish(&array_b);
  if (!status.ok()) {
    return nullptr;
  }
  builder.Reset();
  status = builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2});
  if (!status.ok()) {
    return nullptr;
  }
  status = builder.Finish(&array_c);
  if (!status.ok()) {
    return nullptr;
  }
  return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

// Set up a dataset by writing two Parquet files.
std::string CreateExampleParquetDataset(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
    const std::string& root_path) {
  auto base_path = root_path + "/parquet_dataset";
  auto status = filesystem->CreateDir(base_path);
  if (!status.ok()) {
    return "";
  }
  auto result = CreateTable();

  std::shared_ptr<arrow::Table> table;

  if (result.ok()) {
    table = result.ValueOrDie();
    // Use the table as needed.
  } else {
    // Handle the error.
    auto status = result.status();
    std::cerr << status.ToString() << std::endl;
  }

  auto output = filesystem->OpenOutputStream(base_path + "/data1.parquet");
  if (!output.ok()) {
    return "";
  }

  status = parquet::arrow::WriteTable(*table->Slice(0, 5), arrow::default_memory_pool(),
                                      output.ValueOrDie(), /* chunk_size = */ 2048);
  if (!status.ok()) {
    return "";
  }
  output = filesystem->OpenOutputStream(base_path + "/data2.parquet");
  if (!output.ok()) {
    return "";
  }
  status = parquet::arrow::WriteTable(*table->Slice(5), arrow::default_memory_pool(),
                                      output.ValueOrDie(), /* chunk_size =*/2048);
  if (!status.ok()) {
    return "";
  }
  return base_path;
}

// A main functon that calls the above functions.
int main() {
  // (Doc section: FileSystem Declare)
  // First, we need a filesystem object, which lets us interact with our local
  // filesystem starting at a given path. For the sake of simplicity, that'll be
  // the current directory.
  std::shared_ptr<arrow::fs::FileSystem> fs;
  // (Doc section: FileSystem Declare)

  // (Doc section: FileSystem Init)
  // Get the CWD, use it to make the FileSystem object.
  char init_path[256];
  char* result = getcwd(init_path, 256);
  if (result == NULL) {
    return -1;
  }
  arrow::Result fsresult = arrow::fs::FileSystemFromUriOrPath(init_path);

  if (fsresult.ok()) {
    auto fs = fsresult.ValueOrDie();
    auto base_path = CreateExampleParquetDataset(fs, "/tmp");

    if (base_path.empty()) {
      return 1;
    }
  } else {
    // Handle the error.
    auto status = fsresult.status();
    std::cerr << status.ToString() << std::endl;
  }

  return 0;
}
