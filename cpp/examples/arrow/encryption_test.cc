#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/parquet_encryption_config.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/testing/random.h>
#include <arrow/util/base64.h>
#include <parquet/arrow/reader.h>
#include <parquet/encryption/crypto_factory.h>
#include <parquet/encryption/encryption.h>
#include <parquet/encryption/encryption_internal.h>
#include <parquet/encryption/kms_client.h>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>

using namespace arrow;
using namespace arrow::dataset;
using namespace parquet::encryption;

constexpr std::string_view kFooterKeyName = "footer_key";
constexpr std::string_view kColumnKeyMapping = "col_key: foo";
constexpr std::string_view kBaseDir = "/Users/dtolley/Documents/temp";
// #define ROW_COUNT 100
// #define ROW_COUNT std::pow(2, 15) + 1 + 1

class NoOpKmsClient : public parquet::encryption::KmsClient {
 public:
  NoOpKmsClient() {}

  std::string WrapKey(const std::string& key_bytes,
                      const std::string& master_key_identifier) override {
    // Base64 encode the key
    return arrow::util::base64_encode(std::string_view(key_bytes));
  }

  std::string UnwrapKey(const std::string& wrapped_key,
                        const std::string& master_key_identifier) override {
    // Base64 decode the key
    return arrow::util::base64_decode(std::string_view(wrapped_key));
  }
};

class NoOpKmsClientFactory : public parquet::encryption::KmsClientFactory {
 public:
  NoOpKmsClientFactory() {}

  std::shared_ptr<parquet::encryption::KmsClient> CreateKmsClient(
      const parquet::encryption::KmsConnectionConfig& config) override {
    // Return a new instance of NoOpKmsClient
    return std::make_shared<NoOpKmsClient>();
  }
};

void CreateRandomTable(std::shared_ptr<arrow::Table>& table, int64_t row_count) {
  // Using a FloatBuilder to create the array
  arrow::FloatBuilder float_builder;
  arrow::Status status;

  // Append values to the builder
  for (int64_t i = 0; i < row_count; ++i) {
    // Example: using a simple pattern or fixed value
    float value = static_cast<float>(i);  // or any other logic to generate float values
    status = float_builder.Append(value);
    if (!status.ok()) {
      throw std::runtime_error("Error appending value to FloatBuilder: " +
                               status.ToString());
    }
  }

  // Finalize the builder to get the array
  std::shared_ptr<arrow::Array> foo_array;
  status = float_builder.Finish(&foo_array);
  if (!status.ok()) {
    throw std::runtime_error("Error finalizing FloatBuilder: " + status.ToString());
  }

  // Create the table
  auto foo_field = arrow::field("foo", arrow::float32());
  auto schema = arrow::schema({foo_field});
  table = arrow::Table::Make(schema, {foo_array});
}

int main() {
  std::ifstream file("config.txt");  // Fix the instantiation of the ifstream object

  if (!file) {
    std::cerr << "Unable to open file config.txt";
    return 1;  // call system to stop
  }

  int ROW_COUNT;
  file >> ROW_COUNT;
  int NUM_THREADS;
  file >> NUM_THREADS;
  file.close();

  std::string num_threads_str = std::to_string(NUM_THREADS);
  setenv("OMP_NUM_THREADS", num_threads_str.c_str(), 1);

  std::cout << "Max value of short int: " << std::numeric_limits<short int>::max()
            << std::endl;
  std::cout << "ROW_COUNT: " << ROW_COUNT << std::endl;
  std::cout << "NUM_THREADS: " << NUM_THREADS << std::endl;
  try {
    auto file_system = std::make_shared<fs::LocalFileSystem>();

    std::shared_ptr<Table> table;
    CreateRandomTable(table, ROW_COUNT);

    auto crypto_factory = std::make_shared<CryptoFactory>();
    auto kms_client_factory = std::make_shared<NoOpKmsClientFactory>();
    crypto_factory->RegisterKmsClientFactory(std::move(kms_client_factory));
    auto kms_connection_config = std::make_shared<KmsConnectionConfig>();

    auto encryption_config =
        std::make_shared<EncryptionConfiguration>(std::string(kFooterKeyName));
    encryption_config->column_keys = kColumnKeyMapping;
    auto parquet_encryption_config = std::make_shared<ParquetEncryptionConfig>();
    parquet_encryption_config->crypto_factory = crypto_factory;
    parquet_encryption_config->kms_connection_config = kms_connection_config;
    parquet_encryption_config->encryption_config = std::move(encryption_config);

    // setenv("OMP_NUM_THREADS", "1", 1);

    // Writing dataset
    {
      // cleanup
      for (const auto& entry : std::filesystem::directory_iterator(kBaseDir)) {
        if (entry.path().extension() == ".parquet") {
          std::filesystem::remove(entry.path());
        }
      }

      auto file_format = std::make_shared<ParquetFileFormat>();
      //   auto parquet_file_write_options =
      //       std::make_shared<ParquetFileWriteOptions>(file_format->DefaultWriteOptions());
      auto parquet_file_write_options =
          arrow::internal::checked_pointer_cast<ParquetFileWriteOptions>(
              file_format->DefaultWriteOptions());
      parquet_file_write_options->parquet_encryption_config = parquet_encryption_config;

      auto dataset = std::make_shared<InMemoryDataset>(table);
      auto scanner_builder_result = dataset->NewScan();
      if (!scanner_builder_result.ok()) {
        throw std::runtime_error("Failed to create scanner builder.");
      }
      auto scanner_builder = *scanner_builder_result;
      auto scanner_result = scanner_builder->Finish();
      if (!scanner_result.ok()) {
        throw std::runtime_error("Failed to create scanner.");
      }
      auto scanner = *scanner_result;

      FileSystemDatasetWriteOptions write_options;
      write_options.file_write_options = parquet_file_write_options;
      write_options.filesystem = file_system;
      write_options.base_dir = kBaseDir;
      write_options.basename_template = "part{i}.parquet";
      write_options.partitioning = std::make_shared<DirectoryPartitioning>(schema({}));
      auto write_result = FileSystemDataset::Write(write_options, std::move(scanner));
      if (!write_result.ok()) {
        throw std::runtime_error("Failed to write dataset.");
      }
    }

    // Reading dataset
    {
      auto decryption_config = std::make_shared<DecryptionConfiguration>();
      auto parquet_decryption_config = std::make_shared<ParquetDecryptionConfig>();
      parquet_decryption_config->crypto_factory = crypto_factory;
      parquet_decryption_config->kms_connection_config = kms_connection_config;
      parquet_decryption_config->decryption_config = std::move(decryption_config);

      auto file_format = std::make_shared<ParquetFileFormat>();
      auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();
      parquet_scan_options->parquet_decryption_config = parquet_decryption_config;
      file_format->default_fragment_scan_options = std::move(parquet_scan_options);

      fs::FileSelector selector;
      selector.base_dir = kBaseDir;
      selector.recursive = true;

      FileSystemFactoryOptions factory_options;
      factory_options.partition_base_dir = kBaseDir;
      auto dataset_factory_result = FileSystemDatasetFactory::Make(
          file_system, selector, file_format, factory_options);
      if (!dataset_factory_result.ok()) {
        throw std::runtime_error("Failed to create dataset factory.");
      }
      auto dataset_factory = *dataset_factory_result;

      auto dataset_result = dataset_factory->Finish();
      if (!dataset_result.ok()) {
        throw std::runtime_error("Failed to finish dataset factory.");
      }
      auto dataset = *dataset_result;

      auto scanner_builder_result = dataset->NewScan();
      if (!scanner_builder_result.ok()) {
        throw std::runtime_error("Failed to create scanner builder.");
      }
      auto scanner_builder = *scanner_builder_result;

      if (!scanner_builder->UseThreads(false).ok()) {
        throw std::runtime_error(
            "Failed to set scanner builder to single-threaded mode.");
      }

      auto scanner_result = scanner_builder->Finish();
      if (!scanner_result.ok()) {
        throw std::runtime_error("Failed to finish scanner.");
      }
      auto scanner = *scanner_result;

      auto read_table_result = scanner->ToTable();
      if (!read_table_result.ok()) {
        throw std::runtime_error("Failed to read table from scanner.");
      }
      auto read_table = *read_table_result;
    }

    std::cout << "Dataset written and read successfully." << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
