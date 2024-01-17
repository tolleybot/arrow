// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/parquet_encryption_config.h>
#include <arrow/filesystem/localfs.h>  // Include the header for LocalFileSystem
#include <arrow/filesystem/mockfs.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/testing/random.h>
#include <parquet/arrow/reader.h>
#include <parquet/encryption/crypto_factory.h>
#include <parquet/encryption/encryption.h>
#include <parquet/encryption/encryption_internal.h>
#include <parquet/encryption/kms_client.h>
#include <parquet/encryption/test_in_memory_kms.h>
#include <cmath>
#include <memory>
#include <string_view>
#include "gtest/gtest.h"

#include <arrow/util/base64.h>
#include <filesystem>

constexpr int ROW_COUNT = 32769;

constexpr std::string_view kFooterKeyName = "footer_key";
constexpr std::string_view kColumnKeyMapping = "col_key: foo";
constexpr std::string_view kBaseDir = "/Users/dtolley/Documents/temp";

using arrow::internal::checked_pointer_cast;

namespace arrow {
namespace dataset {

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

class LargeRowEncryptionTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
#ifdef ARROW_VALGRIND
    ::parquet::encryption::EnsureBackendInitialized();
#endif

    EXPECT_OK_AND_ASSIGN(file_system_, fs::internal::MockFileSystem::Make(
                                           std::chrono::system_clock::now(), {}));
    ASSERT_OK(file_system_->CreateDir(std::string(kBaseDir)));

    // Prepare table data with CreateRandomTable.
    CreateRandomTable();

    crypto_factory_ = std::make_shared<parquet::encryption::CryptoFactory>();

    std::shared_ptr<parquet::encryption::KmsClientFactory> kms_client_factory =
        std::make_shared<NoOpKmsClientFactory>();

    // Use your custom KMS Client Factory with CryptoFactory
    crypto_factory_->RegisterKmsClientFactory(std::move(kms_client_factory));

    kms_connection_config_ = std::make_shared<parquet::encryption::KmsConnectionConfig>();

    // Set write options with encryption configuration.
    auto encryption_config =
        std::make_shared<parquet::encryption::EncryptionConfiguration>(
            std::string(kFooterKeyName));
    encryption_config->column_keys = kColumnKeyMapping;
    auto parquet_encryption_config = std::make_shared<ParquetEncryptionConfig>();
    // Directly assign shared_ptr objects to ParquetEncryptionConfig members
    parquet_encryption_config->crypto_factory = crypto_factory_;
    parquet_encryption_config->kms_connection_config = kms_connection_config_;
    parquet_encryption_config->encryption_config = std::move(encryption_config);

    auto file_format = std::make_shared<ParquetFileFormat>();
    auto parquet_file_write_options =
        checked_pointer_cast<ParquetFileWriteOptions>(file_format->DefaultWriteOptions());
    parquet_file_write_options->parquet_encryption_config =
        std::move(parquet_encryption_config);

    // Write dataset.
    auto dataset = std::make_shared<InMemoryDataset>(table_);
    EXPECT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());

    EXPECT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

    FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = parquet_file_write_options;
    write_options.filesystem = file_system_;
    write_options.base_dir = kBaseDir;
    write_options.basename_template = "part{i}.parquet";
    write_options.partitioning =
        std::make_shared<arrow::dataset::DirectoryPartitioning>(arrow::schema({}));
    ASSERT_OK(FileSystemDataset::Write(write_options, std::move(scanner)));
  }

  static void CreateRandomTable() {
    // Define the row count using pow function
    int64_t row_count = ROW_COUNT;

    // Create a random floating-point array
    arrow::random::RandomArrayGenerator rand_gen(0);  // Seed for random number generator
    std::shared_ptr<arrow::Array> foo_array;
    foo_array =
        rand_gen.Float32(row_count, 0.0, 1.0, false);  // Generate random float32 array

    // Create a table from the array
    auto foo_field = arrow::field("foo", arrow::float32());
    auto schema = arrow::schema({foo_field});
    table_ = arrow::Table::Make(schema, {foo_array});
  }

 protected:
  inline static std::shared_ptr<fs::FileSystem> file_system_;
  inline static std::shared_ptr<Table> table_;
  inline static std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory_;
  inline static std::shared_ptr<parquet::encryption::KmsConnectionConfig>
      kms_connection_config_;
  std::string temp_dir_path_;
};

// Test for writing and reading encrypted datasetclear
TEST_F(LargeRowEncryptionTest, ReadEncryptLargeRows) {
  // Create decryption properties.
  auto decryption_config =
      std::make_shared<parquet::encryption::DecryptionConfiguration>();
  auto parquet_decryption_config = std::make_shared<ParquetDecryptionConfig>();
  parquet_decryption_config->crypto_factory = crypto_factory_;
  parquet_decryption_config->kms_connection_config = kms_connection_config_;
  parquet_decryption_config->decryption_config = std::move(decryption_config);

  auto file_format = std::make_shared<ParquetFileFormat>();
  auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();
  parquet_scan_options->parquet_decryption_config = std::move(parquet_decryption_config);
  file_format->default_fragment_scan_options = std::move(parquet_scan_options);

  // Get FileInfo objects for all files under the base directory
  fs::FileSelector selector;
  selector.base_dir = kBaseDir;
  selector.recursive = true;

  FileSystemFactoryOptions factory_options;
  factory_options.partition_base_dir = kBaseDir;
  ASSERT_OK_AND_ASSIGN(auto dataset_factory,
                       FileSystemDatasetFactory::Make(file_system_, selector, file_format,
                                                      factory_options));

  // Read dataset into table
  ASSERT_OK_AND_ASSIGN(auto dataset, dataset_factory->Finish());
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());

  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

  ASSERT_OK_AND_ASSIGN(auto read_table, scanner->ToTable());
}

}  // namespace dataset
}  // namespace arrow
