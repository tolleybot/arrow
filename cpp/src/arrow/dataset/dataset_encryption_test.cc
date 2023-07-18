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

#include <string_view>

#include "gtest/gtest.h"

#include "arrow/api.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/builder.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/parquet_encryption_config.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/api.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "parquet/arrow/reader.h"
#include "parquet/encryption/test_in_memory_kms.h"

constexpr std::string_view kFooterKeyMasterKey = "0123456789012345";
constexpr std::string_view kFooterKeyMasterKeyId = "footer_key";
constexpr std::string_view kColumnMasterKeys[] = {"1234567890123450"};
constexpr std::string_view kColumnMasterKeysIds[] = {"col_key"};
constexpr std::string_view kBaseDir = "";
const int kNumColumns = 1;

namespace arrow {
namespace dataset {

class DatasetEncryptionTest : public ::testing::Test {
 protected:
  // Create dataset encryption properties
  std::pair<std::shared_ptr<DatasetEncryptionConfiguration>,
            std::shared_ptr<DatasetDecryptionConfiguration>>
  CreateDatasetEncryptionConfig(const std::string_view* column_ids,
                                const std::string_view* column_keys, int num_columns,
                                std::string_view footer_id, std::string_view footer_key,
                                std::string_view footer_key_name = "footer_key",
                                std::string_view column_key_mapping = "col_key: a") {
    auto key_list =
        BuildKeyMap(column_ids, column_keys, num_columns, footer_id, footer_key);

    auto crypto_factory = SetupCryptoFactory(/*wrap_locally=*/true, key_list);

    auto encryption_config =
        std::make_shared<::parquet::encryption::EncryptionConfiguration>(
            std::string(footer_key_name));
    encryption_config->column_keys = column_key_mapping;
    if (footer_key_name.size() > 0) {
      encryption_config->footer_key = footer_key_name;
    }

    auto kms_connection_config =
        std::make_shared<parquet::encryption::KmsConnectionConfig>();

    // DatasetEncryptionConfiguration
    DatasetEncryptionConfiguration dataset_encryption_config;
    dataset_encryption_config.Setup(crypto_factory, kms_connection_config,
                                    encryption_config);
    // DatasetDecryptionConfiguration
    auto decryption_config =
        std::make_shared<parquet::encryption::DecryptionConfiguration>();
    DatasetDecryptionConfiguration dataset_decryption_config;
    dataset_decryption_config.Setup(crypto_factory, kms_connection_config,
                                    decryption_config);
    return std::make_pair(
        std::make_shared<DatasetEncryptionConfiguration>(dataset_encryption_config),
        std::make_shared<DatasetDecryptionConfiguration>(dataset_decryption_config));
  }

  // utility to build the key map
  std::unordered_map<std::string, std::string> BuildKeyMap(
      const std::string_view* column_ids, const std::string_view* column_keys,
      int num_columns, const std::string_view& footer_id,
      const std::string_view& footer_key) {
    std::unordered_map<std::string, std::string> key_map;
    // add column keys
    for (int i = 0; i < num_columns; i++) {
      key_map.insert({std::string(column_ids[i]), std::string(column_keys[i])});
    }
    // add footer key
    key_map.insert({std::string(footer_id), std::string(footer_key)});

    return key_map;
  }

  // A utility function to validate our files were written out */
  void ValidateFilesExist(const std::shared_ptr<arrow::fs::FileSystem>& fs,
                          const std::vector<std::string>& files) {
    for (const auto& file_path : files) {
      ASSERT_OK_AND_ASSIGN(auto result, fs->GetFileInfo(file_path));

      ASSERT_EQ(result.type(), arrow::fs::FileType::File);
    }
  }

  // Build a dummy table
  std::shared_ptr<::arrow::dataset::InMemoryDataset> BuildTable() {
    // Create an Arrow Table
    auto schema = arrow::schema(
        {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
         arrow::field("c", arrow::int64()), arrow::field("part", arrow::utf8())});

    std::vector<std::shared_ptr<arrow::Array>> arrays(4);
    arrow::NumericBuilder<arrow::Int64Type> builder;
    ARROW_EXPECT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ARROW_EXPECT_OK(builder.Finish(&arrays[0]));
    builder.Reset();
    ARROW_EXPECT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ARROW_EXPECT_OK(builder.Finish(&arrays[1]));
    builder.Reset();
    ARROW_EXPECT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_EXPECT_OK(builder.Finish(&arrays[2]));
    arrow::StringBuilder string_builder;
    ARROW_EXPECT_OK(
        string_builder.AppendValues({"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}));
    ARROW_EXPECT_OK(string_builder.Finish(&arrays[3]));

    auto table = arrow::Table::Make(schema, arrays);

    // Write it using Datasets
    return std::make_shared<::arrow::dataset::InMemoryDataset>(table);
  }

  // Helper function to create crypto factory and setup
  std::shared_ptr<::parquet::encryption::CryptoFactory> SetupCryptoFactory(
      bool wrap_locally, const std::unordered_map<std::string, std::string>& key_list) {
    auto crypto_factory = std::make_shared<::parquet::encryption::CryptoFactory>();

    auto kms_client_factory =
        std::make_shared<::parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
            wrap_locally, key_list);

    crypto_factory->RegisterKmsClientFactory(kms_client_factory);

    return crypto_factory;
  }
};
// Write dataset to disk with encryption
// The aim of this test is to demonstrate the process of writing a partitioned
// Parquet file while applying distinct file encryption properties to each
// file within the test. This is based on the selected columns.
TEST_F(DatasetEncryptionTest, WriteReadDatasetWithEncryption) {
  auto [dataset_encryption_config, dataset_decryption_config] =
      CreateDatasetEncryptionConfig(kColumnMasterKeysIds, kColumnMasterKeys, kNumColumns,
                                    kFooterKeyMasterKeyId, kFooterKeyMasterKey);

  auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();
  parquet_scan_options->SetDatasetDecryptionConfig(dataset_decryption_config);

  // create our Parquet file format object
  auto file_format = std::make_shared<ParquetFileFormat>();
  // update default scan options
  file_format->default_fragment_scan_options = parquet_scan_options;
  // set write options
  auto parquet_file_write_options =
      internal::checked_pointer_cast<ParquetFileWriteOptions>(
          file_format->DefaultWriteOptions());
  parquet_file_write_options->SetDatasetEncryptionConfig(dataset_encryption_config);

  // create our mock file system
  ::arrow::fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(auto file_system,
                       ::arrow::fs::internal::MockFileSystem::Make(mock_now, {}));
  // create filesystem
  ASSERT_OK(file_system->CreateDir(std::string(kBaseDir)));

  auto partition_schema = ::arrow::schema({::arrow::field("part", ::arrow::utf8())});
  auto partitioning =
      std::make_shared<::arrow::dataset::HivePartitioning>(partition_schema);

  // ----- Write the Dataset ----
  auto dataset_out = BuildTable();
  ASSERT_OK_AND_ASSIGN(auto scanner_builder_out, dataset_out->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner_out, scanner_builder_out->Finish());

  ::arrow::dataset::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = parquet_file_write_options;
  write_options.filesystem = file_system;
  write_options.base_dir = kBaseDir;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.parquet";
  ASSERT_OK(::arrow::dataset::FileSystemDataset::Write(write_options, scanner_out));

  std::vector<std::string> files = {"part=a/part0.parquet", "part=b/part0.parquet",
                                    "part=c/part0.parquet", "part=d/part0.parquet",
                                    "part=e/part0.parquet", "part=f/part0.parquet",
                                    "part=g/part0.parquet", "part=h/part0.parquet",
                                    "part=i/part0.parquet", "part=j/part0.parquet"};
  ValidateFilesExist(file_system, files);

  // ----- Read the Dataset -----

  // Get FileInfo objects for all files under the base directory
  arrow::fs::FileSelector selector;
  selector.base_dir = kBaseDir;
  selector.recursive = true;

  // Create a FileSystemDatasetFactory
  arrow::dataset::FileSystemFactoryOptions factory_options;
  factory_options.partitioning = partitioning;
  factory_options.partition_base_dir = kBaseDir;
  ASSERT_OK_AND_ASSIGN(auto dataset_factory,
                       arrow::dataset::FileSystemDatasetFactory::Make(
                           file_system, selector, file_format, factory_options));
  // Create a Dataset
  ASSERT_OK_AND_ASSIGN(auto dataset_in, dataset_factory->Finish());

  // Define the callback function
  std::function<arrow::Status(arrow::dataset::TaggedRecordBatch tagged_record_batch)>
      visitor =
          [](arrow::dataset::TaggedRecordBatch tagged_record_batch) -> arrow::Status {
    return arrow::Status::OK();
  };

  ASSERT_OK_AND_ASSIGN(auto scanner_builder_in, dataset_in->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner_in, scanner_builder_in->Finish());

  // Scan the dataset and check if there was an error during iteration
  ASSERT_OK(scanner_in->Scan(visitor));
}

// Write dataset to disk with encryption and then read in a single parquet file
TEST_F(DatasetEncryptionTest, WriteReadSingleFile) {
  auto [dataset_encryption_config, dataset_decryption_config] =
      CreateDatasetEncryptionConfig(kColumnMasterKeysIds, kColumnMasterKeys, kNumColumns,
                                    kFooterKeyMasterKeyId, kFooterKeyMasterKey);

  auto parquet_scan_options = std::make_shared<ParquetFragmentScanOptions>();
  parquet_scan_options->SetDatasetDecryptionConfig(dataset_decryption_config);

  // create our Parquet file format object
  auto file_format = std::make_shared<ParquetFileFormat>();
  // update default scan options
  file_format->default_fragment_scan_options = parquet_scan_options;

  // set write options
  auto file_write_options = file_format->DefaultWriteOptions();
  std::shared_ptr<ParquetFileWriteOptions> parquet_file_write_options =
      internal::checked_pointer_cast<ParquetFileWriteOptions>(file_write_options);
  parquet_file_write_options->SetDatasetEncryptionConfig(dataset_encryption_config);

  // create our mock file system
  ::arrow::fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(auto file_system,
                       ::arrow::fs::internal::MockFileSystem::Make(mock_now, {}));
  // create filesystem
  ASSERT_OK(file_system->CreateDir(std::string(kBaseDir)));

  auto partition_schema = ::arrow::schema({::arrow::field("part", ::arrow::utf8())});
  auto partitioning =
      std::make_shared<::arrow::dataset::HivePartitioning>(partition_schema);

  // ----- Write the Dataset ----
  auto dataset_out = BuildTable();
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset_out->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

  ::arrow::dataset::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = parquet_file_write_options;
  write_options.filesystem = file_system;
  write_options.base_dir = kBaseDir;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.parquet";
  ASSERT_OK(::arrow::dataset::FileSystemDataset::Write(write_options, scanner));

  // ----- Read Single File -----

  // Define the path to the encrypted Parquet file
  std::string file_path = "part=a/part0.parquet";

  auto crypto_factory = dataset_decryption_config->crypto_factory;

  // Get the FileDecryptionProperties object using the CryptoFactory object
  auto file_decryption_properties = crypto_factory->GetFileDecryptionProperties(
      *dataset_decryption_config->kms_connection_config,
      *dataset_decryption_config->decryption_config);

  // Create the ReaderProperties object using the FileDecryptionProperties object
  auto reader_properties = std::make_shared<parquet::ReaderProperties>();
  reader_properties->file_decryption_properties(file_decryption_properties);

  // Open the Parquet file using the MockFileSystem
  std::shared_ptr<arrow::io::RandomAccessFile> input;
  ASSERT_OK_AND_ASSIGN(input, file_system->OpenInputFile(file_path));

  parquet::arrow::FileReaderBuilder reader_builder;
  ASSERT_OK(reader_builder.Open(input, *reader_properties));

  ASSERT_OK_AND_ASSIGN(auto arrow_reader, reader_builder.Build());

  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table;
  ASSERT_OK(arrow_reader->ReadTable(&table));

  // Add assertions to check the contents of the table
  ASSERT_EQ(table->num_rows(), 1);
  ASSERT_EQ(table->num_columns(), 3);
}

// verify if Parquet metadata can be read without decryption
// properties when the footer is encrypted:
TEST_F(DatasetEncryptionTest, CannotReadMetadataWithEncryptedFooter) {
  auto [dataset_encryption_config, dataset_decryption_config] =
      CreateDatasetEncryptionConfig(kColumnMasterKeysIds, kColumnMasterKeys, kNumColumns,
                                    kFooterKeyMasterKeyId, kFooterKeyMasterKey);
  // create our Parquet file format object
  auto file_format = std::make_shared<ParquetFileFormat>();
  // set write options
  auto file_write_options = file_format->DefaultWriteOptions();
  std::shared_ptr<ParquetFileWriteOptions> parquet_file_write_options =
      internal::checked_pointer_cast<ParquetFileWriteOptions>(file_write_options);
  parquet_file_write_options->SetDatasetEncryptionConfig(dataset_encryption_config);

  // create our mock file system
  ::arrow::fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(auto file_system,
                       ::arrow::fs::internal::MockFileSystem::Make(mock_now, {}));
  // create filesystem
  ASSERT_OK(file_system->CreateDir(std::string(kBaseDir)));

  auto partition_schema = ::arrow::schema({::arrow::field("part", ::arrow::utf8())});
  auto partitioning =
      std::make_shared<::arrow::dataset::HivePartitioning>(partition_schema);

  // ----- Write the Dataset ----
  auto dataset_out = BuildTable();
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset_out->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

  ::arrow::dataset::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = file_write_options;
  write_options.filesystem = file_system;
  write_options.base_dir = kBaseDir;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.parquet";
  ASSERT_OK(::arrow::dataset::FileSystemDataset::Write(write_options, scanner));

  // ----- Read Single File -----

  // Define the path to the encrypted Parquet file
  std::string file_path = "part=a/part0.parquet";

  auto crypto_factory = dataset_decryption_config->crypto_factory;

  // Get the FileDecryptionProperties object using the CryptoFactory object
  auto file_decryption_properties = crypto_factory->GetFileDecryptionProperties(
      *dataset_decryption_config->kms_connection_config,
      *dataset_decryption_config->decryption_config);

  // Create the ReaderProperties object using the FileDecryptionProperties object
  auto reader_properties = std::make_shared<parquet::ReaderProperties>();
  reader_properties->file_decryption_properties(file_decryption_properties);

  // Open the Parquet file using the MockFileSystem
  std::shared_ptr<arrow::io::RandomAccessFile> input;
  ASSERT_OK_AND_ASSIGN(input, file_system->OpenInputFile(file_path));

  parquet::arrow::FileReaderBuilder reader_builder;
  ASSERT_OK(reader_builder.Open(input, *reader_properties));

  // Try to read metadata without providing decryption properties
  auto reader_properties_without_decryption = parquet::ReaderProperties();
  ASSERT_THROW(parquet::ReadMetaData(input), parquet::ParquetException);

  ASSERT_OK_AND_ASSIGN(auto arrow_reader, reader_builder.Build());

  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table;
  ASSERT_OK(arrow_reader->ReadTable(&table));

  // Add assertions to check the contents of the table
  ASSERT_EQ(table->num_rows(), 1);
  ASSERT_EQ(table->num_columns(), 3);
}

}  // namespace dataset
}  // namespace arrow
