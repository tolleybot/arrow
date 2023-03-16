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

#include "arrow/testing/gtest_util.h"
#include "gtest/gtest.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/testing/future_util.h"

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "arrow/builder.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "parquet/arrow/writer.h"
#include "parquet/encryption/dataset_encryption_config.h"
#include "parquet/encryption/test_in_memory_kms.h"

const char dsFooterMasterKey[] = "0123456789012345";
const char dsFooterMasterKeyId[] = "footer_key";
const char* const dsColumnMasterKeys[] = {"1234567890123450"};
const char* const dsColumnMasterKeyIds[] = {"col_key"};

namespace arrow {
namespace dataset {

class DatasetEncryptionTest : public ::testing::Test {
 protected:
  std::unique_ptr<arrow::internal::TemporaryDir> temp_dir_;
  std::shared_ptr<::arrow::dataset::InMemoryDataset> dataset_;
  std::string footer_key_name_ = "footer_key";

  ::parquet::encryption::DatasetEncryptionConfiguration dataset_encryption_config_;
  ::parquet::encryption::DatasetDecryptionConfiguration dataset_decryption_config_;
  std::string column_key_mapping_;
  ::parquet::encryption::KmsConnectionConfig kms_connection_config_;
  std::shared_ptr<::parquet::encryption::CryptoFactory> crypto_factory_;
  std::shared_ptr<ParquetFileFormat> file_format_;

  /** setup the test
   *
   */
  void SetUp() {
    // build our dummy table
    BuildTable();

    auto key_list = BuildKeyMap(dsColumnMasterKeyIds, dsColumnMasterKeys, dsFooterMasterKeyId,
                            dsFooterMasterKey);

    // build our encryption configurations
    SetupCryptoFactory(true, key_list);

    column_key_mapping_ = "col_key: a";

    // Setup our Dataset encrytion configurations
    dataset_encryption_config_.crypto_factory = crypto_factory_;
    dataset_encryption_config_.kms_connection_config =
        std::make_shared<::parquet::encryption::KmsConnectionConfig>(
            kms_connection_config_);
    dataset_encryption_config_.encryption_config =
        std::make_shared<::parquet::encryption::EncryptionConfiguration>(
            footer_key_name_);
    dataset_encryption_config_.encryption_config->column_keys = column_key_mapping_;
    dataset_encryption_config_.encryption_config->footer_key = footer_key_name_;

    dataset_decryption_config_.crypto_factory = crypto_factory_;
    dataset_decryption_config_.kms_connection_config =
        std::make_shared<::parquet::encryption::KmsConnectionConfig>(
            kms_connection_config_);
    dataset_decryption_config_.decryption_config =
        std::make_shared<::parquet::encryption::DecryptionConfiguration>();

    // create our Parquet file format object
    file_format_ = std::make_shared<ParquetFileFormat>();

    file_format_->SetDatasetEncryptionConfig(
        std::make_shared<::parquet::encryption::DatasetEncryptionConfiguration>(
            dataset_encryption_config_));
    file_format_->SetDatasetDecryptionConfig(
        std::make_shared<::parquet::encryption::DatasetDecryptionConfiguration>(
            dataset_decryption_config_));
  }

  /** utility to build the key map*/
  std::unordered_map<std::string, std::string> BuildKeyMap(const char* const* column_ids,
                                                           const char* const* column_keys,
                                                           const char* footer_id,
                                                           const char* footer_key) {
    std::unordered_map<std::string, std::string> key_map;
    // add column keys
    for (int i = 0; i < 1; i++) {
      key_map.insert({column_ids[i], column_keys[i]});
    }
    // add footer key
    key_map.insert({footer_id, footer_key});

    return key_map;
  }

  /** utilty to build column key mapping
   *
   */
  std::string BuildColumnKeyMapping() {
    std::ostringstream stream;
    stream << dsColumnMasterKeys[0] << ":"
           << "a"
           << ";";
    return stream.str();
  }
  /** Write dataset to disk
   *
   */
  void WriteDataset() {
    ::arrow::fs::TimePoint mock_now = std::chrono::system_clock::now();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<::arrow::fs::FileSystem> filesystem,
                         ::arrow::fs::internal::MockFileSystem::Make(mock_now, {}));
    auto base_path = "";
    ASSERT_OK(filesystem->CreateDir(base_path));
    // Write it using Datasets
    ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset_->NewScan());
    ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

    auto partition_schema = ::arrow::schema({::arrow::field("part", ::arrow::utf8())});
    auto partitioning =
        std::make_shared<::arrow::dataset::HivePartitioning>(partition_schema);
    ::arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = file_format_->DefaultWriteOptions();
    write_options.filesystem = filesystem;
    write_options.base_dir = base_path;
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet";
    ASSERT_OK(::arrow::dataset::FileSystemDataset::Write(write_options, scanner));
  }

  /** Build a dummy table
   *
   */
  void BuildTable() {
    //   std::vector<int32_t> year = {2020, 2022, 2021, 2022, 2019, 2021};
    //   std::vector<int32_t> n_legs = {2, 2, 4, 4, 5, 100};
    //   std::vector<std::string> animal = {"Flamingo", "Parrot",        "Dog",
    //                                      "Horse",    "Brittle stars", "Centipede"};

    //   auto year_arr = ::arrow::MakeArray(::arrow::ArrayData::Make(
    //       ::arrow::int32(), year.size(), {nullptr, ::arrow::Buffer::Wrap(year)}));
    //   auto n_legs_arr = ::arrow::MakeArray(::arrow::ArrayData::Make(
    //       ::arrow::int32(), n_legs.size(), {nullptr, ::arrow::Buffer::Wrap(n_legs)}));
    //   auto animal_arr = ::arrow::MakeArray(::arrow::ArrayData::Make(
    //       ::arrow::utf8(), animal.size(),
    //       {nullptr, ::arrow::Buffer::Wrap(animal.front().data(),
    //                                       animal.back().data() + animal.back().size() -
    //                                           animal.front().data())}));

    //   // Create an Arrow schema for the table
    //   auto year_field = ::arrow::field("year", ::arrow::int32());
    //   auto n_legs_field = ::arrow::field("n_legs", ::arrow::int32());
    //   auto animal_field = ::arrow::field("animal", ::arrow::utf8());
    //   auto schema = ::arrow::schema({year_field, n_legs_field, animal_field});

    //   // Create an Arrow table from the arrays and schema
    //   std::vector<std::shared_ptr<::arrow::Array>> arrays = {year_arr, n_legs_arr,
    //                                                          animal_arr};
    //  table_ = ::arrow::Table::Make(schema, arrays);

    // Create an Arrow Table
    auto schema = arrow::schema(
        {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
         arrow::field("c", arrow::int64()), arrow::field("part", arrow::utf8())});
    std::vector<std::shared_ptr<arrow::Array>> arrays(4);
    arrow::NumericBuilder<arrow::Int64Type> builder;
    ASSERT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ASSERT_OK(builder.Finish(&arrays[0]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ASSERT_OK(builder.Finish(&arrays[1]));
    builder.Reset();
    ASSERT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ASSERT_OK(builder.Finish(&arrays[2]));
    arrow::StringBuilder string_builder;
    ASSERT_OK(
        string_builder.AppendValues({"a", "a", "a", "a", "a", "b", "b", "b", "b", "b"}));
    ASSERT_OK(string_builder.Finish(&arrays[3]));
    auto table = arrow::Table::Make(schema, arrays);
    // Write it using Datasets
    dataset_ = std::make_shared<::arrow::dataset::InMemoryDataset>(table);
  }

  void SaveParquetFile(const std::string& output_file,
                       const std::shared_ptr<::arrow::Table>& table) {
    PARQUET_ASSIGN_OR_THROW(auto file, ::arrow::io::FileOutputStream::Open(output_file));

    auto properties = parquet::WriterProperties::Builder().build();

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        *table, ::arrow::default_memory_pool(), file, 1048576L, properties));

    PARQUET_THROW_NOT_OK(file->Close());

    std::cout << "Saved table to Parquet file: " << output_file << std::endl;
  }

  /** Helper function to create crypto factory and setup
   */
  void SetupCryptoFactory(bool wrap_locally, const std::unordered_map<std::string, std::string>& key_list) {
    crypto_factory_ = std::make_shared<::parquet::encryption::CryptoFactory>();

    std::shared_ptr<::parquet::encryption::KmsClientFactory> kms_client_factory =
        std::make_shared<::parquet::encryption::TestOnlyInMemoryKmsClientFactory>(
            wrap_locally, key_list);

    crypto_factory_->RegisterKmsClientFactory(kms_client_factory);
  }
};

TEST_F(DatasetEncryptionTest, WriteDatasetEncrypted) { this->WriteDataset(); }

}  // namespace dataset
}  // namespace arrow
