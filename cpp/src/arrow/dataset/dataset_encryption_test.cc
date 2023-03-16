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

#include <gtest/gtest.h>
#include "arrow/testing/gtest_util.h"
#include <gmock/gmock.h>


#include "arrow/array/builder_primitive.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/testing/future_util.h"
#include "arrow/dataset/partition.h"

#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/status.h"
#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/dataset/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "parquet/encryption/dataset_encryption_config.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/encryption/test_in_memory_kms.h"
#include "parquet/arrow/writer.h"

const char dsFooterMasterKey[] = "0123456789012345";
const char dsFooterMasterKeyId[] = "footer_key";
const char* const dsColumnMasterKeys[] = {"1234567890123450"};
const char* const dsColumnMasterKeyIds[] = {"col_key"};

namespace parquet {
namespace encryption {
namespace test {

class DatasetEncryptionTest : public ::testing::Test {
 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::shared_ptr<::arrow::Table> table_;
  std::string footer_key_name_ = "footer_key";

  std::unordered_map<std::string, std::string> key_list_;

  DatasetEncryptionConfiguration dataset_encryption_config_;
  DatasetDecryptionConfiguration dataset_decryption_config_;
  std::string column_key_mapping_;
  KmsConnectionConfig kms_connection_config_;
  std::shared_ptr<CryptoFactory> crypto_factory_;
  std::shared_ptr<::arrow::dataset::ParquetFileFormat> file_format_;

  void SetUp() {
    BuildTable();
    SetupCryptoFactory(true);

    key_list_ = BuildKeyMap(dsColumnMasterKeyIds, dsColumnMasterKeys, dsFooterMasterKeyId,
                            dsFooterMasterKey);

    column_key_mapping_ = BuildColumnKeyMapping();

    // set up config structs
    dataset_encryption_config_.crypto_factory = crypto_factory_;
    dataset_encryption_config_.kms_connection_config =
        std::make_shared<KmsConnectionConfig>(kms_connection_config_);
    dataset_encryption_config_.encryption_config =
        std::make_shared<EncryptionConfiguration>(footer_key_name_);
    dataset_encryption_config_.encryption_config->column_keys = column_key_mapping_;
    dataset_encryption_config_.encryption_config->footer_key = footer_key_name_;

    dataset_decryption_config_.crypto_factory = crypto_factory_;        
    dataset_decryption_config_.kms_connection_config =
        std::make_shared<KmsConnectionConfig>(kms_connection_config_);
    dataset_decryption_config_.decryption_config =
        std::make_shared<DecryptionConfiguration>();

    file_format_ = std::make_shared<::arrow::dataset::ParquetFileFormat>();
    file_format_->SetDatasetEncryptionConfig(
        std::make_shared<DatasetEncryptionConfiguration>(dataset_encryption_config_));
    file_format_->SetDatasetDecryptionConfig(
        std::make_shared<DatasetDecryptionConfiguration>(dataset_decryption_config_));

    temp_dir_ = temp_data_dir().ValueOrDie();

  }

  void WriteDataset()
  {
      ::arrow::fs::TimePoint mock_now = std::chrono::system_clock::now();
      ASSERT_OK_AND_ASSIGN(std::shared_ptr<::arrow::fs::FileSystem> filesystem,
                          ::arrow::fs::internal::MockFileSystem::Make(mock_now, {}));
      auto base_path = "";
      ASSERT_OK(filesystem->CreateDir(base_path));
       // Write it using Datasets
      auto dataset = std::make_shared<::arrow::dataset::InMemoryDataset>(table_);
      ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
      ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

      auto partition_schema = ::arrow::schema({::arrow::field("year", ::arrow::utf8())});
      auto partitioning = std::make_shared<::arrow::dataset::HivePartitioning>(partition_schema);     
      ::arrow::dataset::FileSystemDatasetWriteOptions write_options;
      write_options.file_write_options = file_format_->DefaultWriteOptions();
      write_options.filesystem = filesystem;
      write_options.base_dir = base_path;
      write_options.partitioning = partitioning;
      write_options.basename_template = "part{i}.parquet";
      ASSERT_OK(::arrow::dataset::FileSystemDataset::Write(write_options, scanner));
  }


  void BuildTable() {
    std::vector<int32_t> year = {2020, 2022, 2021, 2022, 2019, 2021};
    std::vector<int32_t> n_legs = {2, 2, 4, 4, 5, 100};
    std::vector<std::string> animal = {"Flamingo", "Parrot",        "Dog",
                                       "Horse",    "Brittle stars", "Centipede"};

    auto year_arr = ::arrow::MakeArray(::arrow::ArrayData::Make(
        ::arrow::int32(), year.size(), {nullptr, ::arrow::Buffer::Wrap(year)}));
    auto n_legs_arr = ::arrow::MakeArray(::arrow::ArrayData::Make(
        ::arrow::int32(), n_legs.size(), {nullptr, ::arrow::Buffer::Wrap(n_legs)}));
    auto animal_arr = ::arrow::MakeArray(::arrow::ArrayData::Make(
        ::arrow::utf8(), animal.size(),
        {nullptr, ::arrow::Buffer::Wrap(animal.front().data(),
                                      animal.back().data() + animal.back().size() -
                                          animal.front().data())}));

    // Create an Arrow schema for the table
    auto year_field = ::arrow::field("year", ::arrow::int32());
    auto n_legs_field = ::arrow::field("n_legs", ::arrow::int32());
    auto animal_field = ::arrow::field("animal", ::arrow::utf8());
    auto schema = ::arrow::schema({year_field, n_legs_field, animal_field});

    // Create an Arrow table from the arrays and schema
    std::vector<std::shared_ptr<::arrow::Array>> arrays = {year_arr, n_legs_arr,
                                                         animal_arr};
    auto table = ::arrow::Table::Make(schema, arrays);
  }

  void SaveParquetFile(const std::string& output_file, const std::shared_ptr<::arrow::Table>& table) {
  
  PARQUET_ASSIGN_OR_THROW(auto file, ::arrow::io::FileOutputStream::Open(output_file));

  auto properties = parquet::WriterProperties::Builder().build();

  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table,
                                                ::arrow::default_memory_pool(),
                                                file,
                                                1048576L,
                                                properties
                                                ));

   PARQUET_THROW_NOT_OK(file->Close());

  std::cout << "Saved table to Parquet file: " << output_file << std::endl;
}

  /** Helper function to create crypto factory
   * @param wrap_locally: true if the key is wrapped locally, false if the key is wrapped
   * on the server
   */
  void SetupCryptoFactory(bool wrap_locally) {
    crypto_factory_ = std::make_shared<CryptoFactory>();

    std::shared_ptr<KmsClientFactory> kms_client_factory =
        std::make_shared<TestOnlyInMemoryKmsClientFactory>(wrap_locally, key_list_);

    crypto_factory_->RegisterKmsClientFactory(kms_client_factory);
  }

};

TEST_F(DatasetEncryptionTest, WriteDatasetEncrypted) {
  this->BuildTable();  
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet
