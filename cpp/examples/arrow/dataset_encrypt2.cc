#include <arrow/api.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
// #include <parquet/compression.h>
#include <parquet/encryption/crypto_factory.h>
#include <memory>
#include "parquet/encryption/test_in_memory_kms.h"  // /home/ubuntu/projects/tolleybot_arrow/cpp/src/parquet/encryption/key_management_test.cc
#include "arrow/dataset/dataset_encryption_config.h"

int main() {

  // Create a KMS connection configuration
  //   auto kms_config = std::make_shared<parquet::encryption::KmsConnectionConfig>(
  //       "kms://my-kms-url/v1/keys/my-key");

  // Create an encryption configuration
  // auto encryption_config =
  //    std::make_shared<parquet::encryption::EncryptionConfiguration>("no_key");

  // Create the crypto factory
  // auto crypto_factory =
  //    std::make_shared<parquet::encryption::CryptoFactory>(encryption_config);

  arrow::dataset::DatasetEncryptionConfiguration encryption_config;

  encryption_config.kms_connection_config =
      std::make_shared<::parquet::encryption::KmsConnectionConfig>();

  encryption_config.kms_connection_config->kms_instance_id = "kms://my-kms-url/v1/keys/my-key";

  // Create an instance of the CryptoFactory class
  encryption_config.crypto_factory =
      std::make_shared<::parquet::encryption::CryptoFactory>();

  // Create an instance of the EncryptionConfiguration class
  encryption_config.encryption_config =
      std::make_shared<::parquet::encryption::EncryptionConfiguration>("mykey");

  // Create the Arrow schema
  auto schema = arrow::schema(
      {arrow::field("col1", arrow::int32()), arrow::field("col2", arrow::float64())});

  // Create the Arrow arrays
  std::vector<int32_t> col1_values = {1, 2, 3, 4, 5};
  std::shared_ptr<arrow::Array> col1;
  arrow::Int32Builder col1_builder;
  if (!col1_builder.AppendValues(col1_values).ok()) {
    return -1;
  };
  if (!col1_builder.Finish(&col1).ok()) {
    return -1;
  };

  std::vector<double> col2_values = {1.0, 2.0, 3.0, 4.0, 5.0};
  std::shared_ptr<arrow::Array> col2;
  arrow::DoubleBuilder col2_builder;
  if (!col2_builder.AppendValues(col2_values).ok()) {
    return -1;
  };
  if (!col2_builder.Finish(&col2).ok()) {
    return -1;
  };

  // Create the Arrow table
  auto table = arrow::Table::Make(schema, {col1, col2});
  /*
    // Write the table to a Parquet file
    arrow::Buffer file_buffer;
    auto file = std::make_shared<arrow::io::BufferOutputStream>(&file_buffer);
    auto writer = parquet::arrow::WriteTable(
        table, file, arrow::Compression::SNAPPY,
        parquet::arrow::FileWriterProperties::Builder().build(), crypto_factory);
    */
  return 0;
}
