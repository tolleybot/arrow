#pragma once

#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/kms_client.h"

namespace parquet {
namespace encryption {

struct PARQUET_EXPORT DatasetEncryptionConfiguration {
  /// core class, that translates the parameters of high level encryption
  std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory;

  std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config;

  std::shared_ptr<parquet::encryption::EncryptionConfiguration> encryption_config;
};

struct PARQUET_EXPORT DatasetDecryptionConfiguration {
  /// core class, that translates the parameters of high level encryption
  std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory;

  std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config;

  std::shared_ptr<parquet::encryption::DecryptionConfiguration> decryption_config;
};
}  // namespace encryption
}  // namespace parquet