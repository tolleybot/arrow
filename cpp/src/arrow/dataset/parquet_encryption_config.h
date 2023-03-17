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

#pragma once

#include "arrow/dataset/type_fwd.h"
#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/kms_client.h"

namespace arrow {
namespace dataset {

struct ARROW_DS_EXPORT DatasetEncryptionConfiguration {
  /// core class, that translates the parameters of high level encryption

  DatasetEncryptionConfiguration()
      : kms_connection_config(
            std::make_shared<parquet::encryption::KmsConnectionConfig>()) {}

  void Setup(
      std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory,
      std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config,
      std::shared_ptr<parquet::encryption::EncryptionConfiguration> encryption_config) {
    this->crypto_factory = crypto_factory;
    this->kms_connection_config = kms_connection_config;
    this->encryption_config = encryption_config;
  }

  std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory;

  std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config;

  std::shared_ptr<parquet::encryption::EncryptionConfiguration> encryption_config;
};

struct ARROW_DS_EXPORT DatasetDecryptionConfiguration {
  /// core class, that translates the parameters of high level encryption
  DatasetDecryptionConfiguration()
      : kms_connection_config(
            std::make_shared<parquet::encryption::KmsConnectionConfig>()),
        decryption_config(
            std::make_shared<parquet::encryption::DecryptionConfiguration>()) {}

  void Setup(
      std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory,
      std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config,
      std::shared_ptr<parquet::encryption::DecryptionConfiguration> decryption_config) {
    this->crypto_factory = crypto_factory;
    this->kms_connection_config = kms_connection_config;
    this->decryption_config = decryption_config;
  }

  std::shared_ptr<parquet::encryption::CryptoFactory> crypto_factory;

  std::shared_ptr<parquet::encryption::KmsConnectionConfig> kms_connection_config;

  std::shared_ptr<parquet::encryption::DecryptionConfiguration> decryption_config;
};
}  // namespace dataset
}  // namespace arrow
