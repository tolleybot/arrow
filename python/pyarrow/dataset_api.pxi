# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from libcpp.memory cimport shared_ptr


cdef api bint pyarrow_is_cryptofactory(object crypto_factory):
    return isinstance(crypto_factory, CryptoFactory)

cdef api shared_ptr[CCryptoFactory] pyarrow_unwrap_cryptofactory(object crypto_factory):
    if pyarrow_is_cryptofactory(crypto_factory):
        pycf = (<CryptoFactory> crypto_factory).unwrap()
        return static_pointer_cast[CCryptoFactory, CPyCryptoFactory](pycf)

    raise TypeError("Expected CryptoFactory, got %s" % type(crypto_factory))


cdef api bint pyarrow_is_kmsconnectionconfig(object kmsconnectionconfig):
    return isinstance(kmsconnectionconfig, KmsConnectionConfig)

cdef api shared_ptr[CKmsConnectionConfig] pyarrow_unwrap_kmsconnectionconfig(object kmsconnectionconfig):
    if pyarrow_is_kmsconnectionconfig(kmsconnectionconfig):
        return (<KmsConnectionConfig> kmsconnectionconfig).unwrap()

    raise TypeError("Expected KmsConnectionConfig, got %s" % type(kmsconnectionconfig))


cdef api bint pyarrow_is_encryptionconfig(object encryptionconfig):
    return isinstance(encryptionconfig, EncryptionConfiguration)

cdef api shared_ptr[CEncryptionConfiguration] pyarrow_unwrap_encryptionconfig(object encryptionconfig):
    if pyarrow_is_encryptionconfig(encryptionconfig):
        return (<EncryptionConfiguration> encryptionconfig).unwrap()

    raise TypeError("Expected EncryptionConfiguration, got %s" % type(encryptionconfig))


cdef api bint pyarrow_is_decryptionconfig(object decryptionconfig):
    return isinstance(decryptionconfig, DecryptionConfiguration)

cdef api shared_ptr[CDecryptionConfiguration] pyarrow_unwrap_decryptionconfig(object decryptionconfig):
    if pyarrow_is_decryptionconfig(decryptionconfig):
        return (<DecryptionConfiguration> decryptionconfig).unwrap()

    raise TypeError("Expected DecryptionConfiguration, got %s" % type(decryptionconfig))
