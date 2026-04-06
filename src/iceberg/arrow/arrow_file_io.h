/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/result.h"

namespace iceberg::arrow {

ICEBERG_BUNDLE_EXPORT std::unique_ptr<FileIO> MakeMockFileIO();

ICEBERG_BUNDLE_EXPORT std::unique_ptr<FileIO> MakeLocalFileIO();

/// \brief Create an S3 FileIO backed by Arrow's S3FileSystem.
///
/// Thread-safe: initializes the Arrow S3 subsystem on first call via std::once_flag.
///
/// \param properties S3 configuration properties. See S3Properties for available keys
///        (credentials, region, endpoint, path-style, SSL, etc.). If credentials are not
///        provided, falls back to the default AWS credential chain.
/// \return A FileIO instance for S3 operations, or an error if initialization fails.
ICEBERG_BUNDLE_EXPORT Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties);

/// \brief Finalize the Arrow S3 subsystem, releasing all resources.
///
/// Must be called before process exit or extension unload to avoid shutdown crashes
/// from static destruction order issues in the AWS SDK. Safe to call if S3 was never
/// initialized (no-op). This is a one-way operation -- subsequent MakeS3FileIO() calls
/// will fail.
ICEBERG_BUNDLE_EXPORT Status FinalizeS3();

}  // namespace iceberg::arrow
