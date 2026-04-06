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

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/result.h"

#ifdef ICEBERG_S3_ENABLED

#  include <cstdlib>
#  include <mutex>

#  include <arrow/filesystem/s3fs.h>

#  include "iceberg/arrow/arrow_fs_file_io_internal.h"
#  include "iceberg/arrow/arrow_status_internal.h"
#  include "iceberg/arrow/s3_properties.h"
#  include "iceberg/util/macros.h"

namespace iceberg::arrow {

namespace {

std::once_flag g_s3_init_flag;
bool g_s3_initialized = false;

void FinalizeS3Atexit() {
  if (g_s3_initialized) {
    auto status = ::arrow::fs::FinalizeS3();
    g_s3_initialized = false;
    // Best-effort: nothing useful to do with errors at exit
    (void)status;
  }
}

Status EnsureS3Initialized() {
  static ::arrow::Status init_status;
  std::call_once(g_s3_init_flag, [] {
    // TODO(smaheshwar): support options from ::arrow::fs::S3GlobalOptions when needed
    init_status = ::arrow::fs::EnsureS3Initialized();
    if (init_status.ok()) {
      g_s3_initialized = true;
      std::atexit(FinalizeS3Atexit);
    }
  });
  if (!init_status.ok()) {
    return IOError("Arrow S3 initialization failed: {}", init_status.ToString());
  }
  return {};
}

std::string_view GetProperty(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view key) {
  if (auto it = properties.find(std::string(key)); it != properties.end()) {
    return it->second;
  }
  return {};
}

Result<::arrow::fs::S3Options> ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties) {
  ::arrow::fs::S3Options options;

  auto access_key = GetProperty(properties, S3Properties::kAccessKeyId);
  auto secret_key = GetProperty(properties, S3Properties::kSecretAccessKey);
  auto session_token = GetProperty(properties, S3Properties::kSessionToken);

  if (!access_key.empty() && !secret_key.empty()) {
    if (!session_token.empty()) {
      options.ConfigureAccessKey(std::string(access_key), std::string(secret_key),
                                 std::string(session_token));
    } else {
      options.ConfigureAccessKey(std::string(access_key), std::string(secret_key));
    }
  } else {
    options.ConfigureDefaultCredentials();
  }

  if (auto region = GetProperty(properties, S3Properties::kRegion); !region.empty()) {
    options.region = std::string(region);
  }

  if (auto endpoint = GetProperty(properties, S3Properties::kEndpoint);
      !endpoint.empty()) {
    options.endpoint_override = std::string(endpoint);
  }

  if (GetProperty(properties, S3Properties::kPathStyleAccess) == "true") {
    options.force_virtual_addressing = false;
  }

  if (GetProperty(properties, S3Properties::kSslEnabled) == "false") {
    options.scheme = "http";
  }

  return options;
}

}  // namespace

Status FinalizeS3() {
  FinalizeS3Atexit();
  return {};
}

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());

  ICEBERG_ASSIGN_OR_RAISE(auto options, ConfigureS3Options(properties));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::S3FileSystem::Make(options));

  return std::make_unique<ArrowFileSystemFileIO>(std::move(fs));
}

}  // namespace iceberg::arrow

#else  // !ICEBERG_S3_ENABLED

namespace iceberg::arrow {

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotSupported("Arrow S3 support is not enabled");
}

Status FinalizeS3() { return {}; }

}  // namespace iceberg::arrow

#endif  // ICEBERG_S3_ENABLED
