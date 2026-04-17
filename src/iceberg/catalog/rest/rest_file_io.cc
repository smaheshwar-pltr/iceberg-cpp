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

#include "iceberg/catalog/rest/rest_file_io.h"

#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

bool IsBuiltinImpl(std::string_view io_impl) {
  return io_impl == FileIORegistry::kArrowLocalFileIO ||
         io_impl == FileIORegistry::kArrowS3FileIO;
}

}  // namespace

Result<BuiltinFileIOKind> DetectBuiltinFileIO(std::string_view location) {
  const auto pos = location.find("://");
  if (pos == std::string_view::npos) {
    return BuiltinFileIOKind::kArrowLocal;
  }

  const auto scheme = location.substr(0, pos);
  if (scheme == "file") {
    return BuiltinFileIOKind::kArrowLocal;
  }
  if (scheme == "s3") {
    return BuiltinFileIOKind::kArrowS3;
  }

  return NotSupported("URI scheme '{}' is not supported for automatic FileIO resolution",
                      scheme);
}

std::string_view BuiltinFileIOName(BuiltinFileIOKind kind) {
  switch (kind) {
    case BuiltinFileIOKind::kArrowLocal:
      return FileIORegistry::kArrowLocalFileIO;
    case BuiltinFileIOKind::kArrowS3:
      return FileIORegistry::kArrowS3FileIO;
  }
  std::unreachable();
}

Result<std::unique_ptr<FileIO>> MakeCatalogFileIO(const RestCatalogProperties& config) {
  std::string io_impl = config.Get(RestCatalogProperties::kIOImpl);
  std::string warehouse = config.Get(RestCatalogProperties::kWarehouse);

  if (io_impl.empty()) {
    if (warehouse.empty()) {
      // No io-impl or warehouse configured. Fall back to a local FileIO as a
      // default — enabling per-table ResolveTableFileIO (vending).
      io_impl = std::string(FileIORegistry::kArrowLocalFileIO);
    } else {
      ICEBERG_ASSIGN_OR_RAISE(const auto detected_kind, DetectBuiltinFileIO(warehouse));
      io_impl = std::string(BuiltinFileIOName(detected_kind));
    }
  }

  if (!warehouse.empty() && IsBuiltinImpl(io_impl)) {
    ICEBERG_ASSIGN_OR_RAISE(const auto detected_kind, DetectBuiltinFileIO(warehouse));
    const auto detected_name = BuiltinFileIOName(detected_kind);
    if (io_impl != detected_name) {
      return InvalidArgument(
          R"("io-impl" value '{}' is incompatible with warehouse '{}')", io_impl,
          warehouse);
    }
  }

  // TODO(gangwu): Support Java-style customized FileIO creation flows instead of
  // resolving a single catalog-scoped FileIO instance only from properties.
  return FileIORegistry::Load(io_impl, config.configs());
}

namespace {

const StorageCredential* ResolveStorageCredential(
    const std::vector<StorageCredential>& credentials, std::string_view location) {
  const StorageCredential* best = nullptr;
  for (const auto& cred : credentials) {
    if (location.starts_with(cred.prefix)) {
      if (!best || cred.prefix.size() > best->prefix.size()) {
        best = &cred;
      }
    }
  }
  return best;
}

std::unordered_map<std::string, std::string> MergeTableProperties(
    const std::unordered_map<std::string, std::string>& catalog_props,
    const std::unordered_map<std::string, std::string>& table_config,
    const std::unordered_map<std::string, std::string>& credential_config) {
  auto merged = catalog_props;
  for (const auto& [k, v] : table_config) {
    merged[k] = v;
  }
  for (const auto& [k, v] : credential_config) {
    merged[k] = v;
  }
  return merged;
}

}  // namespace

Result<std::shared_ptr<FileIO>> ResolveTableFileIO(
    const std::shared_ptr<FileIO>& catalog_io,
    const std::unordered_map<std::string, std::string>& catalog_props,
    const std::string& warehouse, const LoadTableResult& result) {
  if (result.config.empty() && result.storage_credentials.empty()) {
    return catalog_io;
  }

  // Merge order: catalog props < table config < storage credentials (highest priority).
  const StorageCredential* cred = nullptr;
  if (!result.metadata_location.empty()) {
    cred = ResolveStorageCredential(result.storage_credentials, result.metadata_location);
  }
  static const std::unordered_map<std::string, std::string> kEmpty;
  auto merged =
      MergeTableProperties(catalog_props, result.config, cred ? cred->config : kEmpty);

  // Detect FileIO type: explicit io-impl > warehouse scheme > metadata_location scheme.
  std::string io_impl;
  if (auto it = merged.find(std::string(RestCatalogProperties::kIOImpl.key()));
      it != merged.end()) {
    io_impl = it->second;
  } else if (!warehouse.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto kind, DetectBuiltinFileIO(warehouse));
    io_impl = std::string(BuiltinFileIOName(kind));
  } else if (!result.metadata_location.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto kind, DetectBuiltinFileIO(result.metadata_location));
    io_impl = std::string(BuiltinFileIOName(kind));
  } else {
    return catalog_io;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto table_io, FileIORegistry::Load(io_impl, merged));
  return std::shared_ptr<FileIO>(std::move(table_io));
}

}  // namespace iceberg::rest
