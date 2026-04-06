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

#include "iceberg/file_io_registry.h"

#include <mutex>

namespace iceberg {

namespace {

std::unordered_map<std::string, FileIORegistry::Factory>& Registry() {
  static std::unordered_map<std::string, FileIORegistry::Factory> registry;
  return registry;
}

std::mutex& RegistryMutex() {
  static std::mutex mutex;
  return mutex;
}

}  // namespace

void FileIORegistry::Register(std::string_view name, Factory factory) {
  std::lock_guard lock(RegistryMutex());
  Registry()[std::string(name)] = std::move(factory);
}

Result<std::unique_ptr<FileIO>> FileIORegistry::Load(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties) {
  Factory factory;
  {
    std::lock_guard lock(RegistryMutex());
    auto it = Registry().find(std::string(name));
    if (it == Registry().end()) {
      return NotFound(
          "FileIO implementation '{}' not registered. "
          "Call iceberg::arrow::RegisterFileIO() to register built-in implementations.",
          name);
    }
    factory = it->second;
  }
  return factory(properties);
}

}  // namespace iceberg
