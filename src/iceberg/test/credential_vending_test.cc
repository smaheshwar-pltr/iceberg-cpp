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

/// \file credential_vending_test.cc
/// \brief Tests for credential vending: ResolveTableFileIO, FileIO detection,
/// and FileIO registry integration.

#include <memory>
#include <string>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/rest_file_io.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// ---------------------------------------------------------------------------
// DetectBuiltinFileIO tests
// ---------------------------------------------------------------------------

TEST(CredentialVendingFileIOTest, DetectS3SchemeReturnsArrowS3) {
  auto result = rest::DetectBuiltinFileIO("s3://bucket/path");
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(*result, rest::BuiltinFileIOKind::kArrowS3);
}

TEST(CredentialVendingFileIOTest, DetectLocalPathReturnsArrowLocal) {
  auto result = rest::DetectBuiltinFileIO("/tmp/warehouse");
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(*result, rest::BuiltinFileIOKind::kArrowLocal);
}

TEST(CredentialVendingFileIOTest, DetectFileSchemeReturnsArrowLocal) {
  auto result = rest::DetectBuiltinFileIO("file:///tmp/warehouse");
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(*result, rest::BuiltinFileIOKind::kArrowLocal);
}

TEST(CredentialVendingFileIOTest, DetectUnsupportedSchemeReturnsError) {
  auto result = rest::DetectBuiltinFileIO("gs://bucket/warehouse");
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

TEST(CredentialVendingFileIOTest, BuiltinFileIONameForS3) {
  EXPECT_EQ(rest::BuiltinFileIOName(rest::BuiltinFileIOKind::kArrowS3),
            FileIORegistry::kArrowS3FileIO);
}

TEST(CredentialVendingFileIOTest, BuiltinFileIONameForLocal) {
  EXPECT_EQ(rest::BuiltinFileIOName(rest::BuiltinFileIOKind::kArrowLocal),
            FileIORegistry::kArrowLocalFileIO);
}

// ---------------------------------------------------------------------------
// ResolveTableFileIO tests
// ---------------------------------------------------------------------------

namespace {

constexpr std::string_view kTestFileIOImpl = "test.credential-vending.TestFileIO";

class DummyFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string&, std::optional<size_t>) override {
    return std::string("dummy");
  }
  Status WriteFile(const std::string&, std::string_view) override { return {}; }
  Status DeleteFile(const std::string&) override { return {}; }
};

rest::LoadTableResult MakeLoadResult(
    std::string metadata_location,
    std::unordered_map<std::string, std::string> config = {},
    std::vector<rest::StorageCredential> creds = {}) {
  return rest::LoadTableResult{
      .metadata_location = std::move(metadata_location),
      .metadata = std::make_shared<TableMetadata>(TableMetadata{.format_version = 2}),
      .config = std::move(config),
      .storage_credentials = std::move(creds),
  };
}

class ResolveTableFileIOTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    FileIORegistry::Register(std::string(kTestFileIOImpl),
                             [](const std::unordered_map<std::string, std::string>&)
                                 -> Result<std::unique_ptr<FileIO>> {
                               return std::make_unique<DummyFileIO>();
                             });
  }

  std::shared_ptr<FileIO> catalog_io_ = std::make_shared<DummyFileIO>();
  std::unordered_map<std::string, std::string> catalog_props_ = {
      {"io-impl", std::string(kTestFileIOImpl)},
      {"s3.region", "us-east-1"},
  };
  std::string warehouse_ = "file:///tmp/warehouse";
};

}  // namespace

TEST_F(ResolveTableFileIOTest, NoConfigReturnsCatalogIO) {
  auto result = rest::ResolveTableFileIO(catalog_io_, catalog_props_, warehouse_,
                                         MakeLoadResult("s3://bucket/meta.json"));
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value().get(), catalog_io_.get());
}

TEST_F(ResolveTableFileIOTest, TableConfigMergedIntoProperties) {
  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult("s3://bucket/meta.json", {{"s3.access-key-id", "TABLE_KEY"}}));
  ASSERT_THAT(result, IsOk());
  const auto& props = result.value()->properties();
  EXPECT_EQ(props.at("s3.access-key-id"), "TABLE_KEY");
  EXPECT_EQ(props.at("s3.region"), "us-east-1");
}

TEST_F(ResolveTableFileIOTest, StorageCredentialsOverrideTableConfig) {
  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult("s3://bucket/warehouse/db/table/meta.json",
                     {{"s3.access-key-id", "TABLE_KEY"}},
                     {{.prefix = "s3://bucket/warehouse/",
                       .config = {{"s3.access-key-id", "CRED_KEY"},
                                  {"s3.session-token", "CRED_TOKEN"}}}}));
  ASSERT_THAT(result, IsOk());
  const auto& props = result.value()->properties();
  EXPECT_EQ(props.at("s3.access-key-id"), "CRED_KEY");
  EXPECT_EQ(props.at("s3.session-token"), "CRED_TOKEN");
  EXPECT_EQ(props.at("s3.region"), "us-east-1");
}

TEST_F(ResolveTableFileIOTest, LongestPrefixMatchWins) {
  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult(
          "s3://bucket/warehouse/db/table/meta.json", {},
          {{.prefix = "s3://bucket/", .config = {{"s3.access-key-id", "SHORT"}}},
           {.prefix = "s3://bucket/warehouse/db/",
            .config = {{"s3.access-key-id", "LONG"}}},
           {.prefix = "s3://other/", .config = {{"s3.access-key-id", "WRONG"}}}}));
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "LONG");
}

TEST_F(ResolveTableFileIOTest, NoMatchingCredentialIgnored) {
  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult(
          "s3://my-bucket/meta.json", {},
          {{.prefix = "s3://other-bucket/", .config = {{"s3.access-key-id", "WRONG"}}}}));
  ASSERT_THAT(result, IsOk());
  EXPECT_FALSE(result.value()->properties().contains("s3.access-key-id"));
}

TEST_F(ResolveTableFileIOTest, EmptyMetadataLocationIgnoresCredentials) {
  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult("",  // empty metadata location
                     {{"s3.access-key-id", "TABLE_KEY"}},
                     {{.prefix = "", .config = {{"s3.access-key-id", "WRONG"}}}}));
  ASSERT_THAT(result, IsOk());
  // Table config should be merged, but the credential should NOT match
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "TABLE_KEY");
}

TEST_F(ResolveTableFileIOTest, FileIOPropertiesPopulated) {
  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult("s3://bucket/meta.json",
                     {{"s3.access-key-id", "KEY"}, {"s3.secret-access-key", "SECRET"}}));
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "KEY");
  EXPECT_EQ(result.value()->properties().at("s3.secret-access-key"), "SECRET");
}

TEST_F(ResolveTableFileIOTest, TableConfigOverridesIOImpl) {
  // Register a second FileIO impl to verify io-impl override from table config
  const std::string alt_impl = "test.credential-vending.AltFileIO";
  FileIORegistry::Register(
      alt_impl,
      [](const std::unordered_map<std::string, std::string>&)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<DummyFileIO>(); });

  auto result = rest::ResolveTableFileIO(
      catalog_io_, catalog_props_, warehouse_,
      MakeLoadResult("s3://bucket/meta.json",
                     {{"io-impl", alt_impl}, {"s3.access-key-id", "KEY"}}));
  ASSERT_THAT(result, IsOk());
  // Should use the alt impl and still have the merged properties
  EXPECT_NE(result.value().get(), catalog_io_.get());
  EXPECT_EQ(result.value()->properties().at("io-impl"), alt_impl);
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "KEY");
}

TEST_F(ResolveTableFileIOTest, DetectsFileIOFromWarehouseScheme) {
  // No io-impl in config, but warehouse is s3:// — should detect S3 FileIO
  const std::string s3_impl = std::string(FileIORegistry::kArrowS3FileIO);
  FileIORegistry::Register(
      s3_impl,
      [](const std::unordered_map<std::string, std::string>&)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<DummyFileIO>(); });

  std::unordered_map<std::string, std::string> no_io_impl_props = {
      {"s3.region", "us-east-1"},
  };
  auto result =
      rest::ResolveTableFileIO(catalog_io_, no_io_impl_props, "s3://warehouse-bucket/",
                               MakeLoadResult("s3://warehouse-bucket/db/meta.json",
                                              {{"s3.access-key-id", "KEY"}}));
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value().get(), catalog_io_.get());
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "KEY");
}

TEST_F(ResolveTableFileIOTest, DetectsFileIOFromMetadataLocationWhenNoWarehouse) {
  // When no warehouse or io-impl, detect from metadata_location scheme (like Python)
  const std::string s3_impl = std::string(FileIORegistry::kArrowS3FileIO);
  FileIORegistry::Register(
      s3_impl,
      [](const std::unordered_map<std::string, std::string>&)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<DummyFileIO>(); });

  std::unordered_map<std::string, std::string> no_io_impl_props = {
      {"s3.region", "us-east-1"},
  };
  auto result = rest::ResolveTableFileIO(
      catalog_io_, no_io_impl_props, "",  // empty warehouse
      MakeLoadResult("s3://bucket/meta.json", {{"s3.access-key-id", "KEY"}}));
  ASSERT_THAT(result, IsOk());
  // Should create a new S3 FileIO, not fall back to catalog_io
  EXPECT_NE(result.value().get(), catalog_io_.get());
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "KEY");
}

TEST_F(ResolveTableFileIOTest, FallsToCatalogIOWhenNothingDetectable) {
  std::unordered_map<std::string, std::string> no_io_impl_props = {
      {"s3.region", "us-east-1"},
  };
  auto result = rest::ResolveTableFileIO(
      catalog_io_, no_io_impl_props, "",                   // empty warehouse
      MakeLoadResult("", {{"s3.access-key-id", "KEY"}}));  // empty metadata_location
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value().get(), catalog_io_.get());
}

// ---------------------------------------------------------------------------
// FileIORegistry tests
// ---------------------------------------------------------------------------

TEST(CredentialVendingFileIOTest, RegistryLoadUnknownImplReturnsNotFound) {
  auto result = FileIORegistry::Load("nonexistent.impl", {});
  EXPECT_THAT(result, IsError(ErrorKind::kNotFound));
}

TEST(CredentialVendingFileIOTest, RegistryPopulatesFileIOProperties) {
  const std::string impl_name = "test.credential-vending.PropsFileIO";
  FileIORegistry::Register(
      impl_name,
      [](const std::unordered_map<std::string, std::string>&)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<DummyFileIO>(); });

  std::unordered_map<std::string, std::string> test_props = {
      {"s3.access-key-id", "AKIA_TEST"},
      {"s3.secret-access-key", "SECRET_TEST"},
  };

  auto result = FileIORegistry::Load(impl_name, test_props);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->properties().at("s3.access-key-id"), "AKIA_TEST");
  EXPECT_EQ(result.value()->properties().at("s3.secret-access-key"), "SECRET_TEST");
}

}  // namespace iceberg
