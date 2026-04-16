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

#include <chrono>

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/mockfs.h>
#include <arrow/io/interfaces.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"

namespace iceberg {

class LocalFileIOTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    temp_filepath_ = CreateNewTempFilePath();
  }

  std::shared_ptr<iceberg::FileIO> file_io_;
  std::string temp_filepath_;
};

TEST_F(LocalFileIOTest, ReadWriteFile) {
  auto read_res = file_io_->ReadFile(temp_filepath_, std::nullopt);
  EXPECT_THAT(read_res, IsError(ErrorKind::kIOError));
  EXPECT_THAT(read_res, HasErrorMessage("Failed to open local file"));

  auto write_res = file_io_->WriteFile(temp_filepath_, "hello world");
  EXPECT_THAT(write_res, IsOk());

  read_res = file_io_->ReadFile(temp_filepath_, std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello world")));
}

TEST_F(LocalFileIOTest, DeleteFile) {
  auto write_res = file_io_->WriteFile(temp_filepath_, "hello world");
  EXPECT_THAT(write_res, IsOk());

  auto del_res = file_io_->DeleteFile(temp_filepath_);
  EXPECT_THAT(del_res, IsOk());

  del_res = file_io_->DeleteFile(temp_filepath_);
  EXPECT_THAT(del_res, IsError(ErrorKind::kIOError));
  EXPECT_THAT(del_res, HasErrorMessage("Cannot delete file"));
}

// Test fixture for ArrowFileSystemFileIO with a mock filesystem, used to test
// OpenInputFile and OpenOutputStream with URI resolution.
class MockFileIOTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto mock_fs = std::make_shared<::arrow::fs::internal::MockFileSystem>(
        std::chrono::system_clock::now());
    mock_fs_ = mock_fs.get();
    file_io_ = std::make_unique<iceberg::arrow::ArrowFileSystemFileIO>(std::move(mock_fs));
  }

  /// \brief Helper to create a file in the mock filesystem with recursive directory
  /// creation.
  void CreateMockFile(const std::string& path, std::string_view content) {
    ASSERT_TRUE(mock_fs_->CreateFile(path, content).ok());
  }

  ::arrow::fs::internal::MockFileSystem* mock_fs_;
  std::unique_ptr<iceberg::arrow::ArrowFileSystemFileIO> file_io_;
};

TEST_F(MockFileIOTest, OpenOutputStreamPlainPath) {
  // A plain path without a URI scheme should pass through unchanged.
  auto result = file_io_->OpenOutputStream("file.txt");
  EXPECT_THAT(result, IsOk());

  // Write some data and close the stream.
  auto& stream = *result;
  auto status = stream->Write("plain path data", 15);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(stream->Close().ok());

  // Verify the file exists at the plain path by reading it back.
  auto read_res = file_io_->ReadFile("file.txt", std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("plain path data")));
}

TEST_F(MockFileIOTest, OpenOutputStreamWithURIScheme) {
  // A path with mock:/// URI scheme should be resolved to strip the scheme.
  auto result = file_io_->OpenOutputStream("mock:///file.txt");
  EXPECT_THAT(result, IsOk());

  auto& stream = *result;
  auto status = stream->Write("uri data", 8);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(stream->Close().ok());

  // Verify the file exists at the resolved path (without mock:/// prefix).
  auto read_res = file_io_->ReadFile("file.txt", std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("uri data")));
}

TEST_F(MockFileIOTest, OpenInputFilePlainPath) {
  // Create a file using the mock filesystem helper (which creates dirs recursively).
  CreateMockFile("input.txt", "hello input");

  // Open the file using OpenInputFile with a plain path.
  auto result = file_io_->OpenInputFile("input.txt");
  EXPECT_THAT(result, IsOk());

  auto& file = *result;
  auto size_result = file->GetSize();
  ASSERT_TRUE(size_result.ok()) << size_result.status().ToString();
  EXPECT_EQ(*size_result, 11);

  auto buf_result = file->Read(*size_result);
  ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
  auto buf = *buf_result;
  std::string content(reinterpret_cast<const char*>(buf->data()), buf->size());
  EXPECT_EQ(content, "hello input");
}

TEST_F(MockFileIOTest, OpenInputFileWithURIScheme) {
  // Create a file at a plain path.
  CreateMockFile("input.txt", "uri input data");

  // Open the file using OpenInputFile with a mock:/// URI — the scheme should be
  // stripped, resolving to the same plain path.
  auto result = file_io_->OpenInputFile("mock:///input.txt");
  EXPECT_THAT(result, IsOk());

  auto& file = *result;
  auto size_result = file->GetSize();
  ASSERT_TRUE(size_result.ok()) << size_result.status().ToString();
  EXPECT_EQ(*size_result, 14);

  auto buf_result = file->Read(*size_result);
  ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
  auto buf = *buf_result;
  std::string content(reinterpret_cast<const char*>(buf->data()), buf->size());
  EXPECT_EQ(content, "uri input data");
}

TEST_F(MockFileIOTest, OpenInputFileWithLengthHint) {
  // Create a file.
  CreateMockFile("sized.txt", "sized content");

  // Open with a length hint via URI scheme.
  auto result = file_io_->OpenInputFile("mock:///sized.txt", 13);
  EXPECT_THAT(result, IsOk());

  auto& file = *result;
  auto buf_result = file->Read(13);
  ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
  auto buf = *buf_result;
  std::string content(reinterpret_cast<const char*>(buf->data()), buf->size());
  EXPECT_EQ(content, "sized content");
}

TEST_F(MockFileIOTest, RoundTripViaOpenOutputStreamAndOpenInputFile) {
  // Write via OpenOutputStream with URI scheme.
  const std::string test_data = "round trip test data with special chars: \xc3\xa9\xc3\xa0";
  {
    auto out_result = file_io_->OpenOutputStream("mock:///data.bin");
    EXPECT_THAT(out_result, IsOk());
    auto& stream = *out_result;
    auto status = stream->Write(test_data.data(), test_data.size());
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(stream->Close().ok());
  }

  // Read back via OpenInputFile with URI scheme.
  {
    auto in_result = file_io_->OpenInputFile("mock:///data.bin");
    EXPECT_THAT(in_result, IsOk());
    auto& file = *in_result;

    auto size_result = file->GetSize();
    ASSERT_TRUE(size_result.ok()) << size_result.status().ToString();
    EXPECT_EQ(static_cast<size_t>(*size_result), test_data.size());

    auto buf_result = file->Read(*size_result);
    ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
    auto buf = *buf_result;
    std::string content(reinterpret_cast<const char*>(buf->data()), buf->size());
    EXPECT_EQ(content, test_data);
  }

  // Also verify we can read the same file with a plain path (no URI scheme).
  {
    auto in_result = file_io_->OpenInputFile("data.bin");
    EXPECT_THAT(in_result, IsOk());
    auto& file = *in_result;

    auto size_result = file->GetSize();
    ASSERT_TRUE(size_result.ok()) << size_result.status().ToString();

    auto buf_result = file->Read(*size_result);
    ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
    auto buf = *buf_result;
    std::string content(reinterpret_cast<const char*>(buf->data()), buf->size());
    EXPECT_EQ(content, test_data);
  }
}

TEST_F(MockFileIOTest, OpenOutputStreamWithNestedURIPath) {
  // Create the parent directory structure so OpenOutputStream can create the file.
  ASSERT_TRUE(mock_fs_->CreateDir("bucket/key").ok());

  auto result = file_io_->OpenOutputStream("mock:///bucket/key/nested.txt");
  EXPECT_THAT(result, IsOk());

  auto& stream = *result;
  auto status = stream->Write("nested data", 11);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(stream->Close().ok());

  // Read back with a plain path to confirm URI scheme was stripped.
  auto read_res = file_io_->ReadFile("bucket/key/nested.txt", std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("nested data")));
}

// Test URI resolution with the local filesystem and file:// scheme.
class LocalFileIOURITest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    file_io_ = std::make_unique<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    temp_filepath_ = CreateNewTempFilePath();
  }

  std::unique_ptr<iceberg::arrow::ArrowFileSystemFileIO> file_io_;
  std::string temp_filepath_;
};

TEST_F(LocalFileIOURITest, OpenOutputStreamWithFileScheme) {
  // Write using file:// URI.
  std::string file_uri = "file://" + temp_filepath_;
  auto out_result = file_io_->OpenOutputStream(file_uri);
  EXPECT_THAT(out_result, IsOk());

  auto& stream = *out_result;
  std::string data = "file scheme data";
  auto status = stream->Write(data.data(), data.size());
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_TRUE(stream->Close().ok());

  // Read back using the plain path.
  auto read_res = file_io_->ReadFile(temp_filepath_, std::nullopt);
  EXPECT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("file scheme data")));
}

TEST_F(LocalFileIOURITest, OpenInputFileWithFileScheme) {
  // Write using plain path.
  auto write_res = file_io_->WriteFile(temp_filepath_, "file input data");
  EXPECT_THAT(write_res, IsOk());

  // Read using file:// URI.
  std::string file_uri = "file://" + temp_filepath_;
  auto in_result = file_io_->OpenInputFile(file_uri);
  EXPECT_THAT(in_result, IsOk());

  auto& file = *in_result;
  auto size_result = file->GetSize();
  ASSERT_TRUE(size_result.ok()) << size_result.status().ToString();

  auto buf_result = file->Read(*size_result);
  ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
  auto buf = *buf_result;
  std::string content(reinterpret_cast<const char*>(buf->data()), buf->size());
  EXPECT_EQ(content, "file input data");
}

}  // namespace iceberg
