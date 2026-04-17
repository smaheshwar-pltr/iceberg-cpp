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

// Test OpenInputFile and OpenOutputStream with URI scheme resolution using
// MockFileSystem.
class OpenFileURITest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto mock_fs = std::make_shared<::arrow::fs::internal::MockFileSystem>(
        std::chrono::system_clock::now());
    mock_fs_ = mock_fs.get();
    file_io_ =
        std::make_unique<iceberg::arrow::ArrowFileSystemFileIO>(std::move(mock_fs));
  }

  ::arrow::fs::internal::MockFileSystem* mock_fs_;
  std::unique_ptr<iceberg::arrow::ArrowFileSystemFileIO> file_io_;
};

TEST_F(OpenFileURITest, RoundTripWithURIScheme) {
  // Write via OpenOutputStream with a URI scheme — the scheme should be stripped.
  const std::string data = "round trip data";
  {
    auto out = file_io_->OpenOutputStream("mock:///data.bin");
    ASSERT_THAT(out, IsOk());
    ASSERT_TRUE((*out)->Write(data.data(), data.size()).ok());
    ASSERT_TRUE((*out)->Close().ok());
  }

  // Read back via OpenInputFile with the same URI scheme.
  {
    auto in = file_io_->OpenInputFile("mock:///data.bin");
    ASSERT_THAT(in, IsOk());
    auto buf_result = (*in)->Read(data.size());
    ASSERT_TRUE(buf_result.ok()) << buf_result.status().ToString();
    auto buf = *buf_result;
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(buf->data()), buf->size()), data);
  }

  // Also readable via plain path (proves the URI was stripped for storage).
  auto read = file_io_->ReadFile("data.bin", std::nullopt);
  ASSERT_THAT(read, IsOk());
  EXPECT_THAT(read, HasValue(::testing::Eq(data)));
}

TEST_F(OpenFileURITest, PlainPathPassesThrough) {
  auto out = file_io_->OpenOutputStream("file.txt");
  ASSERT_THAT(out, IsOk());
  ASSERT_TRUE((*out)->Write("plain", 5).ok());
  ASSERT_TRUE((*out)->Close().ok());

  auto read = file_io_->ReadFile("file.txt", std::nullopt);
  ASSERT_THAT(read, IsOk());
  EXPECT_THAT(read, HasValue(::testing::Eq("plain")));
}

}  // namespace iceberg
