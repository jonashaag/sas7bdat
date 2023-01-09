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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/type_fwd.h>
#include <memory>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/stream_writer.h>
#include <parquet/types.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "include/sas7bdat.hpp"
#include "src/encoding_utils.hpp"

std::shared_ptr<arrow::io::FileOutputStream> outfile;
const uint8_t *dataptr;
size_t datasize;
const struct FileInfo *fileinfo;
size_t nrows;
std::unique_ptr<parquet::arrow::FileWriter> pqwriter;

static void noop() {
}

std::thread writerthread;

#define RGSIZE 100000
#define ROW_MULT 10

std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders, builders2;

void on_pagefault(void *userdata, size_t requested_data_start, size_t requested_data_len,
                  size_t *new_data_offset, const uint8_t **new_data, size_t *new_data_len) {
  *new_data_offset = 0;
  *new_data = dataptr;
  *new_data_len = datasize;
}

static void next_batch() {
  builders.clear();
  nrows = 0;

  for (size_t i = 0; i < fileinfo->column_count; ++i) {
    std::shared_ptr<arrow::DataType> type;
    switch (fileinfo->columns[i].format) {
    case column_format_double:
      type = arrow::float64();
      break;
    case column_format_string:
      type = arrow::utf8();
      break;
    case column_format_datetime: {
      type = arrow::timestamp(arrow::TimeUnit::MICRO);
    } break;
    case column_format_date: {
      type = arrow::date32();
    } break;
    }
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
    builder->Resize(RGSIZE);
    builders.push_back(std::move(builder));
  }
}

void on_metadata(void *userdata, const struct FileInfo *f) {
  fileinfo = f;

  next_batch();

  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  for (size_t i = 0; i < fileinfo->column_count; ++i) {
    schema_vector.push_back(arrow::field(fileinfo->columns[i].name, builders[i]->type()));
  }
  arrow::Schema schema(schema_vector);
  auto p = parquet::WriterProperties::Builder();
  p.compression(parquet::Compression::SNAPPY);
  parquet::arrow::FileWriter::Open(schema, arrow::default_memory_pool(), outfile, p.build(),
                                   parquet::default_arrow_writer_properties(), &pqwriter);
}

bool shouldstop;
std::condition_variable cv;
std::mutex cv_m;

static void writerloop() {
  while (1) {
    std::unique_lock lk(cv_m);
    auto start2 = std::chrono::steady_clock::now();
    cv.wait(lk, [] { return shouldstop || builders2.size() != 0; });
    auto end = std::chrono::steady_clock::now();
    std::cout << "waited for reader "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - start2).count()
              << " ms" << std::endl;
    if (shouldstop) {
      return;
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto &builder : builders2) {
      std::shared_ptr<arrow::Array> a;
      builder->Finish(&a);
      arrays.push_back(a);
    }
    std::shared_ptr<arrow::Table> t = arrow::Table::Make(pqwriter->schema(), arrays);
    pqwriter->WriteTable(*t, RGSIZE);
    builders2.clear();
    lk.unlock();
    cv.notify_one();
  }
}

bool on_row(void *userdata, const uint8_t *buf, bool have_fast_space_offsets) {
  for (size_t r = 0; r < ROW_MULT; ++r) {
    for (size_t i = 0; i < fileinfo->column_count; ++i) {

      // todo raw

      const struct ColumnInfo *colinfo = &fileinfo->columns[i];
      if (colinfo->format == column_format_string) {
        auto builder = std::static_pointer_cast<arrow::StringBuilder>(builders[i]);

        size_t len = colinfo->len;
        if (have_fast_space_offsets) {
          size_t last_known_space =
              column_last_known_space_offset((struct Parser *)userdata, colinfo);
          if (last_known_space <= colinfo->offset || last_known_space == SIZE_MAX) {
            len = 0;
          } else {
            len = last_known_space - colinfo->offset;
          }
        }
        while (len > 0 &&
               (buf[colinfo->offset + len - 1] == 0 || buf[colinfo->offset + len - 1] == 0x20)) {
          --len;
        }

        if (len > 0) {
          // todo faster if we make an encoder function ptr per file?
          if (fileinfo->encoding == encoding_ascii || fileinfo->encoding == encoding_utf_8) {
            builder->Append(std::string_view((char *)&buf[colinfo->offset], len));
          } else {
            char out[3 * len];
            len = to_utf_8(&buf[colinfo->offset], len, (uint8_t *)out, 3 * len, fileinfo->encoding);
            assert(len != SIZE_MAX);
            assert(len > 0);
            builder->Append(std::string_view(out, len));
          }
        } else {
          builder->UnsafeAppendNull();
        }
        continue;
      }

      auto need_byteswap = false;    // todo
      auto is_little_endian = false; // todo

      // fast path
      if (colinfo->format == column_format_double && colinfo->len == 8 && !need_byteswap) {
        std::static_pointer_cast<arrow::DoubleBuilder>(builders[i])
            ->UnsafeAppend(*(double *)&buf[colinfo->offset]);
        continue;
      }
      double val;
      if (colinfo->len == 8 && !need_byteswap) {
        val = *(double *)&buf[colinfo->offset];
      } else {
        decimal2double(&buf[colinfo->offset], colinfo->len, is_little_endian, &val);
      }
      if (isnan(val)) {
        // todo assert(column_format_is_floating() || column_format_is_datetime())
        // todo check nans
        if (colinfo->format == column_format_date) {
          std::static_pointer_cast<arrow::Int32Builder>(builders[i])->UnsafeAppendNull();
        } else if (colinfo->format == column_format_datetime) {
          std::static_pointer_cast<arrow::TimestampBuilder>(builders[i])->UnsafeAppendNull();
        } else {
          std::static_pointer_cast<arrow::DoubleBuilder>(builders[i])->UnsafeAppendNull();
        }
        continue;
      }
      if (colinfo->format == column_format_date) {
        std::static_pointer_cast<arrow::Int32Builder>(builders[i])
            ->UnsafeAppend((int32_t)(val - 3653));
      } else if (colinfo->format == column_format_datetime) {
        std::static_pointer_cast<arrow::TimestampBuilder>(builders[i])
            ->UnsafeAppend((int64_t)((val - 3653 * 24 * 3600) * 1e6));
      } else {
        std::static_pointer_cast<arrow::DoubleBuilder>(builders[i])->UnsafeAppend(val);
      };
    }

    ++nrows;

    if (nrows >= RGSIZE) {
      // todo this throws away info about previous chunk (eg. dict?)
      auto start2 = std::chrono::steady_clock::now();
      std::unique_lock lk(cv_m);
      cv.wait(lk, [] { return builders2.size() == 0; });
      auto end = std::chrono::steady_clock::now();
      std::cout << "waited for writer "
                << std::chrono::duration_cast<std::chrono::milliseconds>(end - start2).count()
                << " ms" << std::endl;
      builders2 = builders;
      lk.unlock();
      cv.notify_one();
      next_batch();
    }
  }

  return false;
}

static std::vector<char> ReadAllBytes(char const *filename) {
  std::ifstream ifs(filename, std::ios::binary | std::ios::ate);
  std::ifstream::pos_type pos = ifs.tellg();

  if (pos == 0) {
    return std::vector<char>{};
  }

  std::vector<char> result(pos);

  ifs.seekg(0, std::ios::beg);
  ifs.read(&result[0], pos);

  return result;
}

arrow::Status RunMain(char *f) {
  // auto data = ReadAllBytes(argv[1]);
  int fd = open(f, O_RDONLY);
  struct stat statbuf;
  int err = fstat(fd, &statbuf);
  dataptr = (uint8_t *)mmap(NULL, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
  datasize = statbuf.st_size;

  // dataptr = (uint8_t *)data.data();
  // datasize = data.size();

  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open("test.parquet"));

  struct Parser *parser = (struct Parser *)malloc(parser_struct_size());
  struct ParserConfig config = {
      .userdata = parser,
      .on_pagefault = on_pagefault,
      .on_metadata = on_metadata,
      .on_row = on_row,
      .filesize_override = datasize,
  };
  parser_init(parser, &config);
  shouldstop = false;
  writerthread = std::thread(writerloop);
  bool ret = parse(parser);
  shouldstop = true;
  {
    std::lock_guard g(cv_m);
    cv.notify_one();
  }
  writerthread.join();
  pqwriter->Close();
  printf("ret %d\n", ret);

  return arrow::Status::OK();
}

int main(int argc, char **argv) {
  if (!strcmp(argv[argc - 1], "confirm")) {
    std::cout << "go?\n";
    std::string ignore;
    std::getline(std::cin, ignore);
    --argc;
  }
  auto start = std::chrono::steady_clock::now();

  for (int i = 1; i < argc; ++i) {
    auto start2 = std::chrono::steady_clock::now();
    std::cout << "Processing " << std::string(argv[i]) << "\n";
    arrow::Status st = RunMain(argv[i]);
    auto end = std::chrono::steady_clock::now();
    std::cout << "Elapsed time in milliseconds: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - start2).count()
              << " ms" << std::endl;
    if (!st.ok()) {
      std::cerr << st << std::endl;
      return 1;
    }
  }
  auto end = std::chrono::steady_clock::now();
  std::cout << "Elapsed time in milliseconds: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms"
            << std::endl;
  return 0;
}
