#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <chrono>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "../include/sas7bdat.hpp"
#include "encoding_utils.hpp"
#include "string_utils.hpp"

static auto column_format_to_arrow_type(enum ColumnFormat fmt) {
  switch (fmt) {
  case column_format_double:
    return arrow::float64();
  case column_format_string:
    return arrow::utf8();
  case column_format_datetime:
    return arrow::timestamp(arrow::TimeUnit::MICRO);
  case column_format_date:
    return arrow::date32();
  default:
    assert(0);
  }
}

struct Context {
  // Config
  size_t row_group_size;

  // Reader
  std::chrono::steady_clock::time_point start_time;
  const uint8_t *sas7bdat_data;
  size_t sas7bdat_data_len;
  struct Parser *parser;
  const struct FileInfo *fileinfo;

  size_t n_rows_read, n_rows_read_chunk;
  bool reader_done;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_read;

  // Writer
  std::thread writer;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_write;
  // TODO(feat): support non-file output
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  std::unique_ptr<parquet::arrow::FileWriter> pqwriter;

  // Reader-Writer synchronization
  // TODO(ref): clean up synchronization
  std::condition_variable reader_writer_sync;
  std::mutex reader_writer_sync_mutex;
};

static void report_progress(const struct Context *ctx, bool done) {
  auto duration_any = std::chrono::steady_clock::now() - ctx->start_time;
  auto duration_m =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration_any).count() / 60'000.0;
  auto n_rows_per_m = ctx->n_rows_read / duration_m;
  auto m_left = (ctx->fileinfo->row_count - ctx->n_rows_read) / n_rows_per_m;
  printf("\rProgress: %luk/%luk rows in %.2f minutes, %luk rows/minute, %.2f minutes left",
         ctx->n_rows_read / 1'000, ctx->fileinfo->row_count / 1'000, duration_m,
         (unsigned long)n_rows_per_m / 1'000, m_left);
  if (done) {
    printf("\n");
  }
  fflush(stdout);
}

static auto get_memory_pool(const struct Context *ctx) {
  return arrow::default_memory_pool();
}

static void on_pagefault(const struct Context *ctx, size_t requested_data_start,
                         size_t requested_data_len, size_t *new_data_offset,
                         const uint8_t **new_data, size_t *new_data_len) {
  *new_data_offset = 0;
  *new_data = ctx->sas7bdat_data;
  *new_data_len = ctx->sas7bdat_data_len;
}

static void next_batch(struct Context *ctx) {
  ctx->builders_read.clear();
  ctx->n_rows_read_chunk = 0;

  for (size_t i = 0; i < ctx->fileinfo->column_count; ++i) {
    auto type = column_format_to_arrow_type(ctx->fileinfo->columns[i].format);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(get_memory_pool(ctx), type, &builder);
    builder->Resize(ctx->row_group_size);
    ctx->builders_read.push_back(std::move(builder));
  }
}

static void on_metadata(struct Context *ctx, const struct FileInfo *f) {
  ctx->fileinfo = f;
  ctx->n_rows_read = 0;

  next_batch(ctx);

  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  for (size_t i = 0; i < ctx->fileinfo->column_count; ++i) {
    schema_vector.push_back(
        arrow::field(ctx->fileinfo->columns[i].name, ctx->builders_read[i]->type()));
  };
  arrow::Schema schema(schema_vector);

  auto pqwriter_properties = parquet::WriterProperties::Builder();
  pqwriter_properties.compression(parquet::Compression::SNAPPY);
  parquet::arrow::FileWriter::Open(schema, get_memory_pool(ctx), ctx->outfile,
                                   pqwriter_properties.build(),
                                   parquet::default_arrow_writer_properties(), &ctx->pqwriter);
}

static void writerloop(struct Context *ctx) {
  while (1) {
    std::unique_lock lk(ctx->reader_writer_sync_mutex);
    ctx->reader_writer_sync.wait(
        lk, [ctx] { return ctx->reader_done || ctx->builders_write.size() != 0; });
    if (ctx->reader_done) {
      // todo: missing last chunk? check two cases: number of rows is divisble by chunk or not
      return;
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto &builder : ctx->builders_write) {
      std::shared_ptr<arrow::Array> a;
      builder->Finish(&a);
      arrays.push_back(a);
    }
    std::shared_ptr<arrow::Table> t = arrow::Table::Make(ctx->pqwriter->schema(), arrays);
    ctx->pqwriter->WriteTable(*t, ctx->row_group_size);
    ctx->builders_write.clear();
    lk.unlock();
    ctx->reader_writer_sync.notify_one();
  }
}

static bool on_row(struct Context *ctx, const uint8_t *buf, bool have_fast_space_offsets) {
  for (size_t i = 0; i < ctx->fileinfo->column_count; ++i) {
    const struct ColumnInfo *colinfo = &ctx->fileinfo->columns[i];

    // todo raw

    if (colinfo->format == column_format_string) {
      auto builder = std::static_pointer_cast<arrow::StringBuilder>(ctx->builders_read[i]);
      size_t len = colinfo->len;
      if (len > 0 && have_fast_space_offsets) {
        len = column_fast_max_length(ctx->parser, colinfo);
      }
      if (len > 0) {
        len = rstrip_whitespace(&buf[colinfo->offset], len);
      }
      if (len > 0) {
        if (ctx->fileinfo->encoding == encoding_ascii ||
            ctx->fileinfo->encoding == encoding_utf_8) {
          builder->Append(std::string_view((char *)&buf[colinfo->offset], len));
        } else {
          char out[3 * len];
          len = to_utf_8(&buf[colinfo->offset], len, (uint8_t *)out, 3 * len,
                         ctx->fileinfo->encoding);
          assert(len != SIZE_MAX);
          assert(len > 0);
          builder->Append(std::string_view(out, len));
        }
      } else {
        builder->UnsafeAppendNull();
      }
      continue;
    }

    // fast path
    if (colinfo->format == column_format_double && colinfo->len == 8 &&
        !ctx->fileinfo->need_byteswap) {
      std::static_pointer_cast<arrow::DoubleBuilder>(ctx->builders_read[i])
          ->UnsafeAppend(*(double *)&buf[colinfo->offset]);
      continue;
    }

    double val;
    if (colinfo->len == 8 && !ctx->fileinfo->need_byteswap) {
      val = *(double *)&buf[colinfo->offset];
    } else {
      decimal2double(&buf[colinfo->offset], colinfo->len, ctx->fileinfo->is_little_endian, &val);
    }
    if (isnan(val)) {
      // todo assert(column_format_is_floating() || column_format_is_datetime())
      // todo check nans
      if (colinfo->format == column_format_date) {
        std::static_pointer_cast<arrow::Int32Builder>(ctx->builders_read[i])->UnsafeAppendNull();
      } else if (colinfo->format == column_format_datetime) {
        std::static_pointer_cast<arrow::TimestampBuilder>(ctx->builders_read[i])
            ->UnsafeAppendNull();
      } else {
        std::static_pointer_cast<arrow::DoubleBuilder>(ctx->builders_read[i])->UnsafeAppendNull();
      }
    } else {
      // todo move date calculations to helper function
      if (colinfo->format == column_format_date) {
        std::static_pointer_cast<arrow::Int32Builder>(ctx->builders_read[i])
            ->UnsafeAppend((int32_t)(val - 3653));
      } else if (colinfo->format == column_format_datetime) {
        std::static_pointer_cast<arrow::TimestampBuilder>(ctx->builders_read[i])
            ->UnsafeAppend((int64_t)((val - 3653 * 24 * 3600) * 1e6));
      } else {
        std::static_pointer_cast<arrow::DoubleBuilder>(ctx->builders_read[i])->UnsafeAppend(val);
      };
    }
  }

  ++ctx->n_rows_read_chunk;

  // todo on last row?
  if (ctx->n_rows_read_chunk >= ctx->row_group_size) {
    ctx->n_rows_read += ctx->n_rows_read_chunk;
    report_progress(ctx, false);
    // todo this throws away info about previous chunk (eg. dict?)
    std::unique_lock lk(ctx->reader_writer_sync_mutex);
    ctx->reader_writer_sync.wait(lk, [ctx] { return ctx->builders_write.size() == 0; });
    ctx->builders_write = ctx->builders_read;
    lk.unlock();
    ctx->reader_writer_sync.notify_one();
    next_batch(ctx);
  }

  return false;
}

void sas2parquet(const char *f, const char *o) {
  int fd = open(f, O_RDONLY);
  struct stat statbuf;
  int err = fstat(fd, &statbuf);
  struct Context ctx = {
      .row_group_size = 100'000,
      .start_time = std::chrono::steady_clock::now(),
      .sas7bdat_data = (uint8_t *)mmap(NULL, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0),
      .sas7bdat_data_len = (size_t)statbuf.st_size,
      .parser = (struct Parser *)malloc(parser_struct_size()),
  };
  PARQUET_ASSIGN_OR_THROW(ctx.outfile, arrow::io::FileOutputStream::Open(o));

  struct ParserConfig config = {
      .userdata = &ctx,
      .on_pagefault =
          (void (*)(void *, size_t, size_t, size_t *, const uint8_t **, size_t *))on_pagefault,
      .on_metadata = (void (*)(void *, const struct FileInfo *))on_metadata,
      .on_row = (bool (*)(void *, const uint8_t *, bool))on_row,
      .filesize_override = ctx.sas7bdat_data_len,
  };
  parser_init(ctx.parser, &config);
  ctx.reader_done = false;
  ctx.writer = std::thread(writerloop, &ctx);
  bool ret = parse(ctx.parser);
  ctx.reader_done = true;
  {
    std::lock_guard g(ctx.reader_writer_sync_mutex);
    ctx.reader_writer_sync.notify_one();
  }
  ctx.writer.join();
  ctx.pqwriter->Close();
  report_progress(&ctx, true);
}

int main(int argc, char **argv) {
  if (argc < 3) {
    return 1;
  }
  sas2parquet(argv[1], argv[2]);
  return 0;
}
