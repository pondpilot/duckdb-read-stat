#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <ctype.h>
#if defined(_MSC_VER)

#define strncasecmp _strnicmp
#define strcasecmp _stricmp

#else
#include <strings.h>
#endif

#include "duckdb_extension.h"

#include "duckdb_read_stat.h"

DUCKDB_EXTENSION_EXTERN

static duckdb_connection g_read_stat_conn = NULL;

#ifdef DUCKDB_WASM_EXTENSION
typedef struct duckdb_web_response {
    double statusCode;
    double dataOrValue;
    double dataSize;
} duckdb_web_response;

extern void duckdb_web_clear_response(void);
extern void duckdb_web_fs_get_file_info_by_name(duckdb_web_response *packed, const char *file_name, size_t cache_epoch);
extern void duckdb_web_copy_file_to_buffer(duckdb_web_response *packed, const char *path);
extern void *duckdb_web_fs_file_open(size_t file_id, uint8_t flags);
extern void duckdb_web_fs_file_close(size_t file_id);
extern ssize_t duckdb_web_fs_file_read(size_t file_id, void *buffer, ssize_t bytes, double location);
#endif

typedef struct duckdb_read_stat_buffer_ctx {
    const uint8_t *data;
    idx_t size;
    idx_t pos;
} duckdb_read_stat_buffer_ctx;

static int duckdb_read_stat_buffer_open(const char *path, void *io_ctx)
{
    (void)path;
    (void)io_ctx;
    return 0;
}

static int duckdb_read_stat_buffer_close(void *io_ctx)
{
    (void)io_ctx;
    return 0;
}

static readstat_off_t duckdb_read_stat_buffer_seek(readstat_off_t offset, readstat_io_flags_t whence, void *io_ctx)
{
    duckdb_read_stat_buffer_ctx *ctx = (duckdb_read_stat_buffer_ctx *)io_ctx;
    readstat_off_t newpos = -1;

    if (whence == READSTAT_SEEK_SET)
    {
        newpos = offset;
    }
    else if (whence == READSTAT_SEEK_CUR)
    {
        newpos = (readstat_off_t)ctx->pos + offset;
    }
    else if (whence == READSTAT_SEEK_END)
    {
        newpos = (readstat_off_t)ctx->size + offset;
    }

    if (newpos < 0)
    {
        return -1;
    }
    if ((idx_t)newpos > ctx->size)
    {
        return -1;
    }

    ctx->pos = (idx_t)newpos;
    return newpos;
}

static ssize_t duckdb_read_stat_buffer_read(void *buf, size_t nbytes, void *io_ctx)
{
    duckdb_read_stat_buffer_ctx *ctx = (duckdb_read_stat_buffer_ctx *)io_ctx;
    ssize_t bytes_left = (ssize_t)ctx->size - (ssize_t)ctx->pos;
    ssize_t bytes_copied = 0;

    if (bytes_left <= 0)
    {
        return 0;
    }

    if ((ssize_t)nbytes <= bytes_left)
    {
        memcpy(buf, ctx->data + ctx->pos, nbytes);
        bytes_copied = (ssize_t)nbytes;
    }
    else
    {
        memcpy(buf, ctx->data + ctx->pos, (size_t)bytes_left);
        bytes_copied = bytes_left;
    }

    ctx->pos += (idx_t)bytes_copied;
    return bytes_copied;
}

static readstat_error_t duckdb_read_stat_buffer_update(long file_size, readstat_progress_handler progress_handler,
                                                       void *user_ctx, void *io_ctx)
{
    (void)file_size;
    if (!progress_handler)
    {
        return READSTAT_OK;
    }

    duckdb_read_stat_buffer_ctx *ctx = (duckdb_read_stat_buffer_ctx *)io_ctx;
    double progress = ctx->size == 0 ? 1.0 : (double)ctx->pos / (double)ctx->size;

    if (progress_handler(progress, user_ctx))
    {
        return READSTAT_ERROR_USER_ABORT;
    }

    return READSTAT_OK;
}

static void duckdb_read_stat_set_error_message(char **target, const char *message)
{
    if (!target || !message)
    {
        return;
    }
    if (*target)
    {
        duckdb_free(*target);
    }
    *target = (char *)duckdb_malloc(strlen(message) + 1);
    strcpy(*target, message);
}

static char *duckdb_read_stat_escape_sql_string(const char *input)
{
    size_t len = 0;
    for (const char *p = input; *p; p++)
    {
        len += (*p == '\'') ? 2 : 1;
    }

    char *out = (char *)duckdb_malloc(len + 1);
    char *dst = out;
    for (const char *p = input; *p; p++)
    {
        if (*p == '\'')
        {
            *dst++ = '\'';
            *dst++ = '\'';
        }
        else
        {
            *dst++ = *p;
        }
    }
    *dst = '\0';
    return out;
}

typedef enum duckdb_read_stat_temporal_type
{
    DUCKDB_READ_STAT_TEMPORAL_NONE,
    DUCKDB_READ_STAT_TEMPORAL_DATE,
    DUCKDB_READ_STAT_TEMPORAL_DATETIME,
    DUCKDB_READ_STAT_TEMPORAL_TIME
} duckdb_read_stat_temporal_type;

static duckdb_read_stat_temporal_type duckdb_read_stat_classify_format(const char *format)
{
    if (!format)
    {
        return DUCKDB_READ_STAT_TEMPORAL_NONE;
    }

    // Date formats
    if (
        // SAS
        !strcmp(format, "WEEKDATE") || !strcmp(format, "MMDDYY") || !strcmp(format, "DDMMYY") || !strcmp(format, "YYMMDD") || !strcmp(format, "DATE") || !strcmp(format, "DATE9") || !strcmp(format, "YYMMDD10") || !strcmp(format, "DDMMYYB") || !strcmp(format, "DDMMYYB10") || !strcmp(format, "DDMMYYC") || !strcmp(format, "DDMMYYC10") || !strcmp(format, "DDMMYYD") || !strcmp(format, "DDMMYYD10") || !strcmp(format, "DDMMYYN6") || !strcmp(format, "DDMMYYN8") || !strcmp(format, "DDMMYYP") || !strcmp(format, "DDMMYYP10") || !strcmp(format, "DDMMYYS") || !strcmp(format, "DDMMYYS10") || !strcmp(format, "MMDDYYB") || !strcmp(format, "MMDDYYB10") || !strcmp(format, "MMDDYYC") || !strcmp(format, "MMDDYYC10") || !strcmp(format, "MMDDYYD") || !strcmp(format, "MMDDYYD10") || !strcmp(format, "MMDDYYN6") || !strcmp(format, "MMDDYYN8") || !strcmp(format, "MMDDYYP") || !strcmp(format, "MMDDYYP10") || !strcmp(format, "MMDDYYS") || !strcmp(format, "MMDDYYS10") || !strcmp(format, "WEEKDATX") || !strcmp(format, "DTDATE") || !strcmp(format, "IS8601DA") || !strcmp(format, "E8601DA") || !strcmp(format, "B8601DA") || !strcmp(format, "YYMMDDB") || !strcmp(format, "YYMMDDD") || !strcmp(format, "YYMMDDN") || !strcmp(format, "YYMMDDP") || !strcmp(format, "YYMMDDS")
        // SPSS
        || !strcmp(format, "DATE8") || !strcmp(format, "DATE11") || !strcmp(format, "DATE12") || !strcmp(format, "ADATE") || !strcmp(format, "ADATE8") || !strcmp(format, "ADATE10") || !strcmp(format, "EDATE") || !strcmp(format, "EDATE8") || !strcmp(format, "EDATE10") || !strcmp(format, "JDATE") || !strcmp(format, "JDATE5") || !strcmp(format, "JDATE7") || !strcmp(format, "SDATE") || !strcmp(format, "SDATE8") || !strcmp(format, "SDATE10")
        // Stata
        || !strcmp(format, "%td") || !strcmp(format, "%d") || !strcmp(format, "%tdD_m_Y") || !strcmp(format, "%tdCCYY-NN-DD"))
    {
        return DUCKDB_READ_STAT_TEMPORAL_DATE;
    }

    // Datetime formats
    if (
        // SAS
        !strcmp(format, "DATETIME") || !strcmp(format, "DATETIME18") || !strcmp(format, "DATETIME19") || !strcmp(format, "DATETIME20") || !strcmp(format, "DATETIME21") || !strcmp(format, "DATETIME22") || !strcmp(format, "E8601DT") || !strcmp(format, "DATEAMPM") || !strcmp(format, "MDYAMPM") || !strcmp(format, "IS8601DT") || !strcmp(format, "B8601DT") || !strcmp(format, "B8601DN")
        // SPSS
        || !strcmp(format, "DATETIME8") || !strcmp(format, "DATETIME17") || !strcmp(format, "DATETIME23.2") || !strcmp(format, "YMDHMS16") || !strcmp(format, "YMDHMS19") || !strcmp(format, "YMDHMS19.2") || !strcmp(format, "YMDHMS20")
        // Stata
        || !strcmp(format, "%tC") || !strcmp(format, "%tc"))
    {
        return DUCKDB_READ_STAT_TEMPORAL_DATETIME;
    }

    // Time formats
    if (
        // SAS
        !strcmp(format, "TIME") || !strcmp(format, "HHMM") || !strcmp(format, "TIME20.3") || !strcmp(format, "TIME20") || !strcmp(format, "TIME5") || !strcmp(format, "TOD") || !strcmp(format, "TIMEAMPM") || !strcmp(format, "IS8601TM") || !strcmp(format, "E8601TM") || !strcmp(format, "B8601TM")
        // SPSS
        || !strcmp(format, "DTIME") || !strcmp(format, "TIME8") || !strcmp(format, "TIME5") || !strcmp(format, "TIME11.2")
        // Stata
        || !strcmp(format, "%tcHH:MM:SS") || !strcmp(format, "%tcHH:MM"))
    {
        return DUCKDB_READ_STAT_TEMPORAL_TIME;
    }

    return DUCKDB_READ_STAT_TEMPORAL_NONE;
}

static readstat_error_t duckdb_read_stat_dispatch_parse(readstat_parser_t *parser, const char *path,
                                                        const char *format,
                                                        duckdb_read_stat_file_format *file_format, void *ctx)
{
    if (format != NULL)
    {
        if (!strcasecmp(format, "sas7bdat"))
        {
            if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SAS;
            return readstat_parse_sas7bdat(parser, path, ctx);
        }
        else if (!strcasecmp(format, "xpt"))
        {
            if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SAS;
            return readstat_parse_xport(parser, path, ctx);
        }
        else if (!strcasecmp(format, "sav") || !strcasecmp(format, "zsav"))
        {
            if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SPSS;
            return readstat_parse_sav(parser, path, ctx);
        }
        else if (!strcasecmp(format, "por"))
        {
            if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SPSS;
            return readstat_parse_por(parser, path, ctx);
        }
        else if (!strcasecmp(format, "dta"))
        {
            if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_STATA;
            return readstat_parse_dta(parser, path, ctx);
        }
    }
    else if (duckdb_read_stat_ends_with(path, ".sas7bdat"))
    {
        if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SAS;
        return readstat_parse_sas7bdat(parser, path, ctx);
    }
    else if (duckdb_read_stat_ends_with(path, ".xpt"))
    {
        if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SAS;
        return readstat_parse_xport(parser, path, ctx);
    }
    else if (duckdb_read_stat_ends_with(path, ".sav") || duckdb_read_stat_ends_with(path, ".zsav"))
    {
        if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SPSS;
        return readstat_parse_sav(parser, path, ctx);
    }
    else if (duckdb_read_stat_ends_with(path, ".por"))
    {
        if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_SPSS;
        return readstat_parse_por(parser, path, ctx);
    }
    else if (duckdb_read_stat_ends_with(path, ".dta"))
    {
        if (file_format) *file_format = DUCKDB_READ_STAT_FILE_FORMAT_STATA;
        return readstat_parse_dta(parser, path, ctx);
    }

    return READSTAT_OK;
}

#ifdef DUCKDB_WASM_EXTENSION
typedef struct duckdb_read_stat_webfs_ctx
{
    duckdb_read_stat_bind_data *bind_data;
    uint32_t file_id;
    uint64_t file_size;
    uint64_t pos;
    const uint8_t *buffer;
    uint64_t buffer_size;
    bool buffer_owned;
    void *open_result;
    bool opened;
} duckdb_read_stat_webfs_ctx;

static char *duckdb_read_stat_webfs_copy_response(const duckdb_web_response *resp)
{
    if (!resp || resp->dataOrValue == 0 || resp->dataSize == 0)
    {
        return NULL;
    }
    const char *src = (const char *)(uintptr_t)resp->dataOrValue;
    size_t len = (size_t)resp->dataSize;
    char *out = (char *)duckdb_malloc(len + 1);
    memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

static bool duckdb_read_stat_webfs_parse_number(const char *json, const char *key, double *out_value)
{
    if (!json || !key)
    {
        return false;
    }
    const char *pos = strstr(json, key);
    if (!pos)
    {
        return false;
    }
    pos = strchr(pos, ':');
    if (!pos)
    {
        return false;
    }
    pos++;
    while (*pos && isspace((unsigned char)*pos))
    {
        pos++;
    }
    if (strncmp(pos, "null", 4) == 0)
    {
        return false;
    }
    char *endptr = NULL;
    double value = strtod(pos, &endptr);
    if (endptr == pos)
    {
        return false;
    }
    if (out_value)
    {
        *out_value = value;
    }
    return true;
}

static bool duckdb_read_stat_webfs_resolve_file(const char *path, uint32_t *out_file_id, uint64_t *out_size,
                                                uint32_t *out_protocol, char **out_error)
{
    duckdb_web_response resp;
    resp.statusCode = 0;
    resp.dataOrValue = 0;
    resp.dataSize = 0;

    duckdb_web_fs_get_file_info_by_name(&resp, path, 0);
    if ((uint64_t)resp.statusCode != 0)
    {
        if (out_error)
        {
            char *msg = duckdb_read_stat_webfs_copy_response(&resp);
            if (msg)
            {
                *out_error = msg;
            }
            else
            {
                duckdb_read_stat_set_error_message(out_error, "Failed to resolve file info");
            }
        }
        duckdb_web_clear_response();
        return false;
    }

    char *json = duckdb_read_stat_webfs_copy_response(&resp);
    duckdb_web_clear_response();
    if (!json)
    {
        duckdb_read_stat_set_error_message(out_error, "File info lookup returned no data");
        return false;
    }

    double file_id = 0.0;
    double file_size = 0.0;
    double data_protocol = 0.0;
    bool has_file_id = duckdb_read_stat_webfs_parse_number(json, "\"fileId\"", &file_id);
    bool has_file_size = duckdb_read_stat_webfs_parse_number(json, "\"fileSize\"", &file_size);
    bool has_protocol = duckdb_read_stat_webfs_parse_number(json, "\"dataProtocol\"", &data_protocol);

    duckdb_free(json);

    if (!has_file_id)
    {
        duckdb_read_stat_set_error_message(
            out_error,
            "File is not registered in DuckDB WebFileSystem. Register a file handle or URL before reading.");
        return false;
    }

    if (out_file_id)
    {
        *out_file_id = (uint32_t)file_id;
    }
    if (out_size && has_file_size)
    {
        *out_size = (uint64_t)file_size;
    }
    if (out_protocol && has_protocol)
    {
        *out_protocol = (uint32_t)data_protocol;
    }

    return true;
}

static bool duckdb_read_stat_webfs_copy_buffer(const char *path, uint8_t **out_data, idx_t *out_size,
                                               char **out_error)
{
    duckdb_web_response resp;
    resp.statusCode = 0;
    resp.dataOrValue = 0;
    resp.dataSize = 0;

    duckdb_web_copy_file_to_buffer(&resp, path);
    if ((uint64_t)resp.statusCode != 0)
    {
        if (out_error)
        {
            char *msg = duckdb_read_stat_webfs_copy_response(&resp);
            if (msg)
            {
                *out_error = msg;
            }
            else
            {
                duckdb_read_stat_set_error_message(out_error, "Failed to copy file buffer");
            }
        }
        duckdb_web_clear_response();
        return false;
    }

    if (resp.dataOrValue == 0 || resp.dataSize == 0)
    {
        duckdb_web_clear_response();
        duckdb_read_stat_set_error_message(out_error, "File buffer was empty");
        return false;
    }

    const uint8_t *src = (const uint8_t *)(uintptr_t)resp.dataOrValue;
    size_t len = (size_t)resp.dataSize;
    uint8_t *buffer = (uint8_t *)duckdb_malloc(len);
    memcpy(buffer, src, len);
    duckdb_web_clear_response();

    if (out_data)
    {
        *out_data = buffer;
    }
    if (out_size)
    {
        *out_size = (idx_t)len;
    }
    return true;
}

static int duckdb_read_stat_webfs_open(const char *path, void *io_ctx)
{
    (void)path;
    duckdb_read_stat_webfs_ctx *ctx = (duckdb_read_stat_webfs_ctx *)io_ctx;
    if (!ctx)
    {
        return -1;
    }

    if (ctx->opened)
    {
        duckdb_web_fs_file_close(ctx->file_id);
        if (ctx->open_result)
        {
            free(ctx->open_result);
        }
        if (ctx->buffer && ctx->buffer_owned)
        {
            free((void *)ctx->buffer);
        }
        ctx->open_result = NULL;
        ctx->buffer = NULL;
        ctx->buffer_size = 0;
        ctx->buffer_owned = false;
        ctx->opened = false;
    }

    ctx->open_result = duckdb_web_fs_file_open(ctx->file_id, 1);
    if (!ctx->open_result)
    {
        if (ctx->bind_data)
        {
            duckdb_read_stat_set_error_message(&ctx->bind_data->error_message, "Failed to open file in WebFileSystem");
        }
        return -1;
    }

    double *open_vals = (double *)ctx->open_result;
    double file_size = open_vals[0];
    double buffer_ptr = open_vals[1];

    if (file_size > 0)
    {
        ctx->file_size = (uint64_t)file_size;
    }
    if (buffer_ptr != 0)
    {
        ctx->buffer = (const uint8_t *)(uintptr_t)buffer_ptr;
        ctx->buffer_size = (uint64_t)file_size;
        ctx->buffer_owned = true;
    }

    ctx->pos = 0;
    ctx->opened = true;
    return 0;
}

static int duckdb_read_stat_webfs_close(void *io_ctx)
{
    duckdb_read_stat_webfs_ctx *ctx = (duckdb_read_stat_webfs_ctx *)io_ctx;
    if (!ctx)
    {
        return 0;
    }
    if (ctx->opened)
    {
        duckdb_web_fs_file_close(ctx->file_id);
    }
    if (ctx->open_result)
    {
        free(ctx->open_result);
    }
    if (ctx->buffer && ctx->buffer_owned)
    {
        free((void *)ctx->buffer);
    }
    ctx->open_result = NULL;
    ctx->buffer = NULL;
    ctx->buffer_size = 0;
    ctx->buffer_owned = false;
    ctx->opened = false;
    ctx->pos = 0;
    return 0;
}

static readstat_off_t duckdb_read_stat_webfs_seek(readstat_off_t offset, readstat_io_flags_t whence, void *io_ctx)
{
    duckdb_read_stat_webfs_ctx *ctx = (duckdb_read_stat_webfs_ctx *)io_ctx;
    readstat_off_t newpos = -1;

    if (whence == READSTAT_SEEK_SET)
    {
        newpos = offset;
    }
    else if (whence == READSTAT_SEEK_CUR)
    {
        newpos = (readstat_off_t)ctx->pos + offset;
    }
    else if (whence == READSTAT_SEEK_END)
    {
        newpos = (readstat_off_t)ctx->file_size + offset;
    }

    if (newpos < 0)
    {
        return -1;
    }
    if (ctx->file_size > 0 && (uint64_t)newpos > ctx->file_size)
    {
        return -1;
    }

    ctx->pos = (uint64_t)newpos;
    return newpos;
}

static ssize_t duckdb_read_stat_webfs_read(void *buf, size_t nbytes, void *io_ctx)
{
    duckdb_read_stat_webfs_ctx *ctx = (duckdb_read_stat_webfs_ctx *)io_ctx;
    if (!ctx)
    {
        return 0;
    }

    if (ctx->buffer)
    {
        ssize_t bytes_left = (ssize_t)ctx->buffer_size - (ssize_t)ctx->pos;
        if (bytes_left <= 0)
        {
            return 0;
        }
        size_t to_copy = nbytes <= (size_t)bytes_left ? nbytes : (size_t)bytes_left;
        memcpy(buf, ctx->buffer + ctx->pos, to_copy);
        ctx->pos += (uint64_t)to_copy;
        return (ssize_t)to_copy;
    }

    if (nbytes == 0)
    {
        return 0;
    }

    size_t total_read = 0;
    uint8_t *out = (uint8_t *)buf;
    while (total_read < nbytes)
    {
        ssize_t bytes_read = duckdb_web_fs_file_read(
            ctx->file_id, out + total_read, (ssize_t)(nbytes - total_read), (double)(ctx->pos + total_read));
        if (bytes_read <= 0)
        {
            if (total_read == 0)
            {
                return bytes_read;
            }
            break;
        }
        total_read += (size_t)bytes_read;
    }
    ctx->pos += (uint64_t)total_read;
    return (ssize_t)total_read;
}

static readstat_error_t duckdb_read_stat_webfs_update(long file_size, readstat_progress_handler progress_handler,
                                                      void *user_ctx, void *io_ctx)
{
    (void)file_size;
    if (!progress_handler)
    {
        return READSTAT_OK;
    }

    duckdb_read_stat_webfs_ctx *ctx = (duckdb_read_stat_webfs_ctx *)io_ctx;
    double denom = ctx->file_size == 0 ? 1.0 : (double)ctx->file_size;
    double progress = ctx->file_size == 0 ? 1.0 : (double)ctx->pos / denom;

    if (progress_handler(progress, user_ctx))
    {
        return READSTAT_ERROR_USER_ABORT;
    }

    return READSTAT_OK;
}

static void duckdb_read_stat_apply_webfs_io(readstat_parser_t *parser, duckdb_read_stat_bind_data *data,
                                            duckdb_read_stat_webfs_ctx *io_ctx)
{
    io_ctx->bind_data = data;
    io_ctx->file_id = data->file_id;
    io_ctx->file_size = data->file_size;
    io_ctx->pos = 0;
    io_ctx->buffer = NULL;
    io_ctx->buffer_size = 0;
    io_ctx->buffer_owned = false;
    io_ctx->open_result = NULL;
    io_ctx->opened = false;

    readstat_set_open_handler(parser, &duckdb_read_stat_webfs_open);
    readstat_set_close_handler(parser, &duckdb_read_stat_webfs_close);
    readstat_set_seek_handler(parser, &duckdb_read_stat_webfs_seek);
    readstat_set_read_handler(parser, &duckdb_read_stat_webfs_read);
    readstat_set_update_handler(parser, &duckdb_read_stat_webfs_update);
    readstat_set_io_ctx(parser, io_ctx);
}
#endif

static bool duckdb_read_stat_load_buffer(const char *path, uint8_t **out_data, idx_t *out_size, char **out_error)
{
    if (!g_read_stat_conn || !path)
    {
        return false;
    }

    char *escaped = duckdb_read_stat_escape_sql_string(path);
    size_t query_len = strlen("SELECT * FROM read_blob('')") + strlen(escaped) + 1;
    char *query = (char *)duckdb_malloc(query_len);
    snprintf(query, query_len, "SELECT * FROM read_blob('%s')", escaped);

    duckdb_result result;
    duckdb_state state = duckdb_query(g_read_stat_conn, query, &result);

    duckdb_free(query);
    duckdb_free(escaped);

    if (state == DuckDBError)
    {
        if (out_error)
        {
            const char *msg = duckdb_result_error(&result);
            if (msg)
            {
                *out_error = (char *)duckdb_malloc(strlen(msg) + 1);
                strcpy(*out_error, msg);
            }
        }
        duckdb_destroy_result(&result);
        return false;
    }

    duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
    if (!chunk || duckdb_data_chunk_get_size(chunk) == 0)
    {
        if (out_error)
        {
            const char *msg = "read_blob returned no data";
            *out_error = (char *)duckdb_malloc(strlen(msg) + 1);
            strcpy(*out_error, msg);
        }
        if (chunk)
        {
            duckdb_destroy_data_chunk(&chunk);
        }
        duckdb_destroy_result(&result);
        return false;
    }

    duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, 0);
    uint64_t *validity = duckdb_vector_get_validity(vector);
    if (validity && !duckdb_validity_row_is_valid(validity, 0))
    {
        if (out_error)
        {
            const char *msg = "read_blob returned NULL";
            *out_error = (char *)duckdb_malloc(strlen(msg) + 1);
            strcpy(*out_error, msg);
        }
        duckdb_destroy_data_chunk(&chunk);
        duckdb_destroy_result(&result);
        return false;
    }

    duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vector);
    duckdb_string_t blob_str = data[0];
    idx_t blob_size = duckdb_string_t_length(blob_str);
    const char *blob_ptr = duckdb_string_t_data(&blob_str);
    if (blob_size == 0 || blob_ptr == NULL)
    {
        if (out_error)
        {
            const char *msg = "read_blob returned empty data";
            *out_error = (char *)duckdb_malloc(strlen(msg) + 1);
            strcpy(*out_error, msg);
        }
        duckdb_destroy_data_chunk(&chunk);
        duckdb_destroy_result(&result);
        return false;
    }

    uint8_t *buffer = (uint8_t *)duckdb_malloc(blob_size);
    memcpy(buffer, blob_ptr, blob_size);

    duckdb_destroy_data_chunk(&chunk);
    duckdb_destroy_result(&result);

    *out_data = buffer;
    *out_size = blob_size;
    return true;
}

static void duckdb_read_stat_bind_data_free(void *ptr)
{
    duckdb_read_stat_bind_data *data = (duckdb_read_stat_bind_data *)ptr;
    if (!data)
    {
        return;
    }
    if (data->error_message)
    {
        duckdb_free(data->error_message);
    }
    if (data->buffer)
    {
        duckdb_free(data->buffer);
    }
    if (data->path)
    {
        duckdb_free((void *)data->path);
    }
    if (data->format)
    {
        duckdb_free((void *)data->format);
    }
    if (data->encoding)
    {
        duckdb_free((void *)data->encoding);
    }
    duckdb_free(data);
}

static void duckdb_read_stat_apply_buffer_io(readstat_parser_t *parser, duckdb_read_stat_bind_data *data,
                                             duckdb_read_stat_buffer_ctx *io_ctx)
{
    if (!data->buffer || data->buffer_size == 0)
    {
        return;
    }

    io_ctx->data = data->buffer;
    io_ctx->size = data->buffer_size;
    io_ctx->pos = 0;

    readstat_set_open_handler(parser, &duckdb_read_stat_buffer_open);
    readstat_set_close_handler(parser, &duckdb_read_stat_buffer_close);
    readstat_set_seek_handler(parser, &duckdb_read_stat_buffer_seek);
    readstat_set_read_handler(parser, &duckdb_read_stat_buffer_read);
    readstat_set_update_handler(parser, &duckdb_read_stat_buffer_update);
    readstat_set_io_ctx(parser, io_ctx);
}

int duckdb_read_stat_ends_with(const char *string, const char *suffix)
{
    if (string == NULL || suffix == NULL)
        return false;
    size_t lenstr = strlen(string);
    size_t lensuffix = strlen(suffix);

    return lensuffix > lenstr ? false : strncasecmp(string + lenstr - lensuffix, suffix, lensuffix) == 0;
}

int duckdb_read_stat_bind_handle_metadata(readstat_metadata_t *metadata, void *ctx)
{
    duckdb_read_stat_bind_data *context = (duckdb_read_stat_bind_data *)ctx;
    context->cardinality = readstat_get_row_count(metadata);
    return READSTAT_HANDLER_OK;
}

int duckdb_read_stat_bind_handle_variable(int index, readstat_variable_t *variable,
                                          const char *val_labels, void *ctx)
{
    duckdb_read_stat_bind_data *context = (duckdb_read_stat_bind_data *)ctx;
    const char *name = readstat_variable_get_name(variable);
    readstat_type_t type = readstat_variable_get_type(variable);
    const char *format = readstat_variable_get_format(variable);
    duckdb_logical_type logical_type = NULL;

    switch (type)
    {
    case READSTAT_TYPE_STRING:
    case READSTAT_TYPE_STRING_REF:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
        break;
    case READSTAT_TYPE_INT8:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT);
        break;
    case READSTAT_TYPE_INT16:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
        break;
    case READSTAT_TYPE_INT32:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        break;
    case READSTAT_TYPE_FLOAT:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
        break;
    case READSTAT_TYPE_DOUBLE:
        switch (duckdb_read_stat_classify_format(format))
        {
        case DUCKDB_READ_STAT_TEMPORAL_DATE:
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_DATE);
            break;
        case DUCKDB_READ_STAT_TEMPORAL_DATETIME:
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP);
            break;
        case DUCKDB_READ_STAT_TEMPORAL_TIME:
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME);
            break;
        default:
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
            break;
        }
        break;
    }

    duckdb_bind_add_result_column(context->bind_info, name, logical_type);
    duckdb_destroy_logical_type(&logical_type);

    return READSTAT_HANDLER_OK;
}

void duckdb_read_stat_bind_handle_error(const char *error_message, void *ctx)
{
    duckdb_read_stat_bind_data *context = (duckdb_read_stat_bind_data *)ctx;
    if (context->error_message != NULL)
    {
        duckdb_free(context->error_message);
    }
    context->error_message = (char *)duckdb_malloc(strlen(error_message) + 1);
    strcpy(context->error_message, error_message);
}

static readstat_error_t duckdb_read_stat_parse_file(readstat_parser_t *parser, duckdb_read_stat_bind_data *data)
{
    return duckdb_read_stat_dispatch_parse(parser, data->path, data->format, &data->file_format, data);
}

void duckdb_read_stat_bind(duckdb_bind_info info)
{
    readstat_parser_t *parser = readstat_parser_init();
    duckdb_value path_value = duckdb_bind_get_parameter(info, 0);
    duckdb_value format_value = duckdb_bind_get_named_parameter(info, "format");
    duckdb_value encoding_value = duckdb_bind_get_named_parameter(info, "encoding");
    char *path = duckdb_get_varchar(path_value);
    duckdb_read_stat_bind_data *data = duckdb_malloc(sizeof(duckdb_read_stat_bind_data));
    readstat_error_t error;

    data->bind_info = info;
    data->path = path;
    data->format = NULL;
    data->encoding = NULL;
    data->buffer = NULL;
    data->buffer_size = 0;
    data->data_protocol = 0;
    data->file_id = 0;
    data->file_size = 0;
    data->use_webfs = false;
    data->error_message = NULL;
    data->cardinality = 0;

    readstat_set_metadata_handler(parser, &duckdb_read_stat_bind_handle_metadata);
    readstat_set_variable_handler(parser, &duckdb_read_stat_bind_handle_variable);
    readstat_set_error_handler(parser, &duckdb_read_stat_bind_handle_error);
    readstat_set_row_limit(parser, 0);

    if (encoding_value != NULL)
    {
        char *encoding = duckdb_get_varchar(encoding_value);
        data->encoding = encoding;
        readstat_set_file_character_encoding(parser, encoding);
    }

#ifdef DUCKDB_WASM_EXTENSION
    // Declare io contexts at function scope so they outlive the if/else block
    duckdb_read_stat_buffer_ctx buffer_io_ctx;
    duckdb_read_stat_webfs_ctx webfs_io_ctx;
    bool io_configured = false;

    // Resolve file info via WebFS first to pick the right IO path
    if (duckdb_read_stat_webfs_resolve_file(
            data->path, &data->file_id, &data->file_size, &data->data_protocol, &data->error_message))
    {
        data->use_webfs = true;

        // Prefer buffered reads for BUFFER + BROWSER_FILEREADER protocols
        if (data->data_protocol == 0 || data->data_protocol == 2)
        {
            data->use_webfs = false;
            if (duckdb_read_stat_webfs_copy_buffer(
                    data->path, &data->buffer, &data->buffer_size, &data->error_message))
            {
                duckdb_read_stat_apply_buffer_io(parser, data, &buffer_io_ctx);
                io_configured = true;
            }
        }
        else
        {
            duckdb_read_stat_apply_webfs_io(parser, data, &webfs_io_ctx);
            io_configured = true;
        }
    }

    if (!io_configured)
    {
        // Clear any prior error before attempting read_blob
        if (data->error_message != NULL)
        {
            duckdb_free(data->error_message);
            data->error_message = NULL;
        }

        if (!duckdb_read_stat_load_buffer(data->path, &data->buffer, &data->buffer_size, &data->error_message))
        {
            if (data->error_message != NULL)
            {
                duckdb_bind_set_error(info, data->error_message);
            }
            readstat_parser_free(parser);
            duckdb_read_stat_bind_data_free(data);
            return;
        }

        data->use_webfs = false;
        duckdb_read_stat_apply_buffer_io(parser, data, &buffer_io_ctx);
    }
#else
    if (!duckdb_read_stat_load_buffer(data->path, &data->buffer, &data->buffer_size, &data->error_message))
    {
        if (data->error_message != NULL)
        {
            duckdb_bind_set_error(info, data->error_message);
            readstat_parser_free(parser);
            duckdb_read_stat_bind_data_free(data);
            return;
        }
    }

    duckdb_read_stat_buffer_ctx io_ctx;
    duckdb_read_stat_apply_buffer_io(parser, data, &io_ctx);
#endif

    if (format_value != NULL)
    {
        char *format = duckdb_get_varchar(format_value);
        data->format = format;
    }

    error = duckdb_read_stat_parse_file(parser, data);

#ifdef DUCKDB_WASM_EXTENSION
    if (error != READSTAT_OK && data->use_webfs)
    {
        uint8_t *fallback_buffer = NULL;
        idx_t fallback_size = 0;
        char *fallback_error = NULL;

        if (data->error_message)
        {
            duckdb_free(data->error_message);
            data->error_message = NULL;
        }

        if (duckdb_read_stat_webfs_copy_buffer(
                data->path, &fallback_buffer, &fallback_size, &fallback_error))
        {
            readstat_parser_free(parser);
            parser = readstat_parser_init();

            readstat_set_metadata_handler(parser, &duckdb_read_stat_bind_handle_metadata);
            readstat_set_variable_handler(parser, &duckdb_read_stat_bind_handle_variable);
            readstat_set_error_handler(parser, &duckdb_read_stat_bind_handle_error);
            readstat_set_row_limit(parser, 0);

            if (data->encoding != NULL)
            {
                readstat_set_file_character_encoding(parser, data->encoding);
            }

            data->buffer = fallback_buffer;
            data->buffer_size = fallback_size;
            data->use_webfs = false;

            duckdb_read_stat_buffer_ctx fallback_io_ctx;
            duckdb_read_stat_apply_buffer_io(parser, data, &fallback_io_ctx);
            error = duckdb_read_stat_parse_file(parser, data);
        }
        else if (fallback_error)
        {
            data->error_message = fallback_error;
            fallback_error = NULL;
        }

        if (fallback_error)
        {
            duckdb_free(fallback_error);
        }
    }
#endif

    if (error != READSTAT_OK)
    {
        duckdb_bind_set_error(info, data->error_message ? data->error_message : readstat_error_message(error));
        readstat_parser_free(parser);
        duckdb_read_stat_bind_data_free(data);
        return;
    }

    duckdb_bind_set_cardinality(data->bind_info, data->cardinality, true);
    duckdb_bind_set_bind_data(info, data, duckdb_read_stat_bind_data_free);
    readstat_parser_free(parser);
}

void duckdb_read_stat_init(duckdb_init_info info)
{
    duckdb_read_stat_init_data *data = duckdb_malloc(sizeof(duckdb_read_stat_init_data));
    data->offset = 0;
    data->actual_rows_read = 0;
    duckdb_init_set_init_data(info, data, duckdb_free);
}

const int days_between_unix_epoch_and_sas_epoch = -3653;
const int days_between_unix_epoch_and_spss_epoch = -141428;
const int days_between_unix_epoch_and_stata_epoch = -3653;

duckdb_date duckdb_read_stat_convert_date(int days, int seconds, int days_between_epochs)
{
    duckdb_date date;
    date.days = days_between_epochs + days + (seconds / (3600 * 24));
    return date;
}

double duckdb_read_stat_modulo(double dividend, double divisor)
{
    double remainder = fmod(dividend, divisor);
    remainder += ((remainder != 0) & ((remainder < 0) ^ (divisor < 0))) * divisor;
    return remainder;
}

duckdb_date duckdb_read_stat_to_date(double timestamp, duckdb_read_stat_file_format file_format)
{
    int days = 0;
    int seconds = 0;

    if (file_format == DUCKDB_READ_STAT_FILE_FORMAT_SPSS)
    {
        days = (int)(floor(timestamp / 86400.0));
        seconds = (int)(duckdb_read_stat_modulo(timestamp, 86400.0));
    }
    else
    {
        days = (int)timestamp;
    }

    switch (file_format)
    {
    case DUCKDB_READ_STAT_FILE_FORMAT_SAS:
        return duckdb_read_stat_convert_date(days, seconds, days_between_unix_epoch_and_sas_epoch);
    case DUCKDB_READ_STAT_FILE_FORMAT_SPSS:
        return duckdb_read_stat_convert_date(days, seconds, days_between_unix_epoch_and_spss_epoch);
    case DUCKDB_READ_STAT_FILE_FORMAT_STATA:
        return duckdb_read_stat_convert_date(days, seconds, days_between_unix_epoch_and_stata_epoch);
    }
}

duckdb_timestamp duckdb_read_stat_convert_timestamp(int days, int seconds, int days_between_epochs)
{
    duckdb_timestamp timestamp;
    timestamp.micros = ((((int64_t)days_between_epochs + (int64_t)days) * 24L * 3600L) + (int64_t)seconds) * 1000000L;
    return timestamp;
}

duckdb_timestamp duckdb_read_stat_to_timestamp(double timestamp, duckdb_read_stat_file_format file_format)
{
    int days = 0;
    double milliseconds = 0;
    int seconds = 0;

    if (file_format == DUCKDB_READ_STAT_FILE_FORMAT_STATA)
    {
        days = (int)(floor(timestamp / 86400000.0));
        milliseconds = duckdb_read_stat_modulo(timestamp, 86400000.0);
        seconds = (int)(milliseconds / 1000.0);
    }
    else
    {
        days = (int)(floor(timestamp / 86400.0));
        seconds = (int)duckdb_read_stat_modulo(timestamp, 86400.0);
    }

    switch (file_format)
    {
    case DUCKDB_READ_STAT_FILE_FORMAT_SAS:
        return duckdb_read_stat_convert_timestamp(days, seconds, days_between_unix_epoch_and_sas_epoch);
    case DUCKDB_READ_STAT_FILE_FORMAT_SPSS:
        return duckdb_read_stat_convert_timestamp(days, seconds, days_between_unix_epoch_and_spss_epoch);
    case DUCKDB_READ_STAT_FILE_FORMAT_STATA:
        return duckdb_read_stat_convert_timestamp(days, seconds, days_between_unix_epoch_and_stata_epoch);
    }
}

const int seconds_in_hour = 60 * 60;
const int seconds_in_minute = 60;

duckdb_time duckdb_read_stat_convert_time(int days, int seconds)
{
    int hours, minutes, remaining_seconds;
    int total = (24 * 3600 * days) + seconds;

    hours = (total / seconds_in_hour);
    remaining_seconds = total - (hours * seconds_in_hour);
    minutes = remaining_seconds / seconds_in_minute;
    remaining_seconds = remaining_seconds - (minutes * seconds_in_minute);

    duckdb_time_struct time_struct;

    time_struct.hour = hours;
    time_struct.min = minutes;
    time_struct.sec = remaining_seconds;
    time_struct.micros = 0;

    return duckdb_to_time(time_struct);
}

duckdb_time duckdb_read_stat_to_time(double tstamp, duckdb_read_stat_file_format file_format)
{
    int days = 0;
    int seconds = 0;
    double msecs = 0;
    int usecs = 0;

    if (file_format == DUCKDB_READ_STAT_FILE_FORMAT_STATA)
    {
        days = (int)(floor(tstamp / 86400000.0));
        msecs = duckdb_read_stat_modulo(tstamp, 86400000.0);
        seconds = (int)(msecs / 1000.0);
        usecs = (int)(duckdb_read_stat_modulo(msecs, 1000.0) * 1000.0);
    }
    else
    {
        days = (int)(floor(tstamp / 86400.0));
        seconds = (int)(duckdb_read_stat_modulo(tstamp, 86400.0));
    }

    return duckdb_read_stat_convert_time(days, seconds);
}

int duckdb_read_stat_handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx)
{
    duckdb_read_stat_context *context = (duckdb_read_stat_context *)ctx;
    duckdb_read_stat_bind_data *bind_data = duckdb_function_get_bind_data(context->function_info);
    duckdb_read_stat_init_data *init_data = duckdb_function_get_init_data(context->function_info);
    idx_t var_index = readstat_variable_get_index(variable);
    readstat_type_t type = readstat_value_type(value);
    const char *format = readstat_variable_get_format(variable);
    duckdb_vector vector = duckdb_data_chunk_get_vector(context->data_chunk, var_index);

    if (!readstat_value_is_system_missing(value) && !readstat_value_is_tagged_missing(value) && !readstat_value_is_defined_missing(value, variable))
    {
        switch (type)
        {
        case READSTAT_TYPE_STRING:
        case READSTAT_TYPE_STRING_REF:
            duckdb_vector_assign_string_element(vector, obs_index, readstat_string_value(value));
            break;

        case READSTAT_TYPE_INT8:
            ((int8_t *)duckdb_vector_get_data(vector))[obs_index] = readstat_int8_value(value);
            break;

        case READSTAT_TYPE_INT16:
            ((int16_t *)duckdb_vector_get_data(vector))[obs_index] = readstat_int16_value(value);
            break;

        case READSTAT_TYPE_INT32:
            ((int32_t *)duckdb_vector_get_data(vector))[obs_index] = readstat_int32_value(value);
            break;

        case READSTAT_TYPE_FLOAT:
            ((float *)duckdb_vector_get_data(vector))[obs_index] = readstat_float_value(value);
            break;

        case READSTAT_TYPE_DOUBLE:
        {
            double double_value = readstat_double_value(value);

            switch (duckdb_read_stat_classify_format(format))
            {
            case DUCKDB_READ_STAT_TEMPORAL_DATE:
            {
                duckdb_date converted = duckdb_read_stat_to_date(double_value, bind_data->file_format);
                ((duckdb_date *)duckdb_vector_get_data(vector))[obs_index] = converted;
                break;
            }
            case DUCKDB_READ_STAT_TEMPORAL_DATETIME:
            {
                duckdb_timestamp converted = duckdb_read_stat_to_timestamp(double_value, bind_data->file_format);
                ((duckdb_timestamp *)duckdb_vector_get_data(vector))[obs_index] = converted;
                break;
            }
            case DUCKDB_READ_STAT_TEMPORAL_TIME:
            {
                duckdb_time converted = duckdb_read_stat_to_time(double_value, bind_data->file_format);
                ((duckdb_time *)duckdb_vector_get_data(vector))[obs_index] = converted;
                break;
            }
            default:
                ((double *)duckdb_vector_get_data(vector))[obs_index] = double_value;
                break;
            }
            break;
        }
        }
    }
    else
    {
        duckdb_vector_ensure_validity_writable(vector);
        uint64_t *validity = duckdb_vector_get_validity(vector);
        duckdb_validity_set_row_invalid(validity, obs_index);
    }

    init_data->actual_rows_read = obs_index + 1;

    return READSTAT_HANDLER_OK;
}

int duckdb_read_stat_handle_metadata(readstat_metadata_t *metadata, void *ctx)
{
    return READSTAT_HANDLER_OK;
}

int duckdb_read_stat_handle_variable(int index, readstat_variable_t *variable,
                                     const char *val_labels, void *ctx)
{
    return READSTAT_HANDLER_OK;
}

int duckdb_read_stat_handle_value_label(const char *val_labels, readstat_value_t value, const char *label, void *ctx)
{
    return READSTAT_HANDLER_OK;
}

void duckdb_read_stat_handle_error(const char *error_message, void *ctx)
{
    duckdb_read_stat_context *context = (duckdb_read_stat_context *)ctx;
    if (context->error_message != NULL)
    {
        duckdb_free(context->error_message);
    }
    context->error_message = (char *)duckdb_malloc(strlen(error_message) + 1);
    strcpy(context->error_message, error_message);
}

void duckdb_read_stat_function(duckdb_function_info info, duckdb_data_chunk output)
{
    duckdb_read_stat_init_data *init_data = (duckdb_read_stat_init_data *)duckdb_function_get_init_data(info);
    duckdb_read_stat_bind_data *bind_data = (duckdb_read_stat_bind_data *)duckdb_function_get_bind_data(info);

    if (init_data->offset >= bind_data->cardinality)
    {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }

    readstat_error_t error = READSTAT_OK;
    readstat_parser_t *parser = readstat_parser_init();

    readstat_set_row_offset(parser, (long)init_data->offset);
    readstat_set_row_limit(parser, (long)duckdb_vector_size());
    readstat_set_metadata_handler(parser, &duckdb_read_stat_handle_metadata);
    readstat_set_variable_handler(parser, &duckdb_read_stat_handle_variable);
    readstat_set_value_handler(parser, &duckdb_read_stat_handle_value);
    readstat_set_error_handler(parser, &duckdb_read_stat_handle_error);

    duckdb_read_stat_context *context = duckdb_malloc(sizeof(duckdb_read_stat_context));
    context->function_info = info;
    context->data_chunk = output;
    context->error_message = NULL;

    if (bind_data->encoding != NULL)
    {
        readstat_set_file_character_encoding(parser, bind_data->encoding);
    }

#ifdef DUCKDB_WASM_EXTENSION
    duckdb_read_stat_webfs_ctx webfs_io_ctx;
    duckdb_read_stat_buffer_ctx buffer_io_ctx;
    if (bind_data->use_webfs)
    {
        duckdb_read_stat_apply_webfs_io(parser, bind_data, &webfs_io_ctx);
    }
    else
    {
        duckdb_read_stat_apply_buffer_io(parser, bind_data, &buffer_io_ctx);
    }
#else
    duckdb_read_stat_buffer_ctx buffer_io_ctx;
    duckdb_read_stat_apply_buffer_io(parser, bind_data, &buffer_io_ctx);
#endif

    error = duckdb_read_stat_dispatch_parse(parser, bind_data->path, bind_data->format, NULL, context);

#ifdef DUCKDB_WASM_EXTENSION
    if (error != READSTAT_OK && bind_data->use_webfs)
    {
        uint8_t *fallback_buffer = NULL;
        idx_t fallback_size = 0;
        char *fallback_error = NULL;

        if (context->error_message)
        {
            duckdb_free(context->error_message);
            context->error_message = NULL;
        }

        if (duckdb_read_stat_webfs_copy_buffer(
                bind_data->path, &fallback_buffer, &fallback_size, &fallback_error))
        {
            readstat_parser_free(parser);
            parser = readstat_parser_init();

            readstat_set_row_offset(parser, (long)init_data->offset);
            readstat_set_row_limit(parser, (long)duckdb_vector_size());
            readstat_set_metadata_handler(parser, &duckdb_read_stat_handle_metadata);
            readstat_set_variable_handler(parser, &duckdb_read_stat_handle_variable);
            readstat_set_value_handler(parser, &duckdb_read_stat_handle_value);
            readstat_set_error_handler(parser, &duckdb_read_stat_handle_error);

            if (bind_data->encoding != NULL)
            {
                readstat_set_file_character_encoding(parser, bind_data->encoding);
            }

            duckdb_read_stat_buffer_ctx fallback_io_ctx;
            duckdb_read_stat_bind_data tmp_bind = *bind_data;
            tmp_bind.buffer = fallback_buffer;
            tmp_bind.buffer_size = fallback_size;

            duckdb_read_stat_apply_buffer_io(parser, &tmp_bind, &fallback_io_ctx);
            error = duckdb_read_stat_dispatch_parse(parser, bind_data->path, bind_data->format, NULL, context);
        }
        else if (fallback_error)
        {
            if (context->error_message)
            {
                duckdb_free(context->error_message);
            }
            context->error_message = fallback_error;
            fallback_error = NULL;
        }

        if (fallback_buffer)
        {
            duckdb_free(fallback_buffer);
        }
        if (fallback_error)
        {
            duckdb_free(fallback_error);
        }
    }
#endif

    if (error != READSTAT_OK)
    {
        if (context->error_message != NULL)
        {
            duckdb_function_set_error(info, context->error_message);
            duckdb_free(context->error_message);
        }
        duckdb_free(context);
        readstat_parser_free(parser);
        return;
    }

    readstat_parser_free(parser);

    init_data->offset += init_data->actual_rows_read;
    duckdb_data_chunk_set_size(output, init_data->actual_rows_read);
    init_data->actual_rows_read = 0;
    duckdb_free(context);
}

void duckdb_read_stat_register_read_stat_function(duckdb_connection connection)
{
    duckdb_table_function function = duckdb_create_table_function();
    duckdb_table_function_set_name(function, "read_stat");

    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(function, varchar_type);
    duckdb_table_function_add_named_parameter(function, "format", varchar_type);
    duckdb_table_function_add_named_parameter(function, "encoding", varchar_type);
    duckdb_destroy_logical_type(&varchar_type);

    duckdb_table_function_set_bind(function, &duckdb_read_stat_bind);
    duckdb_table_function_set_init(function, &duckdb_read_stat_init);
    duckdb_table_function_set_function(function, &duckdb_read_stat_function);

    duckdb_state result = duckdb_register_table_function(connection, function);

    duckdb_destroy_table_function(&function);
}

void duckdb_read_stat_replacement_scan(duckdb_replacement_scan_info info, const char *table_name, void *data)
{
    if (duckdb_read_stat_ends_with(table_name, ".sas7bdat") || duckdb_read_stat_ends_with(table_name, ".xpt") || duckdb_read_stat_ends_with(table_name, ".sav") || duckdb_read_stat_ends_with(table_name, ".zsav") || duckdb_read_stat_ends_with(table_name, ".por") || duckdb_read_stat_ends_with(table_name, ".dta"))
    {
        duckdb_replacement_scan_set_function_name(info, "read_stat");
        duckdb_replacement_scan_add_parameter(info, duckdb_create_varchar(table_name));
    }
}

DUCKDB_EXTENSION_ENTRYPOINT_CUSTOM(duckdb_extension_info info, struct duckdb_extension_access *access)
{
    duckdb_database *db = access->get_database(info);
    if (duckdb_connect(*db, &g_read_stat_conn) == DuckDBError)
    {
        access->set_error(info, "Failed to open connection for read_stat");
        return false;
    }
    duckdb_read_stat_register_read_stat_function(g_read_stat_conn);
    duckdb_add_replacement_scan(*db, &duckdb_read_stat_replacement_scan, NULL, NULL);
    return true;
}
