#pragma once
#include <asio.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

// ================================================================
// HttpParser
// ================================================================
namespace p2psocks
{
    class HttpParser
    {
    public:
        enum class Type
        {
            Unknown,
            Request,
            Response
        };

        enum class State
        {
            ParsingStartLine,
            ParsingHeaders,

            ParsingBody,
            ParsingBodyUntilEOF,

            ParsingChunkSize,
            ParsingChunkData,
            ParsingChunkDataCRLF,
            ParsingTrailers,

            Complete,
            Error
        };

        struct Header
        {
            std::string name;
            std::string value;
        };

        struct Message
        {
            Type type = Type::Unknown;

            // HTTP/1.0 => 10
            // HTTP/1.1 => 11
            int version = 0;

            // --------------------------------------------------------
            // Request
            // --------------------------------------------------------

            std::string method;
            std::string target;

            // /path
            std::string path;

            // foo=1&bar=2
            std::string query;

            // --------------------------------------------------------
            // Response
            // --------------------------------------------------------

            unsigned int status_code = 0;
            std::string reason;

            // --------------------------------------------------------
            // Headers
            // --------------------------------------------------------

            std::vector<Header> headers;

            // Chunked trailers
            std::vector<Header> trailers;

            // --------------------------------------------------------
            // Body
            // --------------------------------------------------------

            std::string body;

            // --------------------------------------------------------
            // Connection
            // --------------------------------------------------------

            bool keep_alive = false;

            bool connection_close = false;

            bool connection_keep_alive = false;

            // --------------------------------------------------------
            // Transfer-Encoding
            // --------------------------------------------------------

            bool chunked = false;

            // --------------------------------------------------------
            // Upgrade
            // --------------------------------------------------------

            bool upgrade = false;

            // --------------------------------------------------------
            // CONNECT
            // --------------------------------------------------------

            bool connect = false;

            // --------------------------------------------------------
            // Body
            // --------------------------------------------------------

            bool has_body = false;

            std::optional<std::uint64_t> content_length;

            // ========================================================
            // Header lookup
            // ========================================================

            std::string_view get_header(
                std::string_view name) const
            {
                for (const auto &h : headers)
                {
                    if (iequals(h.name, name))
                        return h.value;
                }

                return {};
            }

            bool has_header(
                std::string_view name) const
            {
                for (const auto &h : headers)
                {
                    if (iequals(h.name, name))
                        return true;
                }

                return false;
            }

            std::vector<std::string_view> get_headers(
                std::string_view name) const
            {
                std::vector<std::string_view> result;

                for (const auto &h : headers)
                {
                    if (iequals(h.name, name))
                        result.push_back(h.value);
                }

                return result;
            }

            // ========================================================
            // 序列化：把 Message 转成可以直接写到 socket 的原始字节
            // ========================================================
            //
            // 注意：
            // - 如果 headers 里已经有 Content-Length / Transfer-Encoding，
            //   不会重复添加。
            // - chunked == true 时，会把 body 整体编码成一个 chunk 输出
            //   （适合代理转发已经收全的响应体这种场景；如果需要真正的
            //   流式分块发送，应该在上层直接写 chunk，不走这个函数）。
            // - trailers 只有在 chunked == true 时才会被写出。
            //
            std::string serialize() const
            {
                std::string out;

                out.reserve(body.size() + 256);

                // ----------------------------------------------------
                // Start line
                // ----------------------------------------------------

                const std::string_view http_version =
                    (version == 10) ? "HTTP/1.0" : "HTTP/1.1";

                if (type == Type::Response)
                {
                    out += http_version;
                    out += ' ';
                    out += std::to_string(status_code);
                    out += ' ';
                    out += reason.empty()
                               ? std::string(default_reason_phrase(status_code))
                               : reason;
                    out += "\r\n";
                }
                else
                {
                    out += method.empty() ? "GET" : method;
                    out += ' ';
                    out += target.empty() ? "/" : target;
                    out += ' ';
                    out += http_version;
                    out += "\r\n";
                }

                // ----------------------------------------------------
                // Headers
                // ----------------------------------------------------

                const bool has_cl = has_header("Content-Length");
                const bool has_te = has_header("Transfer-Encoding");

                for (const auto &h : headers)
                {
                    out += h.name;
                    out += ": ";
                    out += h.value;
                    out += "\r\n";
                }

                // 自动补全 framing 头（如果调用方没有手动设置）
                //
                // 注意：这里二选一，不会同时补两个，避免 CL+TE 冲突。
                if (!has_cl && !has_te)
                {
                    if (chunked)
                    {
                        out += "Transfer-Encoding: chunked\r\n";
                    }
                    else
                    {
                        out += "Content-Length: ";
                        out += std::to_string(body.size());
                        out += "\r\n";
                    }
                }

                out += "\r\n";

                // ----------------------------------------------------
                // Body
                // ----------------------------------------------------

                if (chunked)
                {
                    if (!body.empty())
                    {
                        char size_buf[2 * sizeof(std::size_t) + 1] = {};

                        std::snprintf(
                            size_buf,
                            sizeof(size_buf),
                            "%zx",
                            body.size());

                        out += size_buf;
                        out += "\r\n";
                        out += body;
                        out += "\r\n";
                    }

                    out += "0\r\n";

                    for (const auto &t : trailers)
                    {
                        out += t.name;
                        out += ": ";
                        out += t.value;
                        out += "\r\n";
                    }

                    out += "\r\n";
                }
                else
                {
                    out += body;
                }

                return out;
            }

            // ========================================================
            // 便捷工厂：构造一条响应消息
            // ========================================================

            static Message make_response(
                unsigned int status,
                std::string body_text = {},
                std::vector<Header> extra_headers = {},
                int http_version = 11,
                std::string reason_phrase = {})
            {
                Message msg;

                msg.type = Type::Response;
                msg.version = http_version;
                msg.status_code = status;
                msg.reason = reason_phrase.empty()
                                 ? std::string(default_reason_phrase(status))
                                 : std::move(reason_phrase);
                msg.headers = std::move(extra_headers);
                msg.body = std::move(body_text);
                msg.has_body = !msg.body.empty();

                return msg;
            }

            static bool iequals(
                std::string_view a,
                std::string_view b)
            {
                if (a.size() != b.size())
                    return false;

                for (std::size_t i = 0; i < a.size(); ++i)
                {
                    if (std::tolower(
                            static_cast<unsigned char>(a[i])) !=
                        std::tolower(
                            static_cast<unsigned char>(b[i])))
                    {
                        return false;
                    }
                }

                return true;
            }

        private:
            
            static std::string_view default_reason_phrase(
                unsigned int status)
            {
                switch (status)
                {
                case 200:
                    return "OK";
                case 201:
                    return "Created";
                case 204:
                    return "No Content";
                case 301:
                    return "Moved Permanently";
                case 302:
                    return "Found";
                case 304:
                    return "Not Modified";
                case 400:
                    return "Bad Request";
                case 401:
                    return "Unauthorized";
                case 403:
                    return "Forbidden";
                case 404:
                    return "Not Found";
                case 405:
                    return "Method Not Allowed";
                case 408:
                    return "Request Timeout";
                case 413:
                    return "Payload Too Large";
                case 414:
                    return "URI Too Long";
                case 431:
                    return "Request Header Fields Too Large";
                case 500:
                    return "Internal Server Error";
                case 501:
                    return "Not Implemented";
                case 502:
                    return "Bad Gateway";
                case 503:
                    return "Service Unavailable";
                case 504:
                    return "Gateway Timeout";
                default:
                    return "Unknown";
                }
            }
        };

        // ============================================================
        // Limits
        // ============================================================

        struct Limits
        {
            // Request/response start line
            std::size_t max_start_line = 8192;

            // Entire HTTP header section
            std::size_t max_header_bytes = 64 * 1024;

            // Header count
            std::size_t max_headers = 128;

            // Body
            std::size_t max_body_bytes = 16 * 1024 * 1024;

            // Chunk-size line
            std::size_t max_chunk_line = 8192;

            // Trailer count
            std::size_t max_trailers = 128;

            // Trailer 单行最大长度
            std::size_t max_trailer_line = 8192;

            // Trailer 部分整体最大字节数（含结束空行）
            std::size_t max_trailer_bytes = 64 * 1024;
        };

    public:
        explicit HttpParser(Limits limits)
            : limits_(limits)
        {
            reset();
        }

        // ============================================================
        // Feed data
        // ============================================================

        void feed(
            const char *data,
            std::size_t size)
        {
            if (size == 0)
                return;

            if (state_ == State::Complete)
                return;

            if (state_ == State::Error)
                return;

            buffer_.append(data, size);

            parse();
        }

        void feed(std::string_view data)
        {
            feed(data.data(), data.size());
        }

        // ============================================================
        // Status
        // ============================================================

        bool complete() const
        {
            return state_ == State::Complete;
        }

        bool error() const
        {
            return state_ == State::Error;
        }

        bool finished() const
        {
            return complete() || error();
        }

        State state() const
        {
            return state_;
        }

        const Message &message() const
        {
            return message_;
        }

        Message &message()
        {
            return message_;
        }

        const std::string &error_message() const
        {
            return error_message_;
        }

        // ============================================================
        // Remaining bytes
        //
        // Example:
        //
        // GET /a HTTP/1.1...
        //
        // GET /b HTTP/1.1...
        //
        // After first message is complete:
        //
        // remaining()
        //
        // returns the bytes belonging to the second message.
        // ============================================================

        std::string_view remaining() const
        {
            return std::string_view(buffer_);
        }

        std::string take_remaining()
        {
            std::string result;

            result.swap(buffer_);

            return result;
        }

        // ============================================================
        // Reset
        //
        // Important:
        //
        // reset() does NOT clear remaining bytes.
        //
        // This is useful for HTTP keep-alive / pipelining.
        // ============================================================

        void reset()
        {
            message_ = Message{};

            state_ =
                State::ParsingStartLine;

            error_message_.clear();

            pos_ = 0;

            header_end_ =
                std::string::npos;

            body_start_ =
                std::string::npos;

            content_length_value_.reset();

            chunk_remaining_ = 0;

            trailer_start_ = 0;
        }

        // Clear everything.
        void clear()
        {
            buffer_.clear();

            reset();
        }

        // ============================================================
        // EOF
        //
        // Used when a response body is delimited by TCP connection
        // close rather than Content-Length / chunked.
        // ============================================================

        bool finish_eof()
        {
            if (state_ == State::Complete)
                return true;

            if (state_ == State::Error)
                return false;

            if (state_ != State::ParsingBodyUntilEOF)
            {
                fail("unexpected EOF");

                return false;
            }

            if (body_start_ > buffer_.size())
            {
                fail("invalid body position");

                return false;
            }

            const std::size_t body_size =
                buffer_.size() - body_start_;

            if (body_size >
                limits_.max_body_bytes)
            {
                fail("body too large");

                return false;
            }

            if (body_size != 0)
            {
                message_.body.assign(
                    buffer_.data() + body_start_,
                    body_size);
            }

            message_.has_body =
                !message_.body.empty();

            pos_ =
                buffer_.size();

            finish_message();

            return true;
        }

    private:
        Limits limits_;

        std::string buffer_;

        std::size_t pos_ = 0;

        std::size_t trailer_start_ = 0;

        std::size_t header_end_ =
            std::string::npos;

        std::size_t body_start_ =
            std::string::npos;

        State state_ =
            State::ParsingStartLine;

        Message message_;

        std::string error_message_;

        std::optional<std::uint64_t>
            content_length_value_;

        std::uint64_t chunk_remaining_ = 0;

        std::size_t scan_pos_ = 0; // 上次扫描 "\r\n" 未果时停下的位置

        // ============================================================
        // Main parser
        // ============================================================

        void parse()
        {
            try
            {
                for (;;)
                {
                    switch (state_)
                    {
                    case State::ParsingStartLine:

                        if (!parse_start_line())
                            return;

                        break;

                    case State::ParsingHeaders:

                        if (!parse_headers())
                            return;

                        break;

                    case State::ParsingBody:

                        if (!parse_content_length_body())
                            return;

                        break;

                    case State::ParsingBodyUntilEOF:

                        // Need more TCP data.
                        return;

                    case State::ParsingChunkSize:

                        if (!parse_chunk_size())
                            return;

                        break;

                    case State::ParsingChunkData:

                        if (!parse_chunk_data())
                            return;

                        break;

                    case State::ParsingChunkDataCRLF:

                        if (!parse_chunk_data_crlf())
                            return;

                        break;

                    case State::ParsingTrailers:

                        if (!parse_trailers())
                            return;

                        break;

                    case State::Complete:
                    case State::Error:

                        return;
                    }
                }
            }
            catch (const std::exception &e)
            {
                fail(e.what());
            }
        }

        // 从 max(pos_, scan_pos_) 开始找 "\r\n"，避免重复扫描已确认不含 "\r\n" 的区间。
        // 找不到时，把 scan_pos_ 推进到安全位置（留一字节防止分割 \r\n）。
        std::size_t find_crlf(std::size_t from)
        {
            const auto start = std::max(from, scan_pos_);

            const auto pos = buffer_.find("\r\n", start);

            if (pos == std::string::npos)
            {
                // 保留最后一个字节，防止 \r 和 \n 被分割在两次 feed 之间
                scan_pos_ = buffer_.empty() ? 0 : buffer_.size() - 1;

                if (scan_pos_ < from)
                    scan_pos_ = from;
            }
            else
            {
                // 找到了，游标可以清掉（下次从新的 pos_ 开始找）
                scan_pos_ = 0;
            }

            return pos;
        }

        // ============================================================
        // Start line
        // ============================================================

        bool parse_start_line()
        {
            const auto end =
                find_crlf(pos_);

            if (end == std::string::npos)
            {
                if (buffer_.size() - pos_ >
                    limits_.max_start_line)
                {
                    fail("start line too long");
                }

                return false;
            }

            const auto length =
                end - pos_;

            if (length >
                limits_.max_start_line)
            {
                fail("start line too long");

                return false;
            }

            std::string_view line(
                buffer_.data() + pos_,
                length);

            pos_ =
                end + 2;

            // Response:
            //
            // HTTP/1.1 200 OK
            //

            if (line.size() >= 5 &&
                line.substr(0, 5) == "HTTP/")
            {
                return parse_response_start_line(line);
            }

            // Request:
            //
            // GET / HTTP/1.1
            //

            return parse_request_start_line(line);
        }

        // ============================================================
        // Request start line
        // ============================================================

        bool parse_request_start_line(
            std::string_view line)
        {
            message_.type =
                Type::Request;

            auto p1 =
                line.find(' ');

            if (p1 == std::string_view::npos)
            {
                fail("invalid request line");

                return false;
            }

            auto p2 =
                line.find(
                    ' ',
                    p1 + 1);

            if (p2 == std::string_view::npos)
            {
                fail("invalid request line");

                return false;
            }

            if (p1 == 0 ||
                p2 <= p1 + 1 ||
                p2 + 1 >= line.size())
            {
                fail("invalid request line");

                return false;
            }

            message_.method =
                std::string(
                    line.substr(
                        0,
                        p1));

            message_.target =
                std::string(
                    line.substr(
                        p1 + 1,
                        p2 - p1 - 1));

            const auto version =
                line.substr(
                    p2 + 1);

            if (!parse_http_version(version))
                return false;

            message_.connect =
                iequals(
                    message_.method,
                    "CONNECT");

            parse_target();

            state_ =
                State::ParsingHeaders;

            return true;
        }

        // ============================================================
        // Response start line
        // ============================================================

        bool parse_response_start_line(
            std::string_view line)
        {
            message_.type =
                Type::Response;

            auto p1 =
                line.find(' ');

            if (p1 == std::string_view::npos)
            {
                fail("invalid response line");

                return false;
            }

            const auto version =
                line.substr(
                    0,
                    p1);

            if (!parse_http_version(version))
                return false;

            auto p2 =
                line.find(
                    ' ',
                    p1 + 1);

            std::string_view code;

            if (p2 == std::string_view::npos)
            {
                code =
                    line.substr(
                        p1 + 1);

                message_.reason.clear();
            }
            else
            {
                code =
                    line.substr(
                        p1 + 1,
                        p2 - p1 - 1);

                message_.reason =
                    std::string(
                        line.substr(
                            p2 + 1));
            }

            if (code.size() != 3)
            {
                fail("invalid status code");

                return false;
            }

            for (char c : code)
            {
                if (!is_digit(c))
                {
                    fail("invalid status code");

                    return false;
                }
            }

            const unsigned int status =
                static_cast<unsigned int>(
                    (code[0] - '0') * 100 +
                    (code[1] - '0') * 10 +
                    (code[2] - '0'));

            if (status < 100 ||
                status > 599)
            {
                fail("invalid status code");

                return false;
            }

            message_.status_code =
                status;

            state_ =
                State::ParsingHeaders;

            return true;
        }

        // ============================================================
        // HTTP version
        // ============================================================

        bool parse_http_version(
            std::string_view version)
        {
            if (version == "HTTP/1.0")
            {
                message_.version = 10;

                return true;
            }

            if (version == "HTTP/1.1")
            {
                message_.version = 11;

                return true;
            }

            fail("unsupported HTTP version");

            return false;
        }

        // ============================================================
        // Headers
        // ============================================================

        bool parse_headers()
        {
            if (header_end_ == std::string::npos)
            {
                header_end_ =
                    buffer_.find(
                        "\r\n\r\n",
                        pos_);

                if (header_end_ == std::string::npos)
                {
                    if (buffer_.size() >
                        limits_.max_header_bytes)
                    {
                        fail("headers too large");
                    }

                    return false;
                }
            }

            const auto total_header_bytes =
                header_end_ + 4;

            if (total_header_bytes >
                limits_.max_header_bytes)
            {
                fail("headers too large");

                return false;
            }

            while (pos_ < header_end_)
            {
                const auto end =
                    buffer_.find(
                        "\r\n",
                        pos_);

                if (end == std::string::npos ||
                    end > header_end_)
                {
                    fail("invalid header");

                    return false;
                }

                std::string_view line(
                    buffer_.data() + pos_,
                    end - pos_);

                pos_ =
                    end + 2;

                if (line.empty())
                    break;

                // Reject obsolete line folding.
                if (line.front() == ' ' ||
                    line.front() == '\t')
                {
                    fail("obsolete header folding");

                    return false;
                }

                const auto colon =
                    line.find(':');

                if (colon == std::string_view::npos)
                {
                    fail("invalid header");

                    return false;
                }

                if (colon == 0)
                {
                    fail("empty header name");

                    return false;
                }

                const auto name =
                    line.substr(
                        0,
                        colon);

                const auto value =
                    trim(
                        line.substr(
                            colon + 1));

                if (!valid_header_name(name))
                {
                    fail("invalid header name");

                    return false;
                }

                if (!valid_header_value(value))
                {
                    fail("invalid header value");

                    return false;
                }

                if (message_.headers.size() >=
                    limits_.max_headers)
                {
                    fail("too many headers");

                    return false;
                }

                message_.headers.push_back(
                    Header{
                        std::string(name),
                        std::string(value)});
            }

            pos_ =
                header_end_ + 4;

            body_start_ =
                pos_;

            if (!process_headers())
                return false;

            // ========================================================
            // Response special cases
            // ========================================================

            if (message_.type ==
                Type::Response)
            {
                // 1xx response
                if (message_.status_code >= 100 &&
                    message_.status_code < 200)
                {
                    finish_message();

                    return true;
                }

                // 204 No Content
                if (message_.status_code == 204)
                {
                    finish_message();

                    return true;
                }

                // 304 Not Modified
                if (message_.status_code == 304)
                {
                    finish_message();

                    return true;
                }

                // CONNECT 2xx:
                //
                // HTTP ends here and the connection becomes a raw
                // byte tunnel.
                //
                if (message_.connect &&
                    message_.status_code >= 200 &&
                    message_.status_code < 300)
                {
                    finish_message();

                    return true;
                }
            }

            // ========================================================
            // Chunked
            // ========================================================

            if (message_.chunked)
            {
                message_.has_body = true;

                state_ =
                    State::ParsingChunkSize;

                return true;
            }

            // ========================================================
            // Content-Length
            // ========================================================

            if (message_.content_length.has_value())
            {
                const auto length =
                    *message_.content_length;

                if (length >
                    limits_.max_body_bytes)
                {
                    fail("body too large");

                    return false;
                }

                if (length == 0)
                {
                    message_.has_body = false;

                    finish_message();

                    return true;
                }

                message_.has_body = true;

                state_ =
                    State::ParsingBody;

                return true;
            }

            // ========================================================
            // Request without body framing
            //
            // A normal HTTP request without CL/TE has no body.
            // ========================================================

            if (message_.type ==
                Type::Request)
            {
                message_.has_body = false;

                finish_message();

                return true;
            }

            // ========================================================
            // Response without CL/TE
            //
            // Body ends when TCP connection closes.
            // ========================================================

            state_ =
                State::ParsingBodyUntilEOF;

            return true;
        }

        // ============================================================
        // Process headers
        // ============================================================

        bool process_headers()
        {
            bool has_transfer_encoding = false;

            std::vector<std::string>
                transfer_codings;

            std::vector<std::string>
                content_lengths;

            for (const auto &h :
                 message_.headers)
            {
                if (iequals(
                        h.name,
                        "Connection"))
                {
                    parse_connection(
                        h.value);
                }
                else if (iequals(
                             h.name,
                             "Content-Length"))
                {
                    content_lengths.push_back(
                        h.value);
                }
                else if (iequals(
                             h.name,
                             "Transfer-Encoding"))
                {
                    has_transfer_encoding = true;

                    parse_transfer_encoding(
                        h.value,
                        transfer_codings);
                }
                else if (iequals(
                             h.name,
                             "Upgrade"))
                {
                    message_.upgrade = true;
                }
            }

            // ========================================================
            // Content-Length
            // ========================================================

            if (!content_lengths.empty())
            {
                std::optional<std::uint64_t>
                    parsed_length;

                for (const auto &value :
                     content_lengths)
                {
                    std::uint64_t length = 0;

                    if (!parse_uint64(
                            value,
                            length))
                    {
                        fail("invalid Content-Length");

                        return false;
                    }

                    if (!parsed_length.has_value())
                    {
                        parsed_length =
                            length;
                    }
                    else if (*parsed_length != length)
                    {
                        // Multiple conflicting CL headers.
                        //
                        // Reject to avoid request smuggling ambiguity.
                        fail("conflicting Content-Length");

                        return false;
                    }
                }

                content_length_value_ =
                    parsed_length;

                message_.content_length =
                    parsed_length;
            }

            // ========================================================
            // Transfer-Encoding
            // ========================================================

            if (has_transfer_encoding)
            {
                if (transfer_codings.empty())
                {
                    fail("invalid Transfer-Encoding");

                    return false;
                }

                // chunked must be final.
                if (!iequals(
                        transfer_codings.back(),
                        "chunked"))
                {
                    fail(
                        "chunked must be final transfer coding");

                    return false;
                }

                message_.chunked = true;

                // Reject CL + TE.
                //
                // Important for a proxy because accepting both can
                // create request-smuggling ambiguity.
                //
                if (message_.content_length.has_value())
                {
                    fail(
                        "Content-Length with Transfer-Encoding");

                    return false;
                }
            }

            // ========================================================
            // Keep-Alive
            // ========================================================

            if (message_.version == 11)
            {
                // HTTP/1.1 persistent by default.
                message_.keep_alive =
                    !message_.connection_close;
            }
            else
            {
                // HTTP/1.0 non-persistent by default.
                message_.keep_alive =
                    message_.connection_keep_alive;
            }

            return true;
        }

        // ============================================================
        // Connection header
        // ============================================================

        void parse_connection(
            std::string_view value)
        {
            std::size_t start = 0;

            while (start < value.size())
            {
                const auto comma =
                    value.find(
                        ',',
                        start);

                auto part =
                    value.substr(
                        start,
                        comma == std::string_view::npos
                            ? value.size() - start
                            : comma - start);

                part = trim(part);

                if (iequals(
                        part,
                        "close"))
                {
                    message_.connection_close =
                        true;
                }
                else if (iequals(
                             part,
                             "keep-alive"))
                {
                    message_.connection_keep_alive =
                        true;
                }
                else if (iequals(
                             part,
                             "upgrade"))
                {
                    message_.upgrade =
                        true;
                }

                if (comma ==
                    std::string_view::npos)
                {
                    break;
                }

                start =
                    comma + 1;
            }
        }

        // ============================================================
        // Transfer-Encoding
        // ============================================================

        void parse_transfer_encoding(
            std::string_view value,
            std::vector<std::string> &codings)
        {
            std::size_t start = 0;

            while (start < value.size())
            {
                const auto comma =
                    value.find(
                        ',',
                        start);

                auto part =
                    value.substr(
                        start,
                        comma == std::string_view::npos
                            ? value.size() - start
                            : comma - start);

                part = trim(part);

                // Remove parameters:
                //
                // chunked;foo=bar
                //
                const auto semicolon =
                    part.find(';');

                if (semicolon !=
                    std::string_view::npos)
                {
                    part =
                        part.substr(
                            0,
                            semicolon);
                }

                part = trim(part);

                if (!part.empty())
                {
                    codings.emplace_back(part);
                }

                if (comma ==
                    std::string_view::npos)
                {
                    break;
                }

                start =
                    comma + 1;
            }
        }

        // ============================================================
        // Content-Length body
        // ============================================================

        bool parse_content_length_body()
        {
            if (!content_length_value_.has_value())
            {
                fail("internal parser error");

                return false;
            }

            const auto length =
                *content_length_value_;

            const auto available =
                buffer_.size() - body_start_;

            if (available < length)
            {
                // TCP half packet.
                return false;
            }

            if (length > 0)
            {
                message_.body.assign(
                    buffer_.data() + body_start_,
                    static_cast<std::size_t>(length));
            }

            pos_ =
                body_start_ +
                static_cast<std::size_t>(length);

            finish_message();

            return true;
        }

        // ============================================================
        // Chunk size
        // ============================================================

        bool parse_chunk_size()
        {
            const auto end = find_crlf(pos_);

            if (end == std::string::npos)
            {
                if (buffer_.size() - pos_ >
                    limits_.max_chunk_line)
                {
                    fail("chunk size line too long");
                }

                return false;
            }

            const auto line_size =
                end - pos_;

            if (line_size >
                limits_.max_chunk_line)
            {
                fail("chunk size line too long");

                return false;
            }

            std::string_view line(
                buffer_.data() + pos_,
                line_size);

            pos_ =
                end + 2;

            // Ignore chunk extensions.
            const auto semicolon =
                line.find(';');

            if (semicolon !=
                std::string_view::npos)
            {
                line =
                    line.substr(
                        0,
                        semicolon);
            }

            line = trim(line);

            if (line.empty())
            {
                fail("empty chunk size");

                return false;
            }

            std::uint64_t size = 0;

            if (!parse_hex_uint64(
                    line,
                    size))
            {
                fail("invalid chunk size");

                return false;
            }

            chunk_remaining_ =
                size;

            // Last chunk.
            if (size == 0)
            {
                trailer_start_ = pos_; // 新增：记录 trailers 起始偏移
                state_ =
                    State::ParsingTrailers;

                return true;
            }

            if (size > limits_.max_body_bytes ||
                message_.body.size() > limits_.max_body_bytes - size)
            {
                fail("chunked body too large");

                return false;
            }

            state_ =
                State::ParsingChunkData;

            return true;
        }

        // ============================================================
        // Chunk data
        // ============================================================

        bool parse_chunk_data()
        {
            const auto available =
                buffer_.size() - pos_;

            if (available < chunk_remaining_)
            {
                // Half packet.
                return false;
            }

            if (chunk_remaining_ > 0)
            {
                message_.body.append(
                    buffer_.data() + pos_,
                    static_cast<std::size_t>(
                        chunk_remaining_));

                pos_ +=
                    static_cast<std::size_t>(
                        chunk_remaining_);
            }

            chunk_remaining_ = 0;

            state_ =
                State::ParsingChunkDataCRLF;

            return true;
        }

        // ============================================================
        // Chunk CRLF
        // ============================================================

        bool parse_chunk_data_crlf()
        {
            if (buffer_.size() - pos_ < 2)
            {
                return false;
            }

            if (buffer_[pos_] != '\r' ||
                buffer_[pos_ + 1] != '\n')
            {
                fail("missing chunk CRLF");

                return false;
            }

            pos_ += 2;

            state_ =
                State::ParsingChunkSize;

            return true;
        }

        // ============================================================
        // Trailers
        // ============================================================

        bool parse_trailers()
        {
            for (;;)
            {
                // 整体字节数上限：从 trailers 起始到当前扫描位置
                if (pos_ - trailer_start_ >
                    limits_.max_trailer_bytes)
                {
                    fail("trailers too large");
                    return false;
                }

                const auto end = find_crlf(pos_);

                if (end == std::string::npos)
                {
                    if (buffer_.size() - pos_ >
                        limits_.max_trailer_line)
                    {
                        fail("trailer line too long");
                        return false;
                    }

                    if (buffer_.size() - trailer_start_ >
                        limits_.max_trailer_bytes)
                    {
                        fail("trailers too large");
                        return false;
                    }

                    return false;
                }

                const auto line_size = end - pos_;

                if (line_size > limits_.max_trailer_line)
                {
                    fail("trailer line too long");
                    return false;
                }

                std::string_view line(
                    buffer_.data() + pos_,
                    line_size);

                pos_ =
                    end + 2;

                // Empty line terminates trailers.
                if (line.empty())
                {
                    finish_message();

                    return true;
                }

                if (message_.trailers.size() >=
                    limits_.max_trailers)
                {
                    fail("too many trailers");

                    return false;
                }

                const auto colon =
                    line.find(':');

                if (colon == std::string_view::npos ||
                    colon == 0)
                {
                    fail("invalid trailer");

                    return false;
                }

                const auto name =
                    line.substr(
                        0,
                        colon);

                const auto value =
                    trim(
                        line.substr(
                            colon + 1));

                if (!valid_header_name(name) ||
                    !valid_header_value(value))
                {
                    fail("invalid trailer");

                    return false;
                }

                message_.trailers.push_back(
                    Header{
                        std::string(name),
                        std::string(value)});
            }
        }

        // ============================================================
        // Complete message
        // ============================================================

        void finish_message()
        {
            state_ =
                State::Complete;

            // Everything before pos_ belongs to this HTTP message.
            //
            // Everything after pos_ belongs to the next message.
            //
            // Example:
            //
            // [HTTP #1][HTTP #2]
            //          ^
            //          remaining()
            //

            if (pos_ > 0)
            {
                buffer_.erase(
                    0,
                    pos_);

                pos_ = 0;

                scan_pos_ = (scan_pos_ > pos_) ? (scan_pos_ - pos_) : 0;

                header_end_ =
                    std::string::npos;

                body_start_ =
                    std::string::npos;
            }
        }

        // ============================================================
        // Error
        // ============================================================

        void fail(std::string message)
        {
            state_ =
                State::Error;

            error_message_ =
                std::move(message);
        }

        // ============================================================
        // Utility
        // ============================================================

        static bool is_digit(char c)
        {
            return c >= '0' &&
                   c <= '9';
        }

        static bool is_hex_digit(char c)
        {
            return (c >= '0' && c <= '9') ||
                   (c >= 'a' && c <= 'f') ||
                   (c >= 'A' && c <= 'F');
        }

        static unsigned int hex_value(char c)
        {
            if (c >= '0' && c <= '9')
                return c - '0';

            if (c >= 'a' && c <= 'f')
                return c - 'a' + 10;

            return c - 'A' + 10;
        }

        static bool iequals(
            std::string_view a,
            std::string_view b)
        {
            if (a.size() != b.size())
                return false;

            for (std::size_t i = 0;
                 i < a.size();
                 ++i)
            {
                if (std::tolower(
                        static_cast<unsigned char>(a[i])) !=
                    std::tolower(
                        static_cast<unsigned char>(b[i])))
                {
                    return false;
                }
            }

            return true;
        }

        static std::string_view trim(
            std::string_view value)
        {
            while (!value.empty() &&
                   (value.front() == ' ' ||
                    value.front() == '\t'))
            {
                value.remove_prefix(1);
            }

            while (!value.empty() &&
                   (value.back() == ' ' ||
                    value.back() == '\t'))
            {
                value.remove_suffix(1);
            }

            return value;
        }

        // RFC token / tchar
        static bool is_tchar(char c)
        {
            if ((c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9'))
            {
                return true;
            }

            switch (c)
            {
            case '!':
            case '#':
            case '$':
            case '%':
            case '&':
            case '\'':
            case '*':
            case '+':
            case '-':
            case '.':
            case '^':
            case '_':
            case '`':
            case '|':
            case '~':
                return true;

            default:
                return false;
            }
        }

        static bool valid_header_name(
            std::string_view name)
        {
            if (name.empty())
                return false;

            for (char c : name)
            {
                if (!is_tchar(c))
                    return false;
            }

            return true;
        }

        static bool valid_header_value(
            std::string_view value)
        {
            for (char c : value)
            {
                if (c == '\r' ||
                    c == '\n' ||
                    c == '\0')
                {
                    return false;
                }
            }

            return true;
        }

        static bool parse_uint64(
            std::string_view text,
            std::uint64_t &result)
        {
            text = trim(text);

            if (text.empty())
                return false;

            std::uint64_t value = 0;

            for (char c : text)
            {
                if (!is_digit(c))
                    return false;

                const auto digit =
                    static_cast<std::uint64_t>(
                        c - '0');

                if (value >
                    (std::numeric_limits<std::uint64_t>::max() - digit) / 10)
                {
                    return false;
                }

                value =
                    value * 10 + digit;
            }

            result = value;

            return true;
        }

        static bool parse_hex_uint64(
            std::string_view text,
            std::uint64_t &result)
        {
            text = trim(text);

            if (text.empty())
                return false;

            std::uint64_t value = 0;

            for (char c : text)
            {
                if (!is_hex_digit(c))
                    return false;

                const auto digit =
                    static_cast<std::uint64_t>(
                        hex_value(c));

                if (value >
                    (std::numeric_limits<std::uint64_t>::max() - digit) / 16)
                {
                    return false;
                }

                value =
                    value * 16 + digit;
            }

            result = value;

            return true;
        }

        void parse_target()
        {
            const std::string_view target =
                message_.target;

            // CONNECT:
            //
            // CONNECT example.com:443 HTTP/1.1
            //

            if (message_.connect)
            {
                message_.path =
                    message_.target;

                message_.query.clear();

                return;
            }

            const auto question =
                target.find('?');

            if (question ==
                std::string_view::npos)
            {
                message_.path =
                    std::string(target);

                message_.query.clear();
            }
            else
            {
                message_.path =
                    std::string(
                        target.substr(
                            0,
                            question));

                message_.query =
                    std::string(
                        target.substr(
                            question + 1));
            }
        }
    };

    // 在转发前调用，原地清理 message，使其适合发给下一跳
    inline void prepare_for_forwarding(
        HttpParser::Message &msg,
        std::string_view proxy_via_name,
        std::string_view client_ip)
    {
        // 1. 收集 Connection 头里点名的 hop-by-hop 头名
        std::vector<std::string> hop_by_hop = {
            "Connection", "Keep-Alive", "Proxy-Authenticate",
            "Proxy-Authorization", "TE", "Trailer",
            "Transfer-Encoding", "Upgrade"};

        for (const auto &h : msg.headers)
        {
            if (HttpParser::Message::iequals(h.name, "Connection"))
            {
                // Connection 头的值里列出的头名也要去掉
                // (简单起见，按逗号切分)
                std::size_t start = 0;
                while (start < h.value.size())
                {
                    auto comma = h.value.find(',', start);
                    auto part = h.value.substr(
                        start,
                        comma == std::string::npos
                            ? h.value.size() - start
                            : comma - start);
                    // trim + push_back 到 hop_by_hop（省略 trim 细节）
                    hop_by_hop.push_back(part);
                    if (comma == std::string::npos)
                        break;
                    start = comma + 1;
                }
            }
        }

        // 2. 删除这些头
        msg.headers.erase(
            std::remove_if(
                msg.headers.begin(),
                msg.headers.end(),
                [&](const HttpParser::Header &h)
                {
                    for (const auto &name : hop_by_hop)
                    {
                        if (HttpParser::Message::iequals(h.name, name))
                            return true;
                    }
                    return false;
                }),
            msg.headers.end());

        // 3. 删除客户端原始的 framing 头，
        //    让 serialize() 用 msg.body.size() / msg.chunked 重新生成
        msg.headers.erase(
            std::remove_if(
                msg.headers.begin(),
                msg.headers.end(),
                [](const HttpParser::Header &h)
                {
                    return HttpParser::Message::iequals(h.name, "Content-Length") || HttpParser::Message::iequals(h.name, "Transfer-Encoding");
                }),
            msg.headers.end());

        // 4. 加代理自己的头
        msg.headers.push_back({"Via", std::string(proxy_via_name)});

        if (msg.type == HttpParser::Type::Request)
        {
            msg.headers.push_back(
                {"X-Forwarded-For", std::string(client_ip)});
        }
    }
} // namespace p2psocks