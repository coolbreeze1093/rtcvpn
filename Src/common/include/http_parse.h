#pragma once
#include <functional>
#include <string_view>
#include <string>
#include <vector>
#include <optional>
#include <cctype>
#include <cstdio>
#include <stdexcept>

#include <llhttp.h>

namespace p2psocks
{
    class HttpParser
    {
    public:
        struct Header
        {
            std::string name;
            std::string value;
        };

        struct Limits
        {
        };

        enum class Type
        {
            Unknown,
            Request,
            Response
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
            //
            // 注意：如果调用方注册了 on_body 回调，这个字段不会被
            // 填充（避免大文件把内存吃满）。只有在没注册回调、走
            // "传统一次性拿整个 message" 模式时，这里才会有完整数据。
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
            std::string serialize() const
            {
                std::string out;

                out.reserve(body.size() + 256);

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

                const bool has_cl = has_header("Content-Length");
                const bool has_te = has_header("Transfer-Encoding");

                for (const auto &h : headers)
                {
                    out += h.name;
                    out += ": ";
                    out += h.value;
                    out += "\r\n";
                }

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

            std::string serialize_headers_only(
                bool force_chunked = false,
                std::optional<std::uint64_t> force_content_length = std::nullopt) const
            {
                std::string out;

                out.reserve(256);

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

                const bool has_cl = has_header("Content-Length");
                const bool has_te = has_header("Transfer-Encoding");

                for (const auto &h : headers)
                {
                    out += h.name;
                    out += ": ";
                    out += h.value;
                    out += "\r\n";
                }

                if (!has_cl && !has_te)
                {
                    if (force_chunked)
                    {
                        out += "Transfer-Encoding: chunked\r\n";
                    }
                    else if (force_content_length.has_value())
                    {
                        out += "Content-Length: ";
                        out += std::to_string(*force_content_length);
                        out += "\r\n";
                    }
                }

                out += "\r\n";

                return out;
            }

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

        using HeadersCompleteCb = std::function<void(Message &)>;
        using BodyDataCb = std::function<void(const char *data, std::size_t len)>;
        using MessageCompleteCb = std::function<void()>;
        // 解析出错时回调，errno_name 是 llhttp 的错误名（如 "HPE_INVALID_METHOD"）
        using ErrorCb = std::function<void(int errno_code, std::string_view reason)>;
        using PausedUpgradeCb = std::function<void(std::string_view)>;

        explicit HttpParser(Type type = Type::Request)
        {
            init(type);
        }

        ~HttpParser() = default;

        HttpParser(const HttpParser &) = delete;
        HttpParser &operator=(const HttpParser &) = delete;

        void set_on_headers_complete(HeadersCompleteCb cb)
        {
            on_headers_complete_ = std::move(cb);
        }

        void set_on_body(BodyDataCb cb)
        {
            on_body_ = std::move(cb);
        }

        void set_on_message_complete(MessageCompleteCb cb)
        {
            on_message_complete_ = std::move(cb);
        }

        void set_on_error(ErrorCb cb)
        {
            on_error_ = std::move(cb);
        }

        void set_on_pa_upgrade(PausedUpgradeCb cb)
        {
            on_paused_upgrade_ = std::move(cb);
        }

        // 是否已经解析出一条完整的 message（message_complete 已触发）
        bool complete() const
        {
            return message_complete_;
        }

        // llhttp 在遇到 upgrade（如 CONNECT / WebSocket）时会停止解析，
        // 剩余没消费的字节需要调用方自己处理（比如透传给隧道）。
        std::string take_remaining()
        {
            std::string remaining = std::move(remaining_after_upgrade_);
            remaining_after_upgrade_.clear();
            return remaining;
        }

        // 重置内部状态，重新开始解析下一条 message。
        // type = Unknown 时沿用上一次 init 的类型。
        void reset(Type type = Type::Unknown)
        {
            init(type == Type::Unknown ? type_ : type);
        }

        // 喂数据。返回值表示已经消费到的字节数是否发生了错误：
        // - 成功：内部通过回调把解析结果吐出去
        // - 失败：触发 on_error_，并且此后再 feed 会直接返回，
        //   需要调用方 reset() 后才能继续解析

        bool feed(const char *data, size_t size)
        {
            if (had_error_)
                return false;

            llhttp_errno_t err = llhttp_execute(&parser_, data, size);

            if (err == HPE_PAUSED_UPGRADE)
            {
                // upgrade 场景：llhttp 已经把 headers 解析完了，
                // 剩下未消费的字节保存起来交给上层。
                const char *consumed_end = llhttp_get_error_pos(&parser_);
                if (consumed_end != nullptr && consumed_end >= data &&
                    static_cast<size_t>(consumed_end - data) <= size)
                {
                    remaining_after_upgrade_.assign(
                        consumed_end, data + size - consumed_end);
                }

                // 恢复解析器，避免下次 feed 直接报错
                llhttp_resume_after_upgrade(&parser_);

                if (on_paused_upgrade_)
                {
                    on_paused_upgrade_(remaining_after_upgrade_);
                }

                return false;
            }

            if (err != HPE_OK)
            {
                had_error_ = true;

                if (on_error_)
                {
                    on_error_(
                        static_cast<int>(err),
                        llhttp_get_error_reason(&parser_));
                }
                return false;
            }
            
            return true;
        }

        Message & message()
        {
            return message_;
        }

    private:
        void init(Type type)
        {
            type_ = (type == Type::Unknown) ? Type::Request : type;

            llhttp_type_t llhttp_type =
                (type_ == Type::Response) ? HTTP_RESPONSE : HTTP_REQUEST;

            llhttp_settings_init(&settings_);

            settings_.on_message_begin = &HttpParser::on_message_begin_cb;
            settings_.on_url = &HttpParser::on_url_cb;
            settings_.on_status = &HttpParser::on_status_cb;
            settings_.on_header_field = &HttpParser::on_header_field_cb;
            settings_.on_header_value = &HttpParser::on_header_value_cb;
            settings_.on_headers_complete = &HttpParser::on_headers_complete_cb;
            settings_.on_body = &HttpParser::on_body_cb;
            settings_.on_message_complete = &HttpParser::on_message_complete_cb;

            llhttp_init(&parser_, llhttp_type, &settings_);
            parser_.data = this;

            message_ = Message{};
            message_.type = type_;

            cur_header_field_.clear();
            cur_header_value_.clear();
            cur_url_.clear();
            cur_status_.clear();
            in_header_value_ = false;

            message_complete_ = false;
            had_error_ = false;
            remaining_after_upgrade_.clear();
        }

        // ------------------------------------------------------------
        // llhttp 回调（静态 trampoline，通过 parser->data 拿回 this）
        // ------------------------------------------------------------

        static HttpParser *self(llhttp_t *p)
        {
            return static_cast<HttpParser *>(p->data);
        }

        static int on_message_begin_cb(llhttp_t *p)
        {
            auto *s = self(p);
            s->message_ = Message{};
            s->message_.type = s->type_;
            s->cur_header_field_.clear();
            s->cur_header_value_.clear();
            s->cur_url_.clear();
            s->cur_status_.clear();
            s->in_header_value_ = false;
            s->message_complete_ = false;
            return 0;
        }

        static int on_url_cb(llhttp_t *p, const char *at, size_t length)
        {
            self(p)->cur_url_.append(at, length);
            return 0;
        }

        static int on_status_cb(llhttp_t *p, const char *at, size_t length)
        {
            self(p)->cur_status_.append(at, length);
            return 0;
        }

        static int on_header_field_cb(llhttp_t *p, const char *at, size_t length)
        {
            auto *s = self(p);

            // 上一个 header 的 value 已经结束（field/value 交替出现），
            // 把上一对 push 进去。
            if (s->in_header_value_)
            {
                s->message_.headers.push_back(
                    {std::move(s->cur_header_field_), std::move(s->cur_header_value_)});
                s->cur_header_field_.clear();
                s->cur_header_value_.clear();
                s->in_header_value_ = false;
            }

            s->cur_header_field_.append(at, length);
            return 0;
        }

        static int on_header_value_cb(llhttp_t *p, const char *at, size_t length)
        {
            auto *s = self(p);
            s->in_header_value_ = true;
            s->cur_header_value_.append(at, length);
            return 0;
        }

        static int on_headers_complete_cb(llhttp_t *p)
        {
            auto *s = self(p);

            // flush 最后一对 field/value
            if (!s->cur_header_field_.empty() || s->in_header_value_)
            {
                s->message_.headers.push_back(
                    {std::move(s->cur_header_field_), std::move(s->cur_header_value_)});
                s->cur_header_field_.clear();
                s->cur_header_value_.clear();
                s->in_header_value_ = false;
            }

            s->message_.version = p->http_major * 10 + p->http_minor;

            if (s->type_ == Type::Request)
            {
                s->message_.method = llhttp_method_name(
                    static_cast<llhttp_method_t>(p->method));
                s->message_.target = s->cur_url_;
                s->message_.connect =
                    (static_cast<llhttp_method_t>(p->method) == HTTP_CONNECT);

                auto qpos = s->cur_url_.find('?');
                if (qpos == std::string::npos)
                {
                    s->message_.path = s->cur_url_;
                }
                else
                {
                    s->message_.path = s->cur_url_.substr(0, qpos);
                    s->message_.query = s->cur_url_.substr(qpos + 1);
                }
            }
            else
            {
                s->message_.status_code = p->status_code;
                s->message_.reason = s->cur_status_;
            }

            s->message_.chunked = (p->flags & F_CHUNKED) != 0;
            s->message_.upgrade = (p->upgrade != 0);
            s->message_.connection_keep_alive =
                (p->flags & F_CONNECTION_KEEP_ALIVE) != 0;
            s->message_.connection_close =
                (p->flags & F_CONNECTION_CLOSE) != 0;
            s->message_.keep_alive = llhttp_should_keep_alive(p) != 0;

            if (p->flags & F_CONTENT_LENGTH)
            {
                s->message_.content_length = p->content_length;
            }

            s->message_.has_body = !(
                s->message_.content_length.has_value() &&
                *s->message_.content_length == 0) ||
                                    s->message_.chunked;

            if (s->on_headers_complete_)
            {
                s->on_headers_complete_(s->message_);
            }

            // CONNECT / Upgrade：llhttp 内部检测到这是 upgrade 类型的消息后，
            // 会在 headers 结束的位置自动暂停（llhttp_execute 返回
            // HPE_PAUSED_UPGRADE），不需要这里手动返回特殊值；
            // 剩余未消费字节会在 feed() 里被捕获并交给上层处理。
            return 0;
        }

        static int on_body_cb(llhttp_t *p, const char *at, size_t length)
        {
            auto *s = self(p);

            if (s->on_body_)
            {
                s->on_body_(at, length);
            }
            else
            {
                // 没注册流式回调时，退回到"整包攒在 Message::body 里"的模式
                s->message_.body.append(at, length);
            }

            return 0;
        }

        static int on_message_complete_cb(llhttp_t *p)
        {
            auto *s = self(p);
            s->message_complete_ = true;

            if (s->on_message_complete_)
            {
                s->on_message_complete_();
            }

            return 0;
        }

        HeadersCompleteCb on_headers_complete_;
        BodyDataCb on_body_;
        MessageCompleteCb on_message_complete_;
        ErrorCb on_error_;
        PausedUpgradeCb on_paused_upgrade_;

        llhttp_t parser_{};
        llhttp_settings_t settings_{};

        Type type_ = Type::Request;
        Message message_;

        std::string cur_header_field_;
        std::string cur_header_value_;
        bool in_header_value_ = false;

        std::string cur_url_;
        std::string cur_status_;

        bool message_complete_ = false;
        bool had_error_ = false;

        std::string remaining_after_upgrade_;
    };

    // 在转发前调用，原地清理 message，使其适合发给下一跳
    inline void prepare_for_forwarding(
        HttpParser::Message &msg,
        std::string_view proxy_via_name,
        std::string_view client_ip)
    {
        // 1. 收集 Connection 头里点名的 hop-by-hop 头名
        std::vector<std::string> hop_by_hop = {
            "Proxy-Authenticate",
            "Proxy-Authorization", "TE", "Trailer",
            "Upgrade"};

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
}