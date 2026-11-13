#include "src/proxy/admin.h"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "src/base/logger.h"
#include "src/proxy/proxy.h"

namespace beast = boost::beast;
namespace http  = beast::http;
using tcp       = asio::ip::tcp;

namespace {

using Request  = http::request<http::string_body>;
using Response = http::response<http::string_body>;

/// Escape text for HTML.
string escape(std::string_view text) {
  string out;
  for (char c : text) {
    switch (c) {
      case '&':
        out += "&amp;";
        break;
      case '<':
        out += "&lt;";
        break;
      case '>':
        out += "&gt;";
        break;
      case '"':
        out += "&quot;";
        break;
      default:
        out += c;
    }
  }
  return out;
}

/// A JSON value as table-cell text: strings without quotes, booleans as yes/no, null as "-".
string cell(boost::json::value const& v) {
  if (v.is_null())
    return "-";
  if (v.is_string())
    return escape(v.as_string());
  if (v.is_bool())
    return v.as_bool() ? "yes" : "no";
  return escape(boost::json::serialize(v));
}

constexpr char const* kHtmlHead =
    "<!doctype html><html><head><meta charset=\"utf-8\">"
    "<meta http-equiv=\"refresh\" content=\"5\"><title>pg-raft proxy</title><style>"
    "body{font-family:sans-serif;margin:2em}table{border-collapse:collapse}"
    "td,th{border:1px solid #999;padding:4px 10px;text-align:left}th{background:#eee}"
    ".yes{color:#070;font-weight:bold}</style></head><body>";

/// The /primary page.
string primary_html(boost::json::object const& p) {
  string html = kHtmlHead;
  html += "<h1>pg-raft primary</h1>";
  if (p.at("server").is_null())
    html += "<p>No primary: an election or a promotion is in progress.</p>";
  else
    html += std::format("<p>Server {} &mdash; {}:{}</p>",
                        cell(p.at("server")),
                        cell(p.at("hostname")),
                        cell(p.at("pg_port")));
  return html + "</body></html>\n";
}

/// The /status page.
string status_html(boost::json::object const& st) {
  string html = kHtmlHead;
  html += "<h1>pg-raft proxy</h1>";
  html +=
      std::format("<p>Primary port {}; primary: {}.</p>",
                  cell(st.at("proxy_pg_port")),
                  st.at("primary").is_null() ? string{"none"} : "server " + cell(st.at("primary")));
  html +=
      "<table><tr><th>Server</th><th>Host</th><th>PostgreSQL</th><th>Raft</th>"
      "<th>Connected</th><th>Leader</th><th>Serving</th><th>Applied index</th>"
      "<th>Primary flushed</th><th>Standby replayed</th><th>Bridges</th></tr>";
  auto const& bridges = st.at("bridges").as_object();
  auto const& per     = bridges.at("servers").as_array();
  for (auto const& v : st.at("servers").as_array()) {
    auto const& s   = v.as_object();
    auto        get = [&s](char const* k) {
      return s.contains(k) ? s.at(k) : boost::json::value(nullptr);
    };
    auto const id = s.at("id").to_number<size_t>();
    html += "<tr>";
    for (auto const* k : {"id", "hostname", "pg_port", "ha_port"})
      html += "<td>" + cell(get(k)) + "</td>";
    for (auto const* k : {"connected", "leader", "serving"}) {
      auto const yes = get(k).is_bool() && get(k).as_bool();
      html += std::format("<td{}>{}</td>", yes ? " class=\"yes\"" : "", cell(get(k)));
    }
    for (auto const* k : {"applied_idx", "primary_flushed_lsn", "standby_applied_lsn"})
      html += "<td>" + cell(get(k)) + "</td>";
    html += "<td>" + (id < per.size() ? cell(per.at(id)) : string{"-"}) + "</td></tr>";
  }
  html += "</table>";
  html += std::format("<p>Bridges on the primary port: {}.</p>", cell(bridges.at("primary")));
  return html + "</body></html>\n";
}

Response html_response(Request const& req, http::status status, string body) {
  Response res{status, req.version()};
  res.set(http::field::server, "pg_raft_proxy");
  res.set(http::field::content_type, "text/html; charset=utf-8");
  res.keep_alive(req.keep_alive());
  res.body() = std::move(body);
  res.prepare_payload();
  return res;
}

/// The resource and whether HTML is asked for: /status and /status.json are JSON, /status.html is
/// HTML (a query string is ignored).
std::tuple<std::string_view, bool> resource(std::string_view target) {
  if (auto q = target.find('?'); q != std::string_view::npos)
    target = target.substr(0, q);
  if (target.ends_with(".html"))
    return {target.substr(0, target.size() - 5), true};
  if (target.ends_with(".json"))
    return {target.substr(0, target.size() - 5), false};
  return {target, false};
}

Response json_response(Request const& req, http::status status, boost::json::value const& body) {
  Response res{status, req.version()};
  res.set(http::field::server, "pg_raft_proxy");
  res.set(http::field::content_type, "application/json");
  res.keep_alive(req.keep_alive());
  res.body() = boost::json::serialize(body) + "\n";
  res.prepare_payload();
  return res;
}

/// One HTTP connection: requests are read and answered one at a time.
class Session : public std::enable_shared_from_this<Session> {
 public:
  Session(tcp::socket socket, weak_ptr<Proxy> proxy)
      : stream_(std::move(socket)), proxy_(std::move(proxy)) {}

  void start() { read(); }

 private:
  void read() {
    request_ = {};
    stream_.expires_after(std::chrono::seconds(30));
    http::async_read(
        stream_, buffer_, request_, [self = shared_from_this()](beast::error_code ec, size_t) {
          if (ec)
            return self->close();
          self->handle();
        });
  }

  void handle() {
    auto const proxy = proxy_.lock();
    if (!proxy)
      return close();

    auto const [target, html] = resource(std::string_view{request_.target()});
    if (request_.method() != http::verb::get)
      return write(
          json_response(request_, http::status::method_not_allowed, {{"error", "GET only"}}));

    if (target == "/health") {
      Response res{http::status::ok, request_.version()};
      res.set(http::field::content_type, "text/plain");
      res.keep_alive(request_.keep_alive());
      res.body() = "ok\n";
      res.prepare_payload();
      return write(std::move(res));
    }

    if (target == "/primary") {
      boost::json::object body{{"server", nullptr}};
      auto                status = http::status::service_unavailable;
      if (auto p = proxy->primary()) {
        auto const& s = proxy->config().servers.at(*p);
        body          = {{"server", *p}, {"hostname", s.hostname}, {"pg_port", s.pg_port}};
        status        = http::status::ok;
      }
      if (html)
        return write(html_response(request_, status, primary_html(body)));
      return write(json_response(request_, status, body));
    }

    if (target == "/status") {
      // Raft status requests block (up to 2 s per node): off the io threads, then back.
      std::thread([self = shared_from_this(), proxy, html] {
        // Nothing thrown here may end the proxy: a failure is a 500.
        auto response = [&]() -> Response {
          try {
            auto body = proxy->status();
            if (html)
              return html_response(self->request_, http::status::ok, status_html(body));
            return json_response(self->request_, http::status::ok, body);
          } catch (std::exception const& e) {
            log(ERROR, "Proxy: /status failed: {}", e.what());
            return json_response(
                self->request_, http::status::internal_server_error, {{"error", e.what()}});
          }
        }();
        asio::post(self->stream_.get_executor(), [self, response = std::move(response)]() mutable {
          self->write(std::move(response));
        });
      }).detach();
      return;
    }

    write(json_response(request_, http::status::not_found, {{"error", "not found"}}));
  }

  void write(Response res) {
    response_ = std::move(res);
    http::async_write(
        stream_, response_, [self = shared_from_this()](beast::error_code ec, size_t) {
          if (ec || !self->response_.keep_alive())
            return self->close();
          self->read();
        });
  }

  void close() {
    beast::error_code ec;
    stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
  }

  beast::tcp_stream  stream_;
  beast::flat_buffer buffer_;
  Request            request_;
  Response           response_;
  weak_ptr<Proxy>    proxy_;
};

}  // namespace

AdminServer::AdminServer(asio::io_context& io, weak_ptr<Proxy> proxy)
    : io_(io), proxy_(std::move(proxy)), acceptor_(io) {}

Result<void> AdminServer::start(asio::ip::address const& address, uint16_t port) {
  beast::error_code ec;
  acceptor_.open(address.is_v6() ? tcp::v6() : tcp::v4(), ec);
  if (!ec)
    acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
  if (!ec)
    acceptor_.bind(tcp::endpoint(address, port), ec);
  if (!ec)
    acceptor_.listen(asio::socket_base::max_listen_connections, ec);
  if (ec)
    return elog(ec, "Proxy: unable to listen for the admin interface on port {}", port);
  port_ = acceptor_.local_endpoint().port();
  accept();
  log(INFO, "Proxy: admin interface on port {}", port_);
  return ok();
}

void AdminServer::stop() {
  beast::error_code ec;
  acceptor_.close(ec);
}

void AdminServer::accept() {
  // Each session on a strand of its own.
  acceptor_.async_accept(asio::make_strand(io_),
                         [self = shared_from_this()](beast::error_code ec, tcp::socket s) {
                           if (ec == asio::error::operation_aborted)
                             return;
                           if (!ec)
                             make_shared<Session>(std::move(s), self->proxy_)->start();
                           self->accept();
                         });
}
