#pragma once

#include "src/net/connection.h"
#include "src/net/host_address.h"

/// Connect to |host| and send the identity |self| (at most 63 characters). See server.h for the
/// handshake.
Result<socket_type> connect(boost::asio::io_context& io_context,
                            string const&            self,
                            HostAddress const&       host);

void async_connect(boost::asio::io_context&                 io_context,
                   string const&                            self,
                   HostAddress const&                       host,
                   std::function<void(Result<socket_type>)> handler);
