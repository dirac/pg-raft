#pragma once

#include "src/net/connection.h"
#include "src/net/host_address.h"

Result<socket_type> connect(boost::asio::io_context& io_context,
                            string const&            self,
                            HostAddress              host);

void async_connect(boost::asio::io_context&                 io_context,
                   string const&                            self,
                   HostAddress                              host,
                   std::function<void(Result<socket_type>)> cb);
