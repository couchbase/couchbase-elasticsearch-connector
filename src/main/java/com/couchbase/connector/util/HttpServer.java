/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.util;

import com.couchbase.client.dcp.deps.io.netty.bootstrap.ServerBootstrap;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.dcp.deps.io.netty.channel.Channel;
import com.couchbase.client.dcp.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.dcp.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.dcp.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.dcp.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.dcp.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.dcp.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.dcp.deps.io.netty.channel.socket.nio.NioServerSocketChannel;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.DefaultFullHttpResponse;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpContentCompressor;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpObject;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpServerCodec;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.dcp.deps.io.netty.handler.codec.http.QueryStringDecoder;
import com.couchbase.client.dcp.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.dcp.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.connector.VersionHelper;
import com.couchbase.connector.cluster.Membership;
import com.couchbase.connector.elasticsearch.Metrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public class HttpServer implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);

  private final int httpPort;
  private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
  private final ServerBootstrap bootstrap = new ServerBootstrap()
      .group(eventLoopGroup)
      .handler(new LoggingHandler(LogLevel.DEBUG))
      .childHandler(new HttpServerInitializer())
      .channel(NioServerSocketChannel.class);

  private boolean started;
  private Channel serverChannel;
  private Membership membership;

  public HttpServer(int httpPort, Membership membership) {
    this.httpPort = httpPort;
    this.membership = requireNonNull(membership);
  }

  public synchronized void start() throws IOException {
    if (httpPort < 0) {
      LOGGER.debug("HTTP server disabled");
      return;
    }

    if (started) {
      throw new IllegalStateException("already started");
    }

    try {
      serverChannel = bootstrap.bind(httpPort).sync().channel();

    } catch (Exception e) {
      eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      LOGGER.error("Failed to bind HTTP server to port {}; {}", httpPort, e.getMessage());
      // the code in the 'try' block might sneakily throw an IOException even though it isn't declared :-/
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    }

    started = true;

    LOGGER.info("HTTP server listening at http://localhost:{}", getBoundPort());
  }

  public int getConfiguredPort() {
    return httpPort;
  }

  public synchronized int getBoundPort() {
    checkState(started, "not started");
    return ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

  public synchronized void close() throws IOException {
    if (!started) {
      return;
    }

    LOGGER.info("HTTP server shutting down...");
    if (serverChannel != null) {
      try {
        serverChannel.close().await();
      } catch (Exception e) {
        LOGGER.warn("Failed to close HTTP server channel", e);
      }
    }
    try {
      eventLoopGroup.shutdownGracefully().await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    started = false;
    LOGGER.info("HTTP server shutdown complete.");
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    try (HttpServer server = new HttpServer(31415, Membership.of(1, 2))) {
      server.start();
      DAYS.sleep(1);
    }
  }

  public class HttpServerInitializer extends ChannelInitializer<Channel> {
    private final int MAX_REQUEST_CONTENT_LENGTH = (int) ByteSizeUnit.MB.toBytes(1);

    @Override
    protected void initChannel(Channel channel) throws Exception {
      channel.pipeline()
          .addLast(new HttpServerCodec())
          .addLast(new HttpObjectAggregator(MAX_REQUEST_CONTENT_LENGTH, true))
          .addLast(new HttpContentCompressor())
          .addLast(new HttpServerHandler());
    }
  }

  private static final ObjectMapper mapper = new ObjectMapper();

  public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
      final HttpRequest request = (HttpRequest) msg;
      LOGGER.debug("HTTP request: {} ", request);

      final QueryStringDecoder decoder = new QueryStringDecoder(request.uri());

      final HttpResponseStatus status;
      final ByteBuf content;
      final String contentType;

      switch (decoder.path()) {
        case "/":
          contentType = "text/html;charset=UTF-8";
          status = HttpResponseStatus.OK;
          final String html = "<h2>Couchbase Elasticsearch Connector</h2>" +
              "Version " + VersionHelper.getVersionString() +
              "<p>" +
              "<a href=\"info\">Connector info</a> (version, membership, etc.)" +
              "<p>" +
              "<a href=\"metrics/prometheus\">Metrics (Prometheus)</a>" +
              "<p>" +
              "<a href=\"metrics/dropwizard?pretty\">Metrics (Dropwizard)</a>";
          content = Unpooled.wrappedBuffer(html.getBytes(UTF_8));
          break;

        case "/info":
          var info = Map.of(
              "version", VersionHelper.getVersion(),
              "commit", VersionHelper.getGitInfo().orElse(""),
              "membership", membership.toString()
          );
          content = Unpooled.wrappedBuffer(mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(info));
          contentType = "application/json";
          status = HttpResponseStatus.OK;
          break;

        case "/metrics": // default to dropwizard for now. Maybe add a switch for the default format?
        case "/metrics/dropwizard":
          final boolean pretty = getBoolean(decoder, "pretty", false);
          final ObjectWriter w = pretty ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer();
          content = Unpooled.wrappedBuffer(w.writeValueAsBytes(Metrics.toJsonNode()));
          contentType = "application/json";
          status = HttpResponseStatus.OK;
          break;

        case "/metrics/prometheus":
          content = Unpooled.wrappedBuffer(Metrics.toPrometheusExpositionFormat().getBytes(UTF_8));
          contentType = "text/plain; version=0.0.4";
          status = HttpResponseStatus.OK;
          break;

        default:
          content = Unpooled.wrappedBuffer(mapper.writeValueAsBytes("path '" + decoder.path() + "' not found"));
          contentType = "application/json";
          status = HttpResponseStatus.NOT_FOUND;
          break;
      }

      final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
      ctx.writeAndFlush(response)
          .addListener(ChannelFutureListener.CLOSE); // ignore keepalive -- let's just play it safe
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      ctx.flush();
    }
  }

  private static boolean getBoolean(QueryStringDecoder decoder, String paramName, boolean defaultValue) {
    boolean value = decoder.parameters().containsKey(paramName) || defaultValue;
    for (String s : decoder.parameters().getOrDefault(paramName, Collections.emptyList())) {
      value = !s.equals("false");
    }
    return value;
  }
}
