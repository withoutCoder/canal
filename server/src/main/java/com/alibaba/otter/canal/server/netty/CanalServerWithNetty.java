package com.alibaba.otter.canal.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;
import com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.server.netty.handler.SessionHandler;

/**
 * 基于netty网络服务的server实现
 * 
 * @author jianghang 2012-7-12 下午01:34:49
 * @version 1.0.0
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {

    private CanalServerWithEmbedded embeddedServer;      // 嵌入式server
    private String                  ip;
    private int                     port;
    private Channel                 serverChannel = null;
    private ServerBootstrap         bootstrap     = null;
    private ChannelGroup            childGroups   = null; // socket channel
                                                          // container, used to
                                                          // close sockets
                                                          // explicitly.

    private static class SingletonHolder {

        private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
    }

    private CanalServerWithNetty(){
        this.embeddedServer = CanalServerWithEmbedded.instance();
        this.childGroups = new DefaultChannelGroup();
    }

    public static CanalServerWithNetty instance() {
        return SingletonHolder.CANAL_SERVER_WITH_NETTY;
    }

    public void start() {
        // 1. 超类 start
        super.start();
        // 2. 先启动嵌入式 canal 服务
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }
        // 3.初始化 bootstrap
        // 参数 NioServerSocketChannelFactory 也是 Netty 的 API，接受2个线程池参数，第一个线程池是 Accept 线程池，第二个线程池是 worker 线程池，Accept 线程池接收到 client 连接请求后，会将代表 client 的对象转发给 worker 线程池处理。
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        // 4.tcp keep alive
        bootstrap.setOption("child.keepAlive", true);
        // 5. 禁用 tcp Nagle 算法，降低延迟
        bootstrap.setOption("child.tcpNoDelay", true);
        // 6. 构造对应的pipeline
        bootstrap.setPipelineFactory(() -> {
            // 设置 pipelines 处理器，按照顺序处理接收到的客户端请求
            ChannelPipeline pipelines = Channels.pipeline();
            // 处理编码、解码、解析网络传入二进制流
            pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
            // support to maintain child socket channel.
            pipelines.addLast(HandshakeInitializationHandler.class.getName(), new HandshakeInitializationHandler(childGroups));
            // 身份验证
            pipelines.addLast(ClientAuthenticationHandler.class.getName(), new ClientAuthenticationHandler(embeddedServer));
            // 设置 SessionHandler 用于处理客户端请求
            SessionHandler sessionHandler = new SessionHandler(embeddedServer);
            pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
            return pipelines;
        });
        // 7. 启动 netty 服务器
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
    }

    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

}
