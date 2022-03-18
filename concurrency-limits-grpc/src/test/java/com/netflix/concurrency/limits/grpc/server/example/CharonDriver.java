package com.netflix.concurrency.limits.grpc.server.example;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.grpc.client.CharonConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.client.GrpcClientRequestContext;
import com.netflix.concurrency.limits.grpc.client.GrpcClientLimiterBuilder;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import java.util.Random;

public class CharonDriver {
    public static final Metadata.Key<String> ID_HEADER = Metadata.Key.of("id", Metadata.ASCII_STRING_MARSHALLER);

    private interface Segment {
        long duration();
        long nextDelay();
        String name();
    }
    
    static public Builder newBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        private List<CharonDriver.Segment> segments = new ArrayList<>();
        private int port;
        private long runtimeSeconds;
        private Consumer<Long> latencyAccumulator;
        private String id = "ThisIsCharonDriverID";

        public Builder normal(double mean, double sd, long duration, TimeUnit units) {
            final NormalDistribution distribution = new NormalDistribution(mean, sd);
            return add("normal(" + mean + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder uniform(double lower, double upper, long duration, TimeUnit units) {
            final UniformRealDistribution distribution = new UniformRealDistribution(lower, upper);
            return add("uniform(" + lower + "," + upper + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder exponential(double mean, long duration, TimeUnit units) {
            final ExponentialDistribution distribution = new ExponentialDistribution(mean);
            return add("exponential(" + mean + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder exponentialRps(double rps, long duration, TimeUnit units) {
            return exponential(1000.0 / rps, duration, units);
        }
        
        public Builder slience(long duration, TimeUnit units) {
            return add("slience()", () -> units.toMillis(duration), duration, units);
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder latencyAccumulator(Consumer<Long> consumer) {
            this.latencyAccumulator = consumer;
            return this;
        }
        
        public Builder runtime(long duration, TimeUnit units) {
            this.runtimeSeconds = units.toNanos(duration);
            return this;
        }
        
        public Builder add(String name, Supplier<Long> delaySupplier, long duration, TimeUnit units) {
            segments.add(new Segment() {
                @Override
                public long duration() {
                    return units.toNanos(duration);
                }
    
                @Override
                public long nextDelay() {
                    return delaySupplier.get();
                }
    
                @Override
                public String name() {
                    return name;
                }
            });
            return this;
        }
        
        public CharonDriver build() {
            return new CharonDriver(this);
        }
    }

    private final List<Segment> segments;
    private Channel channel;
    private final long runtime;
    private final Consumer<Long> latencyAccumulator;
    private final AtomicInteger successCounter = new AtomicInteger(0);
    private final AtomicInteger dropCounter = new AtomicInteger(0);

    Metadata metadata = new Metadata();
    private final int port;
    Random rand = new Random(); //instance of random class

    Limiter<GrpcClientRequestContext> limiter = new GrpcClientLimiterBuilder()
            .blockOnLimit(true)
            .build();

    CharonConcurrencyLimitClientInterceptor interceptor = new CharonConcurrencyLimitClientInterceptor(limiter);

    public CharonDriver(Builder builder) {
        this.segments = builder.segments;
        this.runtime = builder.runtimeSeconds;
        this.latencyAccumulator = builder.latencyAccumulator;
        
        this.port = builder.port;
        int tokens = rand.nextInt(10);
        this.metadata.put(ID_HEADER, String.valueOf(tokens));

        // this.channel = ClientInterceptors.intercept(NettyChannelBuilder.forTarget("localhost:" + builder.port)
        //         .usePlaintext(true)
        //         .build(),
        //             MetadataUtils.newAttachHeadersInterceptor(this.metadata));

    }

    public int getAndResetSuccessCount() { return successCounter.getAndSet(0); }
    public int getAndResetDropCount() { return dropCounter.getAndSet(0); }

    public CompletableFuture<Void> runAsync() {
        return CompletableFuture.runAsync(this::run, Executors.newSingleThreadExecutor());
    }

    public void run() {
        long endTime = System.nanoTime() + this.runtime;
        while (true) {
            for (CharonDriver.Segment segment : segments) {
                long segmentEndTime = System.nanoTime() + segment.duration();
                while (true) {
                    long currentTime = System.nanoTime();
                    if (currentTime > endTime) {
                        return;
                    }
                    
                    if (currentTime > segmentEndTime) {
                        break;
                    }
                    
                    long startTime = System.nanoTime();

                    this.metadata.discardAll(ID_HEADER);
                    int tokens = rand.nextInt(10);
                    this.metadata.put(ID_HEADER, String.valueOf(tokens));
        
                    // this.channel.shutdown();
                    
                    this.channel = ClientInterceptors.intercept(
                        ClientInterceptors.intercept(
                            NettyChannelBuilder.forTarget("localhost:" + port)
                                .usePlaintext(true)
                                .build(),
                            MetadataUtils.newAttachHeadersInterceptor(this.metadata)),
                        interceptor);
        
                    System.out.println("Metadata on the Client side: " + this.metadata);

                    Uninterruptibles.sleepUninterruptibly(Math.max(0, segment.nextDelay()), TimeUnit.MILLISECONDS);
                    ClientCalls.asyncUnaryCall(this.channel.newCall(TestServer.METHOD_DESCRIPTOR, CallOptions.DEFAULT.withWaitForReady()), "request",
                            new StreamObserver<String>() {
                                @Override
                                public void onNext(String value) {
                                    // System.out.println(value);
                                }

                                @Override
                                public void onError(Throwable t) {
                                    dropCounter.incrementAndGet();
                                    System.out.println("Dropped RPC.");
                                }

                                @Override
                                public void onCompleted() {
                                    latencyAccumulator.accept(System.nanoTime() - startTime);
                                    successCounter.incrementAndGet();
                                }
                        });
                }
            }
        }
    }
}