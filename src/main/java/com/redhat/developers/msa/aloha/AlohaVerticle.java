/**
 * JBoss, Home of Professional Open Source
 * Copyright 2016, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.developers.msa.aloha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hawkular.apm.client.opentracing.APMTracer;

import com.github.kennedyoliveira.hystrix.contrib.vertx.metricsstream.EventMetricsStreamHandler;

import feign.Logger;
import feign.Logger.Level;
import feign.hystrix.HystrixFeign;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class AlohaVerticle extends AbstractVerticle {

	private Tracer tracer = new APMTracer();

	@Override
    public void start() throws Exception {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().handler(CorsHandler.create("*")
            .allowedMethod(HttpMethod.GET)
            .allowedHeader("Content-Type"));

        // Aloha EndPoint
        router.get("/api/aloha").handler(ctx -> {
        	SpanContext spanCtx = tracer.extract(Format.Builtin.TEXT_MAP,
                    new HttpHeadersExtractAdapter(ctx.request().headers()));

            try (Span serverSpan = tracer.buildSpan("GET")
                    .asChildOf(spanCtx)
                    .withTag("http.url", "/api/ahola")
                    .withTag("service", "Aloha")
                    .start()) {
            	ctx.response().end(aloha());
            }
        });

        // Aloha Chained Endpoint
        router.get("/api/aloha-chaining").handler(ctx -> {
        	SpanContext spanCtx = tracer.extract(Format.Builtin.TEXT_MAP,
                    new HttpHeadersExtractAdapter(ctx.request().headers()));

            Span serverSpan = tracer.buildSpan("GET")
                    .asChildOf(spanCtx)
                    .withTag("http.url", "/api/ahola-chaining")
                    .withTag("service", "Aloha")
                    .start();

            alohaChaining(ctx, serverSpan, (list) -> {
            	ctx.response()
            		.putHeader("Content-Type", "application/json")
	                .end(Json.encode(list));
            	serverSpan.finish();
            });
        });

        // Health Check
        router.get("/api/health").handler(ctx -> ctx.response().end("I'm ok"));

        // Hysrix Stream Endpoint
        router.get(EventMetricsStreamHandler.DEFAULT_HYSTRIX_PREFIX).handler(EventMetricsStreamHandler.createHandler());

        // Static content
        router.route("/*").handler(StaticHandler.create());

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
        System.out.println("Service running at 0.0.0.0:8080");
    }

    private String aloha() {
        String hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");
        return String.format("Aloha mai %s", hostname);
    }

    private void alohaChaining(RoutingContext context, Span parentSpan, Handler<List<String>> resultHandler) {
        vertx.<String> executeBlocking(
            // Invoke the service in a worker thread, as it's blocking.
            future -> {
            	String result = null;
                try (Span clientSpan = tracer.buildSpan("CallBonjourChaining")
                        .asChildOf(parentSpan)
                        .start()) {
                    Map<String,Object> h = new HashMap<>();
                    tracer.inject(clientSpan.context(), Format.Builtin.TEXT_MAP,
                            new HttpHeadersInjectAdapter(h));
	            	result = getNextService(context).bonjour(h);
                }
            	future.complete(result);
            },
            ar -> {
                // Back to the event loop
                // result cannot be null, hystrix would have called the fallback.
                String result = ar.result();
                List<String> greetings = new ArrayList<>();
                greetings.add(aloha());
                greetings.add(result);
                resultHandler.handle(greetings);
            });
    }

    /**
     * This is were the "magic" happens: it creates a Feign, which is a proxy interface for remote calling a REST endpoint with
     * Hystrix fallback support.
     *
     * @return The feign pointing to the service URL and with Hystrix fallback.
     */
    private BonjourService getNextService(RoutingContext context) {
        final String serviceName = "bonjour";
        final String url = String.format("http://%s:8080/", serviceName);
        BonjourService fallback = (headers) -> "Bonjour response (fallback)";
        return HystrixFeign.builder()
             .logger(new Logger.ErrorLogger()).logLevel(Level.BASIC)
            .target(BonjourService.class, url, fallback);
    }

    public final class HttpHeadersExtractAdapter implements TextMap {
        private final Map<String,String> map;

        public HttpHeadersExtractAdapter(final MultiMap multiValuedMap) {
        	// Convert to single valued map
            this.map = new HashMap<>();
            multiValuedMap.forEach(entry -> map.put(entry.getKey(), entry.getKey()));
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return map.entrySet().iterator();
        }

        @Override
        public void put(String key, String value) {
            throw new UnsupportedOperationException("TextMapInjectAdapter should only be used with Tracer.extract()");
        }
    }

    public final class HttpHeadersInjectAdapter implements TextMap {
        private final Map<String,Object> map;

        public HttpHeadersInjectAdapter(final Map<String,Object> map) {
            this.map = map;
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            throw new UnsupportedOperationException("TextMapInjectAdapter should only be used with Tracer.inject()");
        }

        @Override
        public void put(String key, String value) {
            this.map.put(key, value);
        }
    }
}
