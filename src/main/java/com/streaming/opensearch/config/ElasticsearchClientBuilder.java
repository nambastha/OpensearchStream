package com.streaming.opensearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Elasticsearch client builder with SSL trust-all-certs support
 * Provides a fluent API for creating Elasticsearch clients with various configurations
 */
public class ElasticsearchClientBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClientBuilder.class);
    
    private List<HttpHost> hosts = new ArrayList<>();
    private String username;
    private String password;
    private boolean trustAllCerts = false;
    private String scheme = "http";
    private int connectTimeout = 10000; // 10 seconds
    private int socketTimeout = 120000; // 2 minutes
    private int connectionRequestTimeout = 5000; // 5 seconds
    
    public ElasticsearchClientBuilder() {
        // Default constructor
    }
    
    /**
     * Add a single host
     */
    public ElasticsearchClientBuilder addHost(String hostname, int port) {
        hosts.add(new HttpHost(hostname, port, scheme));
        return this;
    }
    
    /**
     * Add multiple hosts
     */
    public ElasticsearchClientBuilder addHosts(String... hostPorts) {
        for (String hostPort : hostPorts) {
            String[] parts = hostPort.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9200;
            hosts.add(new HttpHost(host, port, scheme));
        }
        return this;
    }
    
    /**
     * Set authentication credentials
     */
    public ElasticsearchClientBuilder withCredentials(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }
    
    /**
     * Enable HTTPS scheme
     */
    public ElasticsearchClientBuilder withHttps() {
        this.scheme = "https";
        // Update existing hosts to use HTTPS
        updateHostsScheme();
        return this;
    }
    
    /**
     * Enable trust all certificates (disable SSL verification)
     * WARNING: This should only be used for development/testing
     */
    public ElasticsearchClientBuilder withTrustAllCerts() {
        this.trustAllCerts = true;
        return this;
    }
    
    /**
     * Set connection timeout
     */
    public ElasticsearchClientBuilder withConnectTimeout(int timeoutMs) {
        this.connectTimeout = timeoutMs;
        return this;
    }
    
    /**
     * Set socket timeout
     */
    public ElasticsearchClientBuilder withSocketTimeout(int timeoutMs) {
        this.socketTimeout = timeoutMs;
        return this;
    }
    
    /**
     * Set connection request timeout
     */
    public ElasticsearchClientBuilder withConnectionRequestTimeout(int timeoutMs) {
        this.connectionRequestTimeout = timeoutMs;
        return this;
    }
    
    /**
     * Create the Elasticsearch client with configured settings
     */
    public ElasticsearchClient build() {
        if (hosts.isEmpty()) {
            // Default to localhost if no hosts specified
            hosts.add(new HttpHost("localhost", 9200, scheme));
        }
        
        logger.info("Building Elasticsearch client with {} hosts: {}", hosts.size(), hosts);
        
        try {
            RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));
            
            // Configure SSL if using HTTPS
            if ("https".equals(scheme)) {
                configureSSL(builder);
            }
            
            // Configure authentication
            if (username != null && password != null) {
                configureAuthentication(builder);
            }
            
            // Configure timeouts
            configureTimeouts(builder);
            
            // Add failure listener for monitoring
            configureFailureListener(builder);
            
            RestClient restClient = builder.build();
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            
            ElasticsearchClient client = new ElasticsearchClient(transport);
            
            logger.info("Successfully created Elasticsearch client");
            return client;
            
        } catch (Exception e) {
            logger.error("Failed to build Elasticsearch client", e);
            throw new RuntimeException("Failed to build Elasticsearch client", e);
        }
    }
    
    /**
     * Create a trust-all SSL context
     */
    private SSLContext createTrustAllSSLContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                    
                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        // Trust all client certificates
                    }
                    
                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        // Trust all server certificates
                    }
                }
            }, new java.security.SecureRandom());
            
            return sslContext;
        } catch (Exception e) {
            logger.error("Failed to create trust-all SSL context", e);
            throw new RuntimeException("SSL context creation failed", e);
        }
    }
    
    /**
     * Configure SSL settings
     */
    private void configureSSL(RestClientBuilder builder) {
        if (trustAllCerts) {
            logger.warn("SSL certificate verification is DISABLED! This is not recommended for production environments.");
            
            SSLContext trustAllSSLContext = createTrustAllSSLContext();
            
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setSSLContext(trustAllSSLContext);
                // Disable hostname verification
                httpClientBuilder.setSSLHostnameVerifier((hostname, session) -> {
                    logger.debug("Accepting SSL connection to: {}", hostname);
                    return true;
                });
                return httpClientBuilder;
            });
        } else {
            logger.info("Using default SSL context with certificate verification");
            // Use default SSL context - certificates will be verified
        }
    }
    
    /**
     * Configure authentication
     */
    private void configureAuthentication(RestClientBuilder builder) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(username, password));
        
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });
        
        logger.info("Configured basic authentication for user: {}", username);
    }
    
    /**
     * Configure connection timeouts
     */
    private void configureTimeouts(RestClientBuilder builder) {
        builder.setRequestConfigCallback(requestConfigBuilder ->
            requestConfigBuilder
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setConnectionRequestTimeout(connectionRequestTimeout)
        );
        
        logger.debug("Configured timeouts - Connect: {}ms, Socket: {}ms, Request: {}ms", 
            connectTimeout, socketTimeout, connectionRequestTimeout);
    }
    
    /**
     * Configure failure listener for node monitoring
     */
    private void configureFailureListener(RestClientBuilder builder) {
        builder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(org.elasticsearch.client.Node node) {
                logger.warn("Elasticsearch node failed: {} - {}", node.getHost(), node.toString());
            }
        });
    }
    
    /**
     * Update scheme for existing hosts
     */
    private void updateHostsScheme() {
        List<HttpHost> updatedHosts = new ArrayList<>();
        for (HttpHost host : hosts) {
            updatedHosts.add(new HttpHost(host.getHostName(), host.getPort(), scheme));
        }
        hosts = updatedHosts;
    }
    
    /**
     * Static factory method for creating a builder
     */
    public static ElasticsearchClientBuilder create() {
        return new ElasticsearchClientBuilder();
    }
    
    /**
     * Static factory method for creating a client with trust-all-certs
     */
    public static ElasticsearchClient createTrustAllClient(String... hostPorts) {
        return ElasticsearchClientBuilder.create()
            .addHosts(hostPorts)
            .withHttps()
            .withTrustAllCerts()
            .build();
    }
    
    /**
     * Static factory method for creating a secure client with credentials
     */
    public static ElasticsearchClient createSecureClient(String username, String password, String... hostPorts) {
        return ElasticsearchClientBuilder.create()
            .addHosts(hostPorts)
            .withHttps()
            .withCredentials(username, password)
            .build();
    }
    
    /**
     * Get current configuration as string (for debugging)
     */
    @Override
    public String toString() {
        return "ElasticsearchClientBuilder{" +
                "hosts=" + hosts +
                ", username='" + username + '\'' +
                ", trustAllCerts=" + trustAllCerts +
                ", scheme='" + scheme + '\'' +
                ", connectTimeout=" + connectTimeout +
                ", socketTimeout=" + socketTimeout +
                ", connectionRequestTimeout=" + connectionRequestTimeout +
                '}';
    }
}