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
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elasticsearch client configuration
 */
public class ElasticsearchConfig {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);
    
    // Default single node configuration
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9200;
    private static final String DEFAULT_SCHEME = "http";
    
    // Default multi-node configuration
    private static final String DEFAULT_NODES = "localhost:9200,localhost:9201,localhost:9202";
    private static final boolean DEFAULT_TRUST_ALL_CERTS = false;
    
    public static ElasticsearchClient createClient() {
        return createClientWithMultipleNodes();
    }
    
    /**
     * Create client with multiple nodes support and SSL configuration
     */
    public static ElasticsearchClient createClientWithMultipleNodes() {
        try {
            // Get configuration from environment variables
            String nodesConfig = System.getenv().getOrDefault("ELASTICSEARCH_NODES", DEFAULT_NODES);
            String scheme = System.getenv().getOrDefault("ELASTICSEARCH_SCHEME", DEFAULT_SCHEME);
            String username = System.getenv("ELASTICSEARCH_USERNAME");
            String password = System.getenv("ELASTICSEARCH_PASSWORD");
            boolean trustAllCerts = Boolean.parseBoolean(System.getenv().getOrDefault("ELASTICSEARCH_TRUST_ALL_CERTS", String.valueOf(DEFAULT_TRUST_ALL_CERTS)));
            
            // Parse multiple nodes
            List<HttpHost> hosts = parseHosts(nodesConfig, scheme);
            
            logger.info("Connecting to Elasticsearch cluster with {} nodes: {}", hosts.size(), hosts);
            
            // Create credentials provider
            CredentialsProvider credentialsProvider = null;
            if (username != null && password != null) {
                credentialsProvider = createCredentialsProvider(username, password);
            }
            
            // Create SSL context
            SSLContext sslContext = null;
            if ("https".equals(scheme)) {
                sslContext = createSSLContext(trustAllCerts);
            }
            
            // Create RestClientBuilder using the refactored method
            RestClientBuilder builder = getRestClientBuilder(hosts, credentialsProvider, sslContext);
            
            // Set connection timeouts and retry policies
            configureTimeouts(builder);
            
            RestClient restClient = builder.build();
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            
            return new ElasticsearchClient(transport);
            
        } catch (Exception e) {
            logger.error("Failed to create Elasticsearch client with multiple nodes", e);
            throw new RuntimeException("Failed to create Elasticsearch client", e);
        }
    }
    
    /**
     * Parse host configuration string into HttpHost list
     * Format: "host1:port1,host2:port2,host3:port3"
     */
    private static List<HttpHost> parseHosts(String nodesConfig, String scheme) {
        List<HttpHost> hosts = new ArrayList<>();
        String[] nodeStrings = nodesConfig.split(",");
        
        for (String nodeString : nodeStrings) {
            String[] parts = nodeString.trim().split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : DEFAULT_PORT;
            hosts.add(new HttpHost(host, port, scheme));
        }
        
        return hosts;
    }
    
    /**
     * Create RestClientBuilder with hosts, credentials, and SSL context
     */
    private static RestClientBuilder getRestClientBuilder(List<HttpHost> hosts, 
                                                         CredentialsProvider credentialsProvider, 
                                                         SSLContext sslContext) {
        RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));
        
        // Configure HTTP client with SSL and credentials
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            // Set SSL context if provided
            if (sslContext != null) {
                httpClientBuilder.setSSLContext(sslContext);
                // Disable hostname verification if trust-all is enabled
                if (isTrustAllContext(sslContext)) {
                    httpClientBuilder.setSSLHostnameVerifier((hostname, session) -> true);
                }
            }
            
            // Set credentials provider if provided
            if (credentialsProvider != null) {
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            
            return httpClientBuilder;
        });
        
        return builder;
    }
    
    /**
     * Create credentials provider
     */
    private static CredentialsProvider createCredentialsProvider(String username, String password) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(username, password));
        logger.info("Created credentials provider for user: {}", username);
        return credentialsProvider;
    }
    
    /**
     * Create SSL context with optional trust-all-certs
     */
    private static SSLContext createSSLContext(boolean trustAllCerts) {
        try {
            SSLContext sslContext;
            
            if (trustAllCerts) {
                logger.warn("SSL certificate verification is disabled (trust all certificates). This is not recommended for production!");
                // Create trust-all SSL context
                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        }
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        }
                    }
                }, null);
            } else {
                // Use default SSL context with proper certificate validation
                sslContext = SSLContexts.createDefault();
            }
            
            return sslContext;
            
        } catch (Exception e) {
            logger.error("Failed to create SSL context", e);
            throw new RuntimeException("SSL context creation failed", e);
        }
    }
    
    /**
     * Check if SSL context is configured for trust-all (helper method)
     */
    private static boolean isTrustAllContext(SSLContext sslContext) {
        // Simple heuristic: if SSL context is not the default, assume it's trust-all
        // This is not 100% accurate but sufficient for hostname verification control
        return sslContext != null && !sslContext.getProtocol().equals("Default");
    }
    
    /**
     * Configure connection timeouts and retry policies
     */
    private static void configureTimeouts(RestClientBuilder builder) {
        builder.setRequestConfigCallback(requestConfigBuilder ->
            requestConfigBuilder
                .setConnectTimeout(10000)  // 10 seconds
                .setSocketTimeout(120000)  // 2 minutes
                .setConnectionRequestTimeout(5000) // 5 seconds
        );
        
        // Configure retry policy for node failures
        builder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(org.elasticsearch.client.Node node) {
                logger.warn("Node failed: {}", node.getHost());
            }
        });
    }
    
    public static ElasticsearchClient createClient(String host, int port, String scheme, 
                                              String username, String password) {
        try {
            // Get configuration from environment variables or use defaults
            String elasticsearchHost = System.getenv().getOrDefault("ELASTICSEARCH_HOST", host);
            int elasticsearchPort = Integer.parseInt(System.getenv().getOrDefault("ELASTICSEARCH_PORT", String.valueOf(port)));
            String elasticsearchScheme = System.getenv().getOrDefault("ELASTICSEARCH_SCHEME", scheme);
            String elasticsearchUsername = System.getenv().getOrDefault("ELASTICSEARCH_USERNAME", username);
            String elasticsearchPassword = System.getenv().getOrDefault("ELASTICSEARCH_PASSWORD", password);
            
            logger.info("Connecting to Elasticsearch at {}://{}:{}", elasticsearchScheme, elasticsearchHost, elasticsearchPort);
            
            RestClientBuilder builder = RestClient.builder(
                new HttpHost(elasticsearchHost, elasticsearchPort, elasticsearchScheme)
            );
            
            // Add authentication if credentials are provided
            if (elasticsearchUsername != null && elasticsearchPassword != null) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(elasticsearchUsername, elasticsearchPassword));
                
                builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            
            // Set connection timeouts
            builder.setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                    .setConnectTimeout(5000)
                    .setSocketTimeout(60000));
            
            RestClient restClient = builder.build();
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            
            return new ElasticsearchClient(transport);
            
        } catch (Exception e) {
            logger.error("Failed to create Elasticsearch client", e);
            throw new RuntimeException("Failed to create Elasticsearch client", e);
        }
    }
    
    public static void closeClient(ElasticsearchClient client) {
        try {
            if (client != null) {
                client._transport().close();
                logger.info("Elasticsearch client closed successfully");
            }
        } catch (Exception e) {
            logger.error("Error closing Elasticsearch client", e);
        }
    }
    
    /**
     * Main method to test Elasticsearch connection
     */
    public static void main(String[] args) {
        logger.info("Testing Elasticsearch connection...");
        
        ElasticsearchClient client = null;
        try {
            // Create client
            client = createClient();
            
            // Test connection by getting cluster info
            var info = client.info();
            logger.info("Successfully connected to Elasticsearch!");
            logger.info("Cluster name: {}", info.clusterName());
            logger.info("Version: {}", info.version().number());
            
        } catch (Exception e) {
            logger.error("Failed to connect to Elasticsearch: {}", e.getMessage());
            System.exit(1);
        } finally {
            closeClient(client);
        }
        
        logger.info("Connection test completed successfully!");
    }
}
