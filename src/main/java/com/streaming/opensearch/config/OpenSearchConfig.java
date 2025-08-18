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

/**
 * Elasticsearch client configuration
 */
public class ElasticsearchConfig {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);
    
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9200;
    private static final String DEFAULT_SCHEME = "http";
    
    public static ElasticsearchClient createClient() {
        return createClient(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SCHEME, null, null);
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
