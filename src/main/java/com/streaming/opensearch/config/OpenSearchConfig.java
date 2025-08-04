package com.streaming.opensearch.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenSearch client configuration
 */
public class OpenSearchConfig {
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConfig.class);
    
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9200;
    private static final String DEFAULT_SCHEME = "http";
    
    public static OpenSearchClient createClient() {
        return createClient(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SCHEME, null, null);
    }
    
    public static OpenSearchClient createClient(String host, int port, String scheme, 
                                              String username, String password) {
        try {
            // Get configuration from environment variables or use defaults
            String opensearchHost = System.getenv().getOrDefault("OPENSEARCH_HOST", host);
            int opensearchPort = Integer.parseInt(System.getenv().getOrDefault("OPENSEARCH_PORT", String.valueOf(port)));
            String opensearchScheme = System.getenv().getOrDefault("OPENSEARCH_SCHEME", scheme);
            String opensearchUsername = System.getenv().getOrDefault("OPENSEARCH_USERNAME", username);
            String opensearchPassword = System.getenv().getOrDefault("OPENSEARCH_PASSWORD", password);
            
            logger.info("Connecting to OpenSearch at {}://{}:{}", opensearchScheme, opensearchHost, opensearchPort);
            
            RestClientBuilder builder = RestClient.builder(
                new HttpHost(opensearchHost, opensearchPort, opensearchScheme)
            );
            
            // Add authentication if credentials are provided
            if (opensearchUsername != null && opensearchPassword != null) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(opensearchUsername, opensearchPassword));
                
                builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            
            // Set connection timeouts
            builder.setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                    .setConnectTimeout(5000)
                    .setSocketTimeout(60000));
            
            RestClient restClient = builder.build();
            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            
            return new OpenSearchClient(transport);
            
        } catch (Exception e) {
            logger.error("Failed to create OpenSearch client", e);
            throw new RuntimeException("Failed to create OpenSearch client", e);
        }
    }
    
    public static void closeClient(OpenSearchClient client) {
        try {
            if (client != null) {
                client._transport().close();
                logger.info("OpenSearch client closed successfully");
            }
        } catch (Exception e) {
            logger.error("Error closing OpenSearch client", e);
        }
    }
}
