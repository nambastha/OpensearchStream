# OpenSearch Event Streaming Architecture with Persistent Offset Management

## High-Level System Architecture

```mermaid
graph TB
    subgraph "Container Environment (Kubernetes/Docker)"
        subgraph "Pod 1"
            C1[Consumer Group A<br/>event-processor-v1]
        end
        subgraph "Pod 2" 
            C2[Consumer Group B<br/>analytics-processor]
        end
        subgraph "Pod 3"
            C3[Consumer Group A<br/>event-processor-v1<br/><i>Replica</i>]
        end
        subgraph "Producer Pod"
            P[Event Producer<br/>Continuous Data Generation]
        end
    end

    subgraph "OpenSearch Cluster"
        subgraph "Data Indices"
            EI[Events Index<br/>- Event Documents<br/>- _seq_no: 1,2,3...<br/>- _primary_term<br/>- processed: false/true]
        end
        subgraph "Offset Management"
            OI[Consumer-Offsets Index<br/>- Consumer Group ID<br/>- Last Processed seq_no<br/>- Primary Term<br/>- Timestamp]
        end
    end

    P -->|Produces Events| EI
    C1 -->|Reads Events WHERE<br/>_seq_no > last_offset| EI
    C2 -->|Reads Events WHERE<br/>_seq_no > last_offset| EI
    C3 -->|Reads Events WHERE<br/>_seq_no > last_offset| EI
    
    C1 -->|Commits Offset<br/>Atomically| OI
    C2 -->|Commits Offset<br/>Atomically| OI
    C3 -->|Commits Offset<br/>Atomically| OI
    
    C1 -.->|On Startup<br/>Load Last Offset| OI
    C2 -.->|On Startup<br/>Load Last Offset| OI
    C3 -.->|On Startup<br/>Load Last Offset| OI

    classDef container fill:#e1f5fe,stroke:#0277bd
    classDef opensearch fill:#f3e5f5,stroke:#7b1fa2
    classDef process fill:#e8f5e8,stroke:#2e7d32
    
    class C1,C2,C3,P container
    class EI,OI opensearch
```

## Detailed Component Architecture

```mermaid
graph TB
    subgraph "ImprovedEventConsumer"
        subgraph "Core Components"
            EC[EventConsumer<br/>Main Processing Loop]
            OM[OffsetManager<br/>Persistent Storage]
            CO[ConsumerOffset<br/>State Model]
        end
        
        subgraph "Processing Flow"
            F1[1. Load Last Offset]
            F2[2. Fetch Unprocessed Events<br/>WHERE _seq_no > last_offset]
            F3[3. Process Event Batch]
            F4[4. Update Event Status<br/>processed = true]
            F5[5. Commit New Offset<br/>Atomic Operation]
        end
    end

    subgraph "OpenSearch Storage"
        subgraph "Events Index"
            ED1[Event Doc 1<br/>_seq_no: 1001<br/>_primary_term: 1<br/>processed: true]
            ED2[Event Doc 2<br/>_seq_no: 1002<br/>_primary_term: 1<br/>processed: true]  
            ED3[Event Doc 3<br/>_seq_no: 1003<br/>_primary_term: 1<br/>processed: false]
        end
        
        subgraph "Consumer-Offsets Index"
            OF1[Group A Offset<br/>lastSeqNo: 1002<br/>lastPrimaryTerm: 1<br/>timestamp: 2024-01-01T10:30:00Z]
            OF2[Group B Offset<br/>lastSeqNo: 1001<br/>lastPrimaryTerm: 1<br/>timestamp: 2024-01-01T10:29:00Z]
        end
    end

    F1 --> OF1
    F1 --> OF2
    F2 --> ED1
    F2 --> ED2
    F2 --> ED3
    F3 --> F4
    F4 --> F5
    F5 --> OF1
    F5 --> OF2

    EC --> F1
    F1 --> F2
    F2 --> F3
    F3 --> F4
    F4 --> F5
    F5 --> F2

    OM --> OF1
    OM --> OF2
    CO --> OM

    classDef component fill:#fff3e0,stroke:#f57c00
    classDef flow fill:#e8f5e8,stroke:#2e7d32
    classDef storage fill:#f3e5f5,stroke:#7b1fa2
    
    class EC,OM,CO component
    class F1,F2,F3,F4,F5 flow
    class ED1,ED2,ED3,OF1,OF2 storage
```

## Container Restart Resilience Flow

```mermaid
sequenceDiagram
    participant K as Kubernetes
    participant C1 as Consumer Pod v1
    participant C2 as Consumer Pod v2
    participant OS as OpenSearch
    participant OI as Offsets Index

    Note over C1, OS: Normal Processing
    C1->>OS: Process events 1-100
    C1->>OI: Commit offset: seq_no=100
    
    Note over K, C1: Pod Restart Event
    K->>C1: Kill Pod (OOMKilled/Evicted)
    C1-->>K: Pod Terminated
    
    K->>C2: Create New Pod
    Note over C2: Pod Startup
    
    C2->>OI: Load last offset for consumer group
    OI->>C2: Return: seq_no=100, primary_term=1
    
    Note over C2, OS: Resume Processing
    C2->>OS: Fetch events WHERE _seq_no > 100
    OS->>C2: Return events 101, 102, 103...
    C2->>OS: Process events 101-150  
    C2->>OI: Commit offset: seq_no=150
    
    Note over C2: No Duplicate Processing!
```

## Comparison: Before vs After

```mermaid
graph TB
    subgraph "BEFORE: Timestamp-Based (Problematic)"
        subgraph "Issues"
            I1[❌ Memory Leaks<br/>HashSet grows indefinitely]
            I2[❌ Data Loss on Restart<br/>Reprocesses from epoch]
            I3[❌ Race Conditions<br/>Concurrent access issues]
            I4[❌ Weak Ordering<br/>Timestamp collisions]
        end
        
        subgraph "Architecture"
            OC[Original Consumer]
            TS[Timestamp Tracking]
            HS[HashSet Cache]
            
            OC --> TS
            OC --> HS
        end
    end

    subgraph "AFTER: Sequence Number-Based (Robust)"
        subgraph "Benefits"
            B1[✅ Constant Memory<br/>No in-memory caching]
            B2[✅ Restart Resilience<br/>Resumes from last offset]
            B3[✅ Atomic Operations<br/>Consistent state]
            B4[✅ Strict Ordering<br/>Sequence number guarantee]
        end
        
        subgraph "Architecture"
            IC[Improved Consumer]
            SNT[Sequence Number Tracking]  
            POS[Persistent Offset Store]
            
            IC --> SNT
            IC --> POS
        end
    end

    classDef problem fill:#ffebee,stroke:#c62828
    classDef solution fill:#e8f5e8,stroke:#2e7d32
    classDef component fill:#fff3e0,stroke:#f57c00
    
    class I1,I2,I3,I4 problem
    class B1,B2,B3,B4 solution
    class OC,IC,TS,SNT,HS,POS component
```

## Deployment Architecture for Organizations

```mermaid
graph TB
    subgraph "Production Environment"
        subgraph "Namespace: data-processing"
            subgraph "Consumer Deployments"
                CD1[Consumer Deployment<br/>Replicas: 3<br/>Group: analytics-v1]
                CD2[Consumer Deployment<br/>Replicas: 2<br/>Group: notifications-v1]  
                CD3[Consumer Deployment<br/>Replicas: 1<br/>Group: audit-v1]
            end
            
            subgraph "Producer Deployment"  
                PD[Producer Deployment<br/>Replicas: 2<br/>Event Generation]
            end
        end
        
        subgraph "ConfigMaps & Secrets"
            CM[ConfigMap<br/>- OpenSearch endpoints<br/>- Index names<br/>- Consumer groups]
            SEC[Secret<br/>- OpenSearch credentials<br/>- SSL certificates]
        end
        
        subgraph "Monitoring"
            MON[Prometheus Metrics<br/>- Events processed<br/>- Offset lag<br/>- Consumer health]
            LOG[Structured Logging<br/>- JSON format<br/>- ELK stack integration]
        end
    end

    subgraph "OpenSearch Cluster"
        subgraph "Data Tier"
            ES1[OpenSearch Node 1<br/>Master + Data]
            ES2[OpenSearch Node 2<br/>Master + Data]
            ES3[OpenSearch Node 3<br/>Data Only]
        end
        
        subgraph "Indices"
            EVI[Events Index<br/>Shards: 5, Replicas: 1]
            OFI[Offsets Index<br/>Shards: 1, Replicas: 1]
        end
    end

    CD1 --> EVI
    CD2 --> EVI  
    CD3 --> EVI
    PD --> EVI
    
    CD1 --> OFI
    CD2 --> OFI
    CD3 --> OFI

    CD1 --> CM
    CD2 --> CM
    CD3 --> CM
    PD --> CM
    
    CD1 --> SEC
    CD2 --> SEC  
    CD3 --> SEC
    PD --> SEC

    CD1 --> MON
    CD2 --> MON
    CD3 --> MON
    PD --> MON

    CD1 --> LOG
    CD2 --> LOG
    CD3 --> LOG
    PD --> LOG

    ES1 --- ES2
    ES2 --- ES3
    ES1 --- ES3

    classDef deployment fill:#e3f2fd,stroke:#1976d2
    classDef storage fill:#f3e5f5,stroke:#7b1fa2  
    classDef config fill:#fff8e1,stroke:#f57c00
    classDef monitoring fill:#e8f5e8,stroke:#388e3c
    
    class CD1,CD2,CD3,PD deployment
    class ES1,ES2,ES3,EVI,OFI storage
    class CM,SEC config
    class MON,LOG monitoring
```

## Business Value Proposition

### Cost Savings
- **Reduced Resource Usage**: No memory leaks = lower memory requirements
- **Eliminated Data Reprocessing**: Saves compute costs from duplicate processing
- **No External Dependencies**: No need for additional databases or storage systems

### Operational Excellence  
- **Zero Downtime Deployments**: Consumers resume seamlessly after restarts
- **Horizontal Scaling**: Multiple consumer groups for different use cases
- **Self-Healing**: Automatic recovery from pod failures

### Risk Mitigation
- **Data Consistency**: Atomic offset commits prevent data loss
- **Audit Trail**: Complete processing history in offset index
- **Container Native**: Designed for modern containerized environments

### Developer Experience
- **Simple Configuration**: Uses existing OpenSearch cluster
- **Clear Monitoring**: Built-in metrics and logging
- **Easy Debugging**: Offset state is queryable and transparent