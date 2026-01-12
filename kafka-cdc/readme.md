### kafka cdc

project for figuring out how to process compensatory events asynchronously


### Interaction Between Services Overview

```mermaid
flowchart TD
    subgraph Producer
        A[EntityRefreshedEventProducer] 
    end
    
    subgraph Kafka
        B[CDC-FILL-COMMAND Topic]
        C[CDC-FILL-EVENT Topic]
    end
    
    subgraph Consumer
        F[CdcFlusherProcessor] 
        G[State Store KeyValueStore]
    end

    A -->|produce EntityRefreshedEvent| B
    B -->|consume by FlusherProcessor| F
    F -->|if meets conditions| G
    G -->|flush CdcEvent| C
```

---

#### **Sequence Diagram**

Alternatively, you could represent it with a sequence diagram to visualize the time-ordered interactions.

### Sequence of Events

```mermaid
sequenceDiagram
    participant Producer as EntityRefreshedEventProducer
    participant Kafka as Kafka Topic (CDC-REFRESH-COMMAND)
    participant Processor as CdcFlusherProcessor
    participant StateStore as KeyValueStore
    participant Consumer as Output Topic (ENTITY-REFRESHED-TOPIC)

    Producer->>Kafka: Produce EntityRefreshedEvent
    Kafka->>Processor: Forward Event to CdcFlusherProcessor
    Processor->>StateStore: Check/Buffer events
    Note over Processor,StateStore: Buffer events in StateStore<br>if conditions are met
    Processor->>Kafka: Produce CdcEvent
```

---

### 3. **Where to Add/Modify in README**

1. **Heading Example**:
   Add a new heading below the existing sections (or replace outdated diagrams):