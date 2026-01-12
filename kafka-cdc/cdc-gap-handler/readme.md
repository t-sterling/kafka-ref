
### Kafka Streams:  

- Service is single threaded, both GapHandlerProcessor & FillEventProcessor access _gap-state-store_.
  - _CDC-INPUT_ & _CDC-FILL-EVENT_ co-partitioned, keyed by CDC record id.
- Hot path is _CDC-INPUT_ > _GapHandlerProcessor_ > _CDC-OUTPUT_
- When _GapHandlerProcessor_ detects gap event:
  - Adds _GapEventState_ to _gap-state-store_, which is a buffer + meta-data, keyed by CDC record-id.
  - Subsequent CDC for the same record-id are buffered in _GapEventState_.
  - _FillCommand_ published for async processing.
- On _FillEvent_ the _FillEventProcessor_:
  - If the _FillEvent_ contains an error, publishes it to _ERROR_
  - If there is no corresponding buffer in the _gap_state_store_:
    - if the _FillEvent_ has a **force-flag** then publish to _CDC-OUTPUT_
    - else, drop the event (multiple deliveries of the event will result the same - idempotent)
  - Deletes GapEventState from the store.
- Periodically, _FillEventProcessor_ must also fail and pending _FillEvents_ if:
  - Too many subsequent CDC events have been buffered
  - Too much time has passed
  - [TODO: Punctuators for this]

```mermaid

flowchart LR
%% Sub-topology: 0

%% Topics (sources & sinks)
  CDC_INPUT[(Topic: CDC-INPUT)]
  CDC_FILL_EVENT[(Topic: CDC-FILL-EVENT)]
  CDC_OUTPUT[(Topic: CDC-OUTPUT)]
  CDC_FILL_COMMAND[(Topic: CDC-FILL-COMMAND)]
  ERROR[(Topic: ERROR)]

%% Processors
GapHandler[GapHandlerProcessor\nstore: gap-state-store]
FillEventProc[FillEventProcessor\nstore: gap-state-store]

%% Edges
CDC_INPUT --> GapHandler
CDC_FILL_EVENT --> FillEventProc

GapHandler --> CDC_FILL_COMMAND
GapHandler --> CDC_OUTPUT
FillEventProc --> CDC_OUTPUT
FillEventProc --> ERROR


%% Styles
classDef cmdtopic fill:#DDE6F3,stroke:#5F78A6,stroke-width:1px,color:#888888
classDef evttopic fill:#FFF6E3,stroke:#E0C27A,stroke-width:1px,color:#888888
classDef processor fill:#CDCDCD,stroke:#888888,stroke-width:1px,color:#888888

class CDC_FILL_COMMAND cmdtopic
class CDC_FILL_EVENT evttopic
class GapHandler,CdcFwd,FillCmdFwd,FillEventProc processor
class CDC_INPUT,CDC_OUTPUT,ERROR processor
```

Async 'FillService'
- Simple Kafka Consumer and Publisher
- Is a work queue so:
  - Turn auto-commit off, use Spring _Acknowledge_ param **after** FillEvent is sent
  - Non-transient errors are published as _FillEvents_ with failure status, to be handled by _FillEventProcessor_
  - On a transient error (e.g: Source system not available)
    - Spring _MessageListenerContainer_ can pause the Kafka subscription without causing rebalance.
    - Retry strategy until it works or crash after maxOutage time.
      - ack will not be sent on retry, _FillCommand_ will be processed twice, but Kafka is at-least-once anyway.
      - The _FillEventProcessor_ is idempotent anyway

```mermaid
flowchart LR
    %% Async Fill Route with Color Coding

    FillCmdTopic[(Topic: CDC-FILL-COMMAND)]
    FillEventTopic[(Topic: CDC-FILL-EVENT)]

    subgraph FillService["Async Fill Service"]
        direction TB
        Consume[Consume FillCommand]
        Decide[Query \nSource System]
        Produce[Produce FillEvent]

        Consume --> Decide
        Decide --> Produce
    end

    FillCmdTopic --> Consume
    Produce --> FillEventTopic

    %% Styles
    classDef cmdtopic fill:#DDE6F3,stroke:#5F78A6,stroke-width:1px,color:#888888
    classDef evttopic fill:#FFF6E3,stroke:#E0C27A,stroke-width:1px,color:#888888
    classDef service fill:#CDCDCD,stroke:#888888,stroke-width:1px,color:#888888
    classDef decision fill:#CDCDCD,stroke:#888888,stroke-width:1px,color:#888888
        
    class FillCmdTopic cmdtopic
    class FillEventTopic evttopic
    class Consume,Produce service
    class Decide decision

```


