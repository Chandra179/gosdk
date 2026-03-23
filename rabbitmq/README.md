# rabbitmq

Go SDK module for RabbitMQ with auto-reconnection, channel pooling, OTel tracing, and dead-letter support.

## Component Diagram

```mermaid
graph TB
    subgraph External["External Dependencies"]
        Broker[("RabbitMQ Broker<br/><i>AMQP 0-9-1</i>")]
        OTel["OpenTelemetry<br/><i>Trace Propagation</i>"]
        AMQPLib["amqp091-go<br/><i>AMQP Client Library</i>"]
    end

    subgraph Package["rabbitmq package"]

        subgraph Connection["Connection Layer"]
            Client["<b>Client</b><br/>Connection Manager<br/><i>Dual connections, state machine,<br/>auto-reconnect with backoff</i>"]
            Pool["<b>ChannelPool</b><br/>Publisher Channel Pool<br/><i>Context-aware get/return,<br/>safe drain &amp; close</i>"]
            Cache["<b>TopologyCache</b><br/>Reconnection State<br/><i>Queues, exchanges, bindings<br/>restored after reconnect</i>"]
        end

        subgraph Messaging["Messaging Layer"]
            Producer["<b>Producer</b><br/>«Publisher»<br/><i>Publish with confirms,<br/>batch publish, OTel inject</i>"]
            Consumer["<b>Consumer</b><br/>«Subscriber»<br/><i>QoS, manual ack, retry/DLX,<br/>auto-reconnect, OTel extract</i>"]
            Batch["<b>BatchConsumer</b><br/>Batch Subscriber<br/><i>Accumulate → flush on size/timeout,<br/>batch ack/nack</i>"]
        end

        subgraph Infra["Infrastructure Layer"]
            Topology["<b>TopologyManager</b><br/>«TopologyDeclarer»<br/><i>Declare queues, exchanges,<br/>bindings, DLX setup</i>"]
        end

        subgraph Cfg["Configuration"]
            Config["<b>Config</b><br/><i>Connection, producer,<br/>consumer, queue, retry,<br/>reconnect settings</i>"]
        end
    end

    %% Internal relationships
    Producer -- "GetPublisherChannel(ctx)<br/>ReturnPublisherChannel" --> Client
    Consumer -- "CreateConsumerChannel" --> Client
    Batch -- "CreateConsumerChannel" --> Client
    Topology -- "CreateConsumerChannel" --> Client

    Client -- "fill / get / return /<br/>drain / close" --> Pool
    Client -. "restores on<br/>reconnect" .-> Cache
    Topology -. "caches<br/>declarations" .-> Cache

    Client -. "reads" .-> Config
    Producer -. "reads" .-> Config
    Consumer -. "reads" .-> Config

    %% External relationships
    Client -- "Dial / Channel" --> AMQPLib
    AMQPLib -- "AMQP 0-9-1<br/>protocol" --> Broker

    Producer -- "Inject trace ctx<br/>into headers" --> OTel
    Consumer -- "Extract trace ctx<br/>from headers" --> OTel
    Batch -- "Extract trace ctx<br/>from headers" --> OTel

    %% Styling
    classDef iface fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    classDef internal fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1px
    classDef ext fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef store fill:#e8f5e9,stroke:#2e7d32,stroke-width:1px

    class Producer,Consumer,Topology iface
    class Client,Pool,Batch internal
    class Broker,OTel,AMQPLib ext
    class Cache,Config store
```

## Architecture

### Connection Layer

**Client** manages two separate AMQP connections (publisher and consumer) with a state machine (`Disconnected → Connecting → Connected → Closing`). On connection loss, it triggers exponential-backoff reconnection and restores cached topology.

**ChannelPool** pre-allocates publisher channels into a bounded pool. `get(ctx)` respects context cancellation (no hardcoded timeouts). The pool is safe against send-to-closed-channel panics via atomic close guards.

**TopologyCache** stores declared queues, exchanges, and bindings. On reconnect, the cache is snapshot-and-reset before re-declaring, preventing unbounded growth across reconnection cycles.

### Messaging Layer

**Producer** (`Publisher` interface) publishes JSON messages with optional publisher confirms, OTel trace injection, and a single context-aware retry on disconnect.

**Consumer** (`Subscriber` interface) consumes messages with QoS prefetch, manual ack by default, retry counting via `x-death` headers, and dead-letter routing. Supports `AutoReconnect` mode that restarts the consume loop after delivery channel closure.

**BatchConsumer** accumulates messages up to `batchSize` or `flushTimeout`, then calls the batch handler. It manages its own AMQP channel and ack lifecycle to avoid double-ack bugs.

### Infrastructure Layer

**TopologyManager** (`TopologyDeclarer` interface) declares queues (with quorum support), exchanges, bindings, and sets up dead-letter exchanges with retry and parking-lot queues.

## Usage

```go
client, err := rabbitmq.NewClient(&rabbitmq.Config{
    URL:              "amqp://guest:guest@localhost:5672/",
    ConnectionName:   "my-service",
    PublisherConfirms: true,
    // ... see Config for all options
}, logger)
defer client.Close()

// Topology
tm := rabbitmq.NewTopologyManager(client)
tm.DeclareQueue(rabbitmq.QueueConfig{Name: "orders", Durable: true})

// Produce
producer := rabbitmq.NewProducer(client)
producer.SendMessage(ctx, "orders", payload)

// Consume (with auto-reconnect)
consumer := rabbitmq.NewConsumer(client, logger)
consumer.StartConsumer(ctx, rabbitmq.ConsumeOptions{
    QueueName:     "orders",
    AutoReconnect: true,
}, handler)
```

## Testing

```bash
make test
# Expands to: go test -tags integration -v -count=1 -timeout 300s ./...
```

Requires Docker (testcontainers).
