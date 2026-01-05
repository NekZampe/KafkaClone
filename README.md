-----

# KafkaClone: A High-Performance Distributed Streaming Platform in C\#

**KafkaClone** is a lightweight, educational implementation of a distributed event streaming platform, built from scratch in .NET 8. It replicates the core architecture of Apache Kafka, focusing on high-throughput sequential disk I/O, a custom binary protocol, and efficient networking.

## 🌟 Features

  * **Custom Binary Protocol:** Hand-rolled TCP protocol using `BinaryPrimitives` and `Span<T>` for low-allocation parsing.
  * **Segmented Log Storage:** Append-only storage engine splitting data into segments (`.log`) for efficient file management.
  * **Sparse Indexing:** Memory-mapped style `.index` files allowing $O(1)$ lookups for specific message offsets.
  * **Batch Processing:** Support for batched production and consumption to maximize network throughput.
  * **Consumer Groups:** Basic implementation of offset tracking for different consumer groups.
  * **Zero-Copy Networking:** Utilizes `BufferedStream` and direct memory manipulation to minimize GC pressure.

-----

## 🏗️ Architecture

The solution is divided into four main projects following Clean Architecture principles:

  * **`KafkaClone.Server`**: The core broker. Handles TCP connections, request routing, and manages the lifecycle of topics/partitions.
  * **`KafkaClone.Storage`**: The persistence engine. Manages directories, segmented files, and the sparse index lookup logic.
  * **`KafkaClone.Shared`**: Common data models (e.g., `Broker`, `Message`) and serialization logic shared between client and server.
  * **`KafkaClone.Playground`**: A CLI client used for producing, consuming, and load-testing the cluster.

### Storage Internals

Data is stored on disk using a paired file strategy:

  * `00000.log`: Contains the raw message bytes (Length + Payload).
  * `00000.index`: Stores `(RelativeOffset, PhysicalPosition)` pairs to map logical offsets to byte locations in the log.

-----

## 🚀 Getting Started

### Prerequisites

  * [.NET 8 SDK](https://dotnet.microsoft.com/download)

### Installation

1.  Clone the repository:

    ```bash
    git clone https://github.com/NekZampe/KafkaClone.git
    cd KafkaClone
    ```

2.  Build the solution:

    ```bash
    dotnet build
    ```

### Running the Cluster

**1. Start the Server**
The server listens on port `9092` by default. Data is stored in a local `kafka-data` directory.

```bash
dotnet run --project KafkaClone.Server
```

**2. Start the Client (Playground)**
Open a new terminal window to run the interactive CLI.

```bash
dotnet run --project KafkaClone.Playground
```

-----

## 🎮 CLI Usage Commands

Once the **Playground** client is running, you can interact with the server using the following command codes:

| Command | Syntax | Description | Example |
| :--- | :--- | :--- | :--- |
| **0** | `0 <topic> <partitions>` | **Create Topic** | `0 payments 3` |
| **1** | `1 <topic> <message>` | **Produce (Single)** | `1 payments "Hello World"` |
| **2** | `2 <topic> <pid> <offset>` | **Consume (Single)** | `2 payments 0 0` |
| **3** | `3 <group> <topic> <pid> <offset>` | **Commit Offset** | `3 groupA payments 0 15` |
| **4** | `4 <group> <topic> <pid>` | **Fetch Offset** | `4 groupA payments 0` |
| **5** | `5 <topic> <msg1>,<msg2>...` | **Batch Produce** | `5 logs error1,error2,error3` |
| **6** | `6 <topic> <pid> <offset> <count>` | **Batch Consume** | `6 logs 0 0 100` |
| **TEST** | `test <topic> <count>` | **Run Load Test** | `test logs 10000` |


-----

## 🗺️ Roadmap

  - [x] TCP Server & Binary Protocol
  - [x] Segmented Log Storage
  - [x] Sparse Indexing
  - [x] Batch Producer/Consumer
  - [ ] **Cluster Controller & Broker Discovery** (In Progress)
  - [ ] Partition Replication
  - [ ] Leader Election ( KRaft ) 

-----

## 📄 License

This project is licensed under the MIT License.


<img width="553" height="682" alt="image" src="https://github.com/user-attachments/assets/0336f7fa-5aee-449c-af29-6bf24d5ad575" />

