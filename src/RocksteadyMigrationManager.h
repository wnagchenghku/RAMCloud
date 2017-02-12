#ifndef RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
#define RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H

#include "Buffer.h"
#include "Context.h"
#include "SideLog.h"
#include "Dispatch.h"
#include "ServerId.h"
#include "Transport.h"
#include "MasterClient.h"
#include "ObjectManager.h"

namespace RAMCloud {

// Forward declaration.
class RocksteadyMigration;

class RocksteadyMigrationManager : Dispatch::Poller {
  public:
    explicit RocksteadyMigrationManager(Context* context);
    ~RocksteadyMigrationManager();

    int poll();
    bool startMigration(ServerId sourceServerId, uint64_t tableId,
                uint64_t startKeyHash, uint64_t endKeyHash);

  PRIVATE:
    Context* context;

    const string localLocator;

    std::vector<RocksteadyMigration*> migrationsInProgress;

    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigrationManager);
};

class RocksteadyMigration {
  public:
    explicit RocksteadyMigration(Context* context, string localLocator,
            ServerId sourceServerId, uint64_t tableId, uint64_t startKeyHash,
            uint64_t endKeyHash);
    ~RocksteadyMigration();

    int poll();

  PRIVATE:
    int prepare();
    int pullAndReplay();
    int tearDown();

    Context* context;

    ObjectManager* objectManager;

    const string localLocator;

    const ServerId sourceServerId;

    const uint64_t tableId;

    const uint64_t startKeyHash;

    const uint64_t endKeyHash;

    enum MigrationPhase {
        SETUP,

        MIGRATING_DATA,

        TEAR_DOWN,

        COMPLETED
    };

    MigrationPhase phase;

    uint64_t sourceNumHTBuckets;

    Tub<uint64_t> sourceSafeVersion;

    Tub<RocksteadyPrepForMigrationRpc> prepareSourceRpc;

    static const uint32_t PARTITION_PIPELINE_DEPTH = 2;

    class RocksteadyHashPartition {
      public:
        explicit RocksteadyHashPartition(uint64_t startHTBucket,
                uint64_t endHTBucket)
            : startHTBucket(startHTBucket)
            , endHTBucket(endHTBucket)
            , currentHTBucket(0)
            , currentHTBucketEntry(0)
            , totalPulledBytes(0)
            , totalReplayedBytes(0)
            , pullRpcInProgress(false)
            , numReplaysInProgress(0)
            , rpcBuffers()
            , freePullBuffers()
            , freeReplayBuffers()
        {
            // In the beginning, all buffers can be used for pull requests
            // to the destination.
            for (uint32_t i = 0; i < PARTITION_PIPELINE_DEPTH; i++) {
                freePullBuffers.push_back(&(rpcBuffers[i]));
            }
        }

        ~RocksteadyHashPartition() {}

      PRIVATE:
        const uint64_t startHTBucket;

        const uint64_t endHTBucket;

        uint64_t currentHTBucket;

        uint64_t currentHTBucketEntry;

        uint64_t totalPulledBytes;

        uint64_t totalReplayedBytes;

        bool pullRpcInProgress;

        uint32_t numReplaysInProgress;

        Tub<Buffer> rpcBuffers[PARTITION_PIPELINE_DEPTH];

        std::deque<Tub<Buffer>*> freePullBuffers;

        std::deque<Tub<Buffer>*> freeReplayBuffers;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyHashPartition);
    };

    static const uint32_t MAX_PARALLEL_PULL_RPCS = 3;

    Tub<RocksteadyHashPartition> partitions[MAX_PARALLEL_PULL_RPCS];

    class RocksteadyPullRpc {
      public:
        RocksteadyPullRpc(Context* context, ServerId sourceServerId,
                uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash,
                uint64_t currentHTBucket, uint64_t currentHTBucketEntry,
                uint64_t endHTBucket, uint32_t numRequestedBytes,
                Tub<Buffer>* response, RocksteadyHashPartition* partition)
            : partition(partition)
            , responseBuffer(response)
            , rpc()
        {
            rpc.construct(context, sourceServerId, tableId, startKeyHash,
                    endKeyHash, currentHTBucket, currentHTBucketEntry,
                    endHTBucket, numRequestedBytes, response->get());
        }

        ~RocksteadyPullRpc()
        {
            rpc.destroy();
        }

      PRIVATE:
        RocksteadyHashPartition* partition;

        Tub<Buffer>* responseBuffer;

        Tub<RocksteadyMigrationPullHashesRpc> rpc;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyPullRpc);
    };

    Tub<RocksteadyPullRpc> pullRpcs[MAX_PARALLEL_PULL_RPCS];

    std::deque<Tub<RocksteadyPullRpc>*> freePullRpcs;

    std::vector<Tub<RocksteadyPullRpc>*> busyPullRpcs;

    static const uint32_t MAX_PARALLEL_REPLAY_RPCS = 6;

    class RocksteadyReplayRpc : public Transport::ServerRpc {
      public:
        explicit RocksteadyReplayRpc(RocksteadyHashPartition* partition,
                Tub<Buffer>* response, string localLocator)
            : partition(partition)
            , responseBuffer(response)
            , localLocator(localLocator)
        {}

        ~RocksteadyReplayRpc() {}

        void sendReply() {}

        string getClientServiceLocator()
        {
            return this->localLocator;
        }

      PRIVATE:
        RocksteadyHashPartition* partition;

        Tub<Buffer>* responseBuffer;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyReplayRpc);
    };

    Tub<RocksteadyReplayRpc> replayRpcs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub<RocksteadyReplayRpc>*> freeReplayRpcs;

    std::vector<Tub<RocksteadyReplayRpc>*> busyReplayRpcs;

    Tub<SideLog> sideLogs[MAX_PARALLEL_REPLAY_RPCS];

    friend class RocksteadyMigrationManager;
    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigration);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
