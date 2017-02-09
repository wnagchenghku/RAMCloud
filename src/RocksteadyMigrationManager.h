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
    ~RocksteadyMigration() {}

    int poll();
    int prepare();
    int pullAndReplay();
    int tearDown();

  PRIVATE:
    enum MigrationPhase {
        SETUP,

        MIGRATING_DATA,

        TEAR_DOWN,

        COMPLETED
    };

    class RocksteadyReplayRpc : public Transport::ServerRpc {
      public:
        explicit RocksteadyReplayRpc(string localLocator)
            : localLocator(localLocator) {}
        ~RocksteadyReplayRpc() {}

        void sendReply() {}

        string getClientServiceLocator()
        {
            return this->localLocator;
        }

        const string localLocator;
    };

    class RocksteadyHashPartition {
      public:
        explicit RocksteadyHashPartition(uint64_t startHTBucket,
                uint64_t endHTBucket)
            : startHTBucket(startHTBucket)
            , endHTBucket(endHTBucket)
            , currentHTBucket(0)
            , currentHTBucketEntry(0)
            , totalNumReturnedBytes(0)
            , pullRpcInProgress(false)
            , numReplaysInProgress(0)
        {}
        ~RocksteadyHashPartition() {}

        const uint64_t startHTBucket;

        const uint64_t endHTBucket;

        const uint64_t currentHTBucket;

        const uint64_t currentHTBucketEntry;

        uint64_t totalNumReturnedBytes;

        bool pullRpcInProgress;

        uint32_t numReplaysInProgress;
    };

    Context* context;

    ObjectManager* objectManager;

    const string localLocator;

    const ServerId sourceServerId;

    const uint64_t tableId;

    const uint64_t startKeyHash;

    const uint64_t endKeyHash;

    MigrationPhase phase;

    static const uint64_t MAX_PARALLEL_PULL_RPCS = 3;

    static const uint64_t PARTITION_PIPELINE_DEPTH = 2;

    Tub<RocksteadyPrepForMigrationRpc> prepareSourceRpc;

    Tub<RocksteadyMigrationPullHashesRpc> pullRpcs[MAX_PARALLEL_PULL_RPCS];

    Tub<RocksteadyReplayRpc>
            replayTasks[MAX_PARALLEL_PULL_RPCS * PARTITION_PIPELINE_DEPTH];

    Tub<Buffer> pullBuffers[MAX_PARALLEL_PULL_RPCS * PARTITION_PIPELINE_DEPTH];

    Tub<SideLog> sideLogs[MAX_PARALLEL_PULL_RPCS * PARTITION_PIPELINE_DEPTH];

    Tub<RocksteadyHashPartition> partitions[MAX_PARALLEL_PULL_RPCS];

    friend class RocksteadyMigrationManager;
    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigration);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
