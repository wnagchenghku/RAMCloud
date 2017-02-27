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
    explicit RocksteadyMigrationManager(Context* context, string localLocator);
    ~RocksteadyMigrationManager();

    int poll();
    bool startMigration(ServerId sourceServerId, uint64_t tableId,
                uint64_t startKeyHash, uint64_t endKeyHash);

  PRIVATE:
    // Shared RAMCloud information.
    Context* context;

    Tub<ObjectManager::TombstoneProtector> tombstoneProtector;

    // Address of this RAMCloud master. Required by RocksteadyMigration.
    const string localLocator;

    // The list of in-progress migrations for which this RAMCloud master
    // is the destination.
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

    // Change as necessary.
    LogLevel ll = DEBUG;

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
            , currentHTBucket(startHTBucket)
            , currentHTBucketEntry(0)
            , totalPulledBytes(0)
            , totalReplayedBytes(0)
            , allDataPulled(false)
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

        bool allDataPulled;

        bool pullRpcInProgress;

        uint32_t numReplaysInProgress;

        Tub<Buffer> rpcBuffers[PARTITION_PIPELINE_DEPTH];

        std::deque<Tub<Buffer>*> freePullBuffers;

        std::deque<Tub<Buffer>*> freeReplayBuffers;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyHashPartition);
    };

    static const uint32_t MAX_NUM_PARTITIONS = 6;

    Tub<RocksteadyHashPartition> partitions[MAX_NUM_PARTITIONS];

    uint32_t numCompletedPartitions;

    class RocksteadyPullRpc {
      public:
        RocksteadyPullRpc(Context* context, ServerId sourceServerId,
                uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash,
                uint64_t currentHTBucket, uint64_t currentHTBucketEntry,
                uint64_t endHTBucket, uint32_t numRequestedBytes,
                Tub<Buffer>* response, Tub<RocksteadyHashPartition>* partition)
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
        Tub<RocksteadyHashPartition>* partition;

        Tub<Buffer>* responseBuffer;

        Tub<RocksteadyMigrationPullHashesRpc> rpc;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyPullRpc);
    };

    static const uint32_t MAX_PARALLEL_PULL_RPCS = 6;

    Tub<RocksteadyPullRpc> pullRpcs[MAX_PARALLEL_PULL_RPCS];

    std::deque<Tub<RocksteadyPullRpc>*> freePullRpcs;

    std::deque<Tub<RocksteadyPullRpc>*> busyPullRpcs;

    static const uint32_t MAX_PARALLEL_REPLAY_RPCS = 6;

    class RocksteadyReplayRpc : public Transport::ServerRpc {
      public:
        explicit RocksteadyReplayRpc(Tub<RocksteadyHashPartition>* partition,
                Tub<Buffer>* response, Tub<SideLog>* sideLog,
                string localLocator)
            : partition(partition)
            , responseBuffer(response)
            , sideLog(sideLog)
            , completed(false)
            , localLocator(localLocator)
        {
            WireFormat::RocksteadyMigrationReplay::Request* reqHdr =
                    requestPayload.emplaceAppend<
                    WireFormat::RocksteadyMigrationReplay::Request>();

            reqHdr->common.opcode =
                    WireFormat::RocksteadyMigrationReplay::opcode;
            reqHdr->common.service =
                    WireFormat::RocksteadyMigrationReplay::service;

            reqHdr->bufferPtr = reinterpret_cast<uintptr_t>(responseBuffer);
            reqHdr->sideLogPtr = reinterpret_cast<uintptr_t>(sideLog);

            // XXX: This needs to change. Ideally, the pull rpc should return
            // a certificate which goes in here.
            SegmentCertificate certificate;
            certificate.segmentLength = (*responseBuffer)->size();
            reqHdr->certificate = certificate;
        }

        ~RocksteadyReplayRpc() {}

        void sendReply()
        {
            completed = true;
        }

        string getClientServiceLocator()
        {
            return this->localLocator;
        }

        bool isReady()
        {
            return completed;
        }

      PRIVATE:
        Tub<RocksteadyHashPartition>* partition;

        Tub<Buffer>* responseBuffer;

        Tub<SideLog>* sideLog;

        bool completed;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyReplayRpc);
    };

    Tub<RocksteadyReplayRpc> replayRpcs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub<RocksteadyReplayRpc>*> freeReplayRpcs;

    std::deque<Tub<RocksteadyReplayRpc>*> busyReplayRpcs;

    Tub<SideLog> sideLogs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub<SideLog>*> freeSideLogs;

    friend class RocksteadyMigrationManager;
    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigration);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
