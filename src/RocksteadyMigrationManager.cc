#include <new>

#include "OptionParser.h"
#include "WorkerManager.h"
#include "MasterService.h"
#include "RocksteadyMigrationManager.h"

namespace RAMCloud {

/**
 * Constructor for RocksteadyMigrationManager. The manager is registered
 * as a Poller with this master's dispatch thread.
 */
RocksteadyMigrationManager::RocksteadyMigrationManager(Context* context)
    : Dispatch::Poller(context->dispatch, "RocksteadyMigrationManager")
    , context(context)
    , localLocator(context->options->getLocalLocator())
    , migrationsInProgress()
{
    ;
}

/**
 * Destructor for RocksteadyMigrationManager. Before destroying the manager,
 * make sure that all in-progress migrations for which this master is the
 * destination complete.

 * This method could end up hogging the dispatch thread if there are a large
 * number of in-progress migrations when invoked. However, it is very likely
 * that this destructor is called only when a master is shutting down. The
 * alternative is to cancel any in-progress migrations and deal with two
 * recoveries.
 */
RocksteadyMigrationManager::~RocksteadyMigrationManager()
{
    for (auto migration = migrationsInProgress.begin();
            migration != migrationsInProgress.end(); ) {
        RocksteadyMigration* currentMigration = *migration;

        // If this migration has not completed, wait for it to do so.
        while (currentMigration->phase != RocksteadyMigration::COMPLETED) {
            currentMigration->poll();
        }

        migration = migrationsInProgress.erase(migration);
        delete currentMigration;
    }
}

/**
 * Poll on any in-progress migrations on this RAMCloud master.
 */
int
RocksteadyMigrationManager::poll()
{
    int workPerformed = 0;

    for (auto migration = migrationsInProgress.begin();
        migration != migrationsInProgress.end(); ) {
        RocksteadyMigration* currentMigration = *migration;

        workPerformed += currentMigration->poll();

        // Delete any completed migrations.
        if (currentMigration->phase == RocksteadyMigration::COMPLETED) {
            migration = migrationsInProgress.erase(migration);
            delete currentMigration;
        } else {
            migration++;
        }
    }

    return workPerformed == 0 ? 0 : 1;
}

/**
 * Add a new migration to this RAMCloud master.
 *
 * \param[in] sourceServerId
 *      Identifier of the source server.
 * \param[in] tableId
 *      Identifier of the tablet the requested tablet belongs to.
 * \param[in] startKeyHash
 *      Starting key hash of the tablet to be migrated.
 * \param[in] endKeyHash
 *      Ending key hash of the tablet to be migrated.
 *
 * \return
 *      True if the migration was added to the master's migration manager.
 *      False if it wasn't because there was a pre-existing migration that
 *      overlapped with it.
 */
bool
RocksteadyMigrationManager::startMigration(ServerId sourceServerId,
                            uint64_t tableId, uint64_t startKeyHash,
                            uint64_t endKeyHash)
{
    // Check if
    // - the requested tablet is already under migration.
    // - an overlapping tablet is already under migration.
    for (auto& migration : migrationsInProgress) {
        // This condition checks for an overlapping in progress migration. It
        // assumes that the rest of RAMCloud is correct.
        bool migrationExists = (sourceServerId == migration->sourceServerId) &&
                (tableId = migration->tableId) &&
                // Either startKeyHash overlaps with an existing migration
                ((startKeyHash >= migration->startKeyHash &&
                startKeyHash <= migration->endKeyHash) ||
                // or endKeyHash overlaps with an existing migration
                (endKeyHash >= migration->startKeyHash &&
                endKeyHash <= migration->endKeyHash) ||
                // or an existing migration is a subset of the requested tablet.
                (startKeyHash <= migration->startKeyHash &&
                endKeyHash >= migration->endKeyHash));

        if (migrationExists) {
            return false;
        }
    }

    // Add the new migration to the manager.
    RocksteadyMigration* newMigration = new RocksteadyMigration(context,
                                        this->localLocator, sourceServerId,
                                        tableId, startKeyHash, endKeyHash);

    migrationsInProgress.push_back(newMigration);

    return true;
}

RocksteadyMigration::RocksteadyMigration(Context* context,
                        string localLocator, ServerId sourceServerId,
                        uint64_t tableId, uint64_t startKeyHash,
                        uint64_t endKeyHash)
    : context(context)
    , objectManager()
    , localLocator(localLocator)
    , sourceServerId(sourceServerId)
    , tableId(tableId)
    , startKeyHash(startKeyHash)
    , endKeyHash(endKeyHash)
    , phase(RocksteadyMigration::SETUP)
    , sourceNumHTBuckets()
    , sourceSafeVersion()
    , prepareSourceRpc()
    , partitions()
    , numCompletedPartitions(0)
    , pullRpcs()
    , freePullRpcs()
    , busyPullRpcs()
    , replayRpcs()
    , freeReplayRpcs()
    , busyReplayRpcs()
    , sideLogs()
    , freeSideLogs()
{
    objectManager = &((context->getMasterService())->objectManager);

    // To begin with, all pull rpcs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_PULL_RPCS; i++) {
        freePullRpcs.push_back(&(pullRpcs[i]));
    }

    // To begin with, all replay rpcs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeReplayRpcs.push_back(&(replayRpcs[i]));
    }

    // Construct all the sidelogs.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        sideLogs[i].construct(objectManager->getLog());
    }

    // To begin with, all sidelogs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeSideLogs.push_back(&(sideLogs[i]));
    }
}

RocksteadyMigration::~RocksteadyMigration()
{
    sourceSafeVersion.destroy();
    prepareSourceRpc.destroy();
}

int
RocksteadyMigration::poll()
{
    switch(phase) {
        case SETUP : return prepare();

        case MIGRATING_DATA : return pullAndReplay();

        case TEAR_DOWN : return tearDown();

        case COMPLETED : return 0;

        default : return 0;
    }
}

int
RocksteadyMigration::prepare()
{
    // TODO: Once the take-ownership rpc has been implemented, add code here
    // to check if it has completed. Once completed, construct the set of
    // partitions.

    if (prepareSourceRpc) {
        // If the prepare rpc was sent out, check for it's completion.
        if (prepareSourceRpc->isReady()) {
            // If the prepare rpc has completed, update the local safeVersion.
            sourceSafeVersion.construct(
                    prepareSourceRpc->wait(&sourceNumHTBuckets));
            objectManager->raiseSafeVersion(*sourceSafeVersion);

            // TODO: Once the take-ownership rpc has been implemented, add
            // code here to invoke it.

            // TODO: This code should be moved to after the destination has
            // taken over ownership of the tablet under migration.
            for (uint32_t i = 0; i < MAX_NUM_PARTITIONS; i++) {
                uint64_t partitionStartHTBucket =
                        i * (sourceNumHTBuckets / MAX_NUM_PARTITIONS);
                uint64_t partitionEndHTBucket =
                        ((i + 1) * (sourceNumHTBuckets /
                        MAX_NUM_PARTITIONS)) - 1;

                partitions[i].construct(partitionStartHTBucket,
                        partitionEndHTBucket);

                RAMCLOUD_LOG(DEBUG, "Created hash table partition from bucket"
                        " %lu to bucket %lu. (Migrating tablet [0x%lx, 0x%lx],"
                        " tableId %lu)", partitionStartHTBucket,
                        partitionEndHTBucket, startKeyHash, endKeyHash,
                        tableId);
            }

            // The destination can now start migrating data.
            phase = MIGRATING_DATA;

            return 1;
        } else {
            // If the prepare rpc was sent out, but a response hasn't been
            // received yet.
            return 0;
        }
    } else {
        // Send out the prepare rpc if it hasn't been sent out yet.
        prepareSourceRpc.construct(context, sourceServerId, tableId,
                startKeyHash, endKeyHash);

        return 1;
    }
}

int
RocksteadyMigration::pullAndReplay()
{
    int workDone = 0;

    // Check if any of the in-progress pulls have completed.
    for (size_t i = 0; i < busyPullRpcs.size(); i++) {
        Tub<RocksteadyPullRpc>* pullRpc = busyPullRpcs.front();

        if ((*pullRpc)->rpc->isReady()) {
            // If this pull rpc completed, first obtain the return values.
            uint64_t nextHTBucket = 0;
            uint64_t nextHTBucketEntry = 0;
            uint32_t numReturnedBytes = 0;

            numReturnedBytes = (*pullRpc)->rpc->wait(&nextHTBucket,
                    &nextHTBucketEntry);

            // Update the partition state on which this pull rpc was issued.
            Tub<RocksteadyHashPartition>* partition = (*pullRpc)->partition;
            (*partition)->currentHTBucket = nextHTBucket;
            (*partition)->currentHTBucketEntry = nextHTBucketEntry;
            (*partition)->totalPulledBytes += numReturnedBytes;

            // There is nothing left to be pulled from within this partition.
            if ((*partition)->currentHTBucket > (*partition)->endHTBucket) {
                (*partition)->allDataPulled = true;
            }

            // The response buffer is now eligible for replay.
            (*partition)->freeReplayBuffers.push_back(
                    (*pullRpc)->responseBuffer);
            (*partition)->pullRpcInProgress = false;

            // Add this rpc to the free list.
            (*pullRpc).destroy();
            freePullRpcs.push_back(pullRpc);

            workDone++;
        } else {
            busyPullRpcs.push_back(pullRpc);
        }

        busyPullRpcs.pop_front();
    }

    // Check if all in-progress replays have completed.
    for (size_t i = 0; i < busyReplayRpcs.size(); i++) {
        Tub<RocksteadyReplayRpc>* replayRpc = busyReplayRpcs.front();

        if ((*replayRpc)->isReady()) {
            // If the replay has completed, update the corresponding
            // partition's state.
            Tub<RocksteadyHashPartition>* partition = (*replayRpc)->partition;
            Tub<Buffer>* responseBuffer = (*replayRpc)->responseBuffer;
            Tub<SideLog>* sideLog = (*replayRpc)->sideLog;
            uint32_t numReplayedBytes = (*responseBuffer)->size();

            // Free up the response buffer so that it can be used for a pull
            // rpc.
            (*responseBuffer).destroy();
            (*partition)->freePullBuffers.push_back(responseBuffer);
            (*partition)->totalReplayedBytes += numReplayedBytes;
            (*partition)->numReplaysInProgress--;

            // All data within this partition has been pulled and replayed.
            if (((*partition)->allDataPulled == true) &&
                    ((*partition)->freeReplayBuffers.size() == 0)) {
                (*partition).destroy();
                numCompletedPartitions++;
            }

            // Add the sideLog to the free list.
            freeSideLogs.push_back(sideLog);

            // Add this rpc to the free list.
            (*replayRpc).destroy();
            freeReplayRpcs.push_back(replayRpc);

            workDone++;
        } else {
            busyReplayRpcs.push_back(replayRpc);
        }

        busyReplayRpcs.pop_front();
    }

    // All partitions have completed pulling and replaying data.
    if (numCompletedPartitions == MAX_NUM_PARTITIONS) {
        phase = TEAR_DOWN;
        return workDone;
    }

    // Issue a new batch of pulls if possible.
    if (freePullRpcs.size() != 0 ) {
        // Identify the set of partitions on which a pull rpc can be
        // issued.
        std::deque<Tub<RocksteadyHashPartition>*> candidatePartitions;

        for (uint32_t i = 0; i < MAX_NUM_PARTITIONS; i++) {
            // A partition is eligible for a pull rpc only if
            // - there is no other pull rpc in progress on it.
            // - there is a free buffer on which a pull rpc can be issued.
            // - there is data left to be pulled.
            if ((partitions[i]->pullRpcInProgress == false) &&
                    (partitions[i]->freePullBuffers.size() != 0) &&
                    (partitions[i]->allDataPulled == false)) {
                candidatePartitions.push_back(&(partitions[i]));
            }
        }

        // Sort the set of candidate partitions on the number of bytes pulled
        // so far.
        std::sort(candidatePartitions.begin(), candidatePartitions.end(),
                [](const Tub<RocksteadyHashPartition>* leftPartition,
                const Tub<RocksteadyHashPartition>* rightPartition) -> bool
                {
                    return (*leftPartition)->totalPulledBytes <
                            (*rightPartition)->totalPulledBytes;
                });

        // Issue a new batch of pulls on the set of candidate partitions.
        while ((freePullRpcs.size() != 0) &&
                (candidatePartitions.size() != 0)) {
            // Get a pull rpc and partition to issue it on.
            Tub<RocksteadyPullRpc>* pullRpc = freePullRpcs.front();
            Tub<RocksteadyHashPartition>* partition =
                    candidatePartitions.front();

            uint64_t currentHTBucket = (*partition)->currentHTBucket;
            uint64_t currentHTBucketEntry = (*partition)->currentHTBucketEntry;
            uint64_t endHTBucket = (*partition)->endHTBucket;
            Tub<Buffer>* responseBuffer = (*partition)->freePullBuffers.front();
            (*responseBuffer).construct();

            // Issue the rpc.
            (*pullRpc).construct(context, sourceServerId, tableId, startKeyHash,
                    endKeyHash, currentHTBucket, currentHTBucketEntry,
                    endHTBucket, 10 * 1024 /* Ask the source for 10 KB */ ,
                    responseBuffer, partition);

            // Update partition and pull rpc state.
            (*partition)->freePullBuffers.pop_front();
            (*partition)->pullRpcInProgress = true;
            candidatePartitions.pop_front();
            busyPullRpcs.push_back(pullRpc);
            freePullRpcs.pop_front();

            workDone++;
        }
    }

    // Issue a new batch of replays if possible.
    if (freeReplayRpcs.size() != 0) {
        // Identify the set of partitions eligible for a replay.
        std::deque<Tub<RocksteadyHashPartition>*> candidatePartitions;

        // A partition is eligible for a replay only if
        // - it still exists.
        // - the number of in-progress replays is lesser than the pipeline
        //   depth.
        // - there is data to be replayed within the partition.
        for (uint32_t i = 0; i < MAX_NUM_PARTITIONS; i++) {
            if (partitions[i] &&
                    (partitions[i]->numReplaysInProgress <
                    PARTITION_PIPELINE_DEPTH) &&
                    (partitions[i]->freeReplayBuffers.size() != 0)) {
                candidatePartitions.push_back(&(partitions[i]));
            }
        }

        // Sort the set of candidate partitions on the number of bytes
        // replayed so far.
        std::sort(candidatePartitions.begin(), candidatePartitions.end(),
                [](const Tub<RocksteadyHashPartition>* leftPartition,
                const Tub<RocksteadyHashPartition>* rightPartition) -> bool
                {
                    return (*leftPartition)->totalReplayedBytes <
                            (*rightPartition)->totalReplayedBytes;
                });

        // Issue the next batch of replays.
        while ((freeReplayRpcs.size() != 0) &&
                (candidatePartitions.size() != 0)) {
            // Get a replay rpc and a partition to issue it on.
            Tub<RocksteadyReplayRpc>* replayRpc = freeReplayRpcs.front();
            Tub<SideLog>* sideLog = freeSideLogs.front();
            Tub<RocksteadyHashPartition>* partition =
                    candidatePartitions.front();
            Tub<Buffer>* responseBuffer =
                    (*partition)->freeReplayBuffers.front();

            // First remove the pull rpc's response header.
            (*responseBuffer)->truncateFront(sizeof32(
                    WireFormat::RocksteadyMigrationReplay::Response));

            // Construct the rpc and hand it over to the worker manager.
            (*replayRpc).construct(partition, responseBuffer, sideLog,
                    localLocator);
            context->workerManager->handleRpc(replayRpc->get());

            (*partition)->freeReplayBuffers.pop_front();
            (*partition)->numReplaysInProgress++;

            // If there are more buffers within this partition, re-enqueue
            // it for replay.
            if (((*partition)->numReplaysInProgress <
                    PARTITION_PIPELINE_DEPTH) &&
                    ((*partition)->freeReplayBuffers.size() != 0)) {
                candidatePartitions.push_back(partition);
            }
            candidatePartitions.pop_front();

            freeSideLogs.pop_front();

            // The rpc is now busy.
            busyReplayRpcs.push_back(replayRpc);
            freeReplayRpcs.pop_front();

            workDone++;
        }
    }

    return workDone == 0 ? 0 : 1;
}

int
RocksteadyMigration::tearDown()
{
    int workDone = 0;

    // TODO: Add code to issue a completion rpc to the source here.

    // Commit all the sidelogs to the main in-memory log.
    for(uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        sideLogs[i]->commit();
    }

    phase = COMPLETED;
    workDone++;

    return workDone;
}

}  // namespace RAMCloud
