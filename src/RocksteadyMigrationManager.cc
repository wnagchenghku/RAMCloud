#include <new>

#include "OptionParser.h"
#include "WorkerManager.h"
#include "MasterService.h"
#include "RocksteadyMigrationManager.h"

// Uncomment to disable replay of migrated data. Useful for benchmarking
// RocksteadyMigrationPullHashesRpc() throughput.
// #define ROCKSTEADY_NO_REPLAY

// Uncomment to enable a check for whether the migration manager is work
// conserving at the target machine.
// #define ROCKSTEADY_CHECK_WORK_CONSERVING

// #define ROCKSTEADY_RPC_UTILIZATION

namespace RAMCloud {

/**
 * Constructor for RocksteadyMigrationManager. The manager is registered
 * as a Poller with this master's dispatch thread.
 */
RocksteadyMigrationManager::RocksteadyMigrationManager(Context* context,
        string localLocator)
    : Dispatch::Poller(context->dispatch, "RocksteadyMigrationManager")
    , context(context)
    , tombstoneProtector()
    , localLocator(localLocator)
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

    // Check to see if a tombstone protector is required.
    if (migrationsInProgress.size() == 0) {
        // If the migration manager is holding a tombstone protector and there
        // aren't any in-progress migrations, release the protector.
        if (tombstoneProtector) {
            tombstoneProtector.destroy();
        }
    } else {
        // If the migration manager is not holding a tombstone protector and
        // there are in-progress migrations, acquire a protector.
        if (!tombstoneProtector) {
            tombstoneProtector.construct(
                    &(context->getMasterService())->objectManager);
        }
    }

    // Poll on any in-progress migrations.
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

    // Add the new migration to the manager. The check for a tombstone
    // protector will be made before the poll() method on this migration
    // is invoked.
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
    , tabletManager()
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
    , getHeadOfLogRpc()
    , takeOwnershipRpc()
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
    , nextSideLogCommit(0)
    , sideLogCommitRpc()
{
    // Get a pointer to this master's tablet and object manager.
    tabletManager = &((context->getMasterService())->tabletManager);
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
        sideLogs[i].construct(objectManager->getLog(),
                context->rocksteadyMigrationManager);
    }

    // To begin with, all sidelogs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeSideLogs.push_back(&(sideLogs[i]));
    }
}

RocksteadyMigration::~RocksteadyMigration()
{
    takeOwnershipRpc.destroy();
    getHeadOfLogRpc.destroy();
    sourceSafeVersion.destroy();
    prepareSourceRpc.destroy();
}

int
RocksteadyMigration::poll()
{
    switch(phase) {
        case SETUP : return prepare();

        case MIGRATING_DATA : return pullAndReplay();

        case SIDELOG_COMMIT : return sideLogCommit();

        case TEAR_DOWN : return tearDown();

        case COMPLETED : return 0;

        default : return 0;
    }
}

int
RocksteadyMigration::prepare()
{
    if (takeOwnershipRpc) {
        // If the take ownership rpc was sent out, check for it's completion.
        if (takeOwnershipRpc->isReady()) {
            takeOwnershipRpc->wait();

            // Create logical partitions on the source's hash table once the
            // take ownership rpc has returned.
            for (uint32_t i = 0; i < MAX_NUM_PARTITIONS; i++) {
                uint64_t partitionStartHTBucket =
                        i * (sourceNumHTBuckets / MAX_NUM_PARTITIONS);
                uint64_t partitionEndHTBucket =
                        ((i + 1) * (sourceNumHTBuckets /
                        MAX_NUM_PARTITIONS)) - 1;

                partitions[i].construct(partitionStartHTBucket,
                        partitionEndHTBucket);

                RAMCLOUD_LOG(ll, "Created hash table partition from"
                        " bucket %lu to bucket %lu (Migrating tablet [0x%lx,"
                        " 0x%lx], tableId %lu).", partitionStartHTBucket,
                        partitionEndHTBucket, startKeyHash, endKeyHash,
                        tableId);
            }

            // The destination can now start migrating data.
            phase = MIGRATING_DATA;

            return 1;
        } else {
            return 0;
        }
    } else if (getHeadOfLogRpc) {
        if (getHeadOfLogRpc->isReady()) {
            LogPosition head = getHeadOfLogRpc->wait();

            // Add the tablet to the target. The tablet starts off in state
            // ROCKSTEADY_MIGRATION.
            tabletManager->addTablet(tableId, startKeyHash, endKeyHash,
                    TabletManager::ROCKSTEADY_MIGRATING);

            RAMCLOUD_LOG(ll, "Added tablet[0x%lx, 0x%lx] in table %lu with"
                    " state ROCKSTEADY_MIGRATING. The tablet's logical"
                    " creation time is [%lu, %u]", startKeyHash, endKeyHash,
                    tableId, head.getSegmentId(), head.getSegmentOffset());

            // Initiate ownership transfer for the tablet under migration.
            uint64_t ctimeSegmentId = head.getSegmentId();
            uint64_t ctimeSegmentOffset = head.getSegmentOffset();
            takeOwnershipRpc.construct(context, tableId, startKeyHash,
                    endKeyHash, (context->getMasterService())->serverId,
                    ctimeSegmentId, ctimeSegmentOffset);

            return 1;
        } else {
            return 0;
        }
    } else if (prepareSourceRpc) {
        // If the prepare rpc was sent out, check for it's completion.
        if (prepareSourceRpc->isReady()) {
            // If the prepare rpc has completed, update the local safeVersion.
            sourceSafeVersion.construct(
                    prepareSourceRpc->wait(&sourceNumHTBuckets));
            objectManager->raiseSafeVersion(*sourceSafeVersion);

            RAMCLOUD_LOG(ll, "Successfully raised safeVersion above %lu"
                    " in preparation for migrating tablet[0x%lx, 0x%lx] in"
                    " table %lu from master %lu.", *sourceSafeVersion,
                    startKeyHash, endKeyHash, tableId, sourceServerId.getId());

            // Obtain the head of this master's log in order to initiate
            // ownership transfer.
            getHeadOfLogRpc.construct((context->getMasterService())->serverId,
                    localLocator);
            context->workerManager->handleRpc(getHeadOfLogRpc.get());

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

        RAMCLOUD_LOG(ll, "Sent out a prepare-for-migration request to"
                " master %lu for tablet[0x%lx, 0x%lx] in table %lu.",
                sourceServerId.getId(), startKeyHash, endKeyHash, tableId);

        return 1;
    }
}

// TODO: Break this up into multiple inlined functions.
int
RocksteadyMigration::pullAndReplay()
{
    int workDone = 0;

    // STEP-1: Check if any of the in-progress pulls have completed.
    size_t numBusyPullRpcs = busyPullRpcs.size();
    for (size_t i = 0; i < numBusyPullRpcs; i++) {
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

            LOG(ll, "Pull request migrated %u Bytes in partition[%lu,"
                    " %lu] (migrating tablet[0x%lx, 0x%lx] in table %lu)."
                    " Partition has %lu buffers scheduled for replay and"
                    " %lu buffers available for pull requests.",
                    numReturnedBytes, (*partition)->startHTBucket,
                    (*partition)->endHTBucket, startKeyHash, endKeyHash,
                    tableId, (*partition)->freeReplayBuffers.size() + 1,
                    (*partition)->freePullBuffers.size());

            // There is nothing left to be pulled from within this partition.
            if ((*partition)->currentHTBucket > (*partition)->endHTBucket) {
                (*partition)->allDataPulled = true;

                LOG(ll, "Finished pulling all data within partition[%lu,"
                        " %lu] (migrating tablet[0x%lx, 0x%lx] in table %lu).",
                        (*partition)->startHTBucket, (*partition)->endHTBucket,
                        startKeyHash, endKeyHash, tableId);
            }

#ifdef ROCKSTEADY_NO_REPLAY
            (*((*pullRpc)->responseBuffer)).destroy();
            (*partition)->freePullBuffers.push_back(
                    (*pullRpc)->responseBuffer);
            (*partition)->pullRpcInProgress = false;

            if ((*partition)->allDataPulled) {
                LOG(NOTICE, "Finished replaying all data in partition[%lu,"
                        " %lu]. Pulled %lu Bytes and replayed %lu Bytes"
                        " (Migrating tablet[0x%lx, 0x%lx] in table %lu).",
                        (*partition)->startHTBucket, (*partition)->endHTBucket,
                        (*partition)->totalPulledBytes,
                        (*partition)->totalReplayedBytes, startKeyHash,
                        endKeyHash, tableId);

                (*partition).destroy();
                numCompletedPartitions++;
            }
#else
            // The response buffer is now eligible for replay.
            (*partition)->freeReplayBuffers.push_back(
                    (*pullRpc)->responseBuffer);
            (*partition)->pullRpcInProgress = false;
#endif

            // Add this rpc to the free list.
            (*pullRpc).destroy();
            freePullRpcs.push_back(pullRpc);

            workDone++;
        } else {
            // This pull rpc has not completed yet.
            busyPullRpcs.push_back(pullRpc);
        }

        busyPullRpcs.pop_front();
    } // End of STEP-1.

    // STEP-2: Check if any in-progress replays have completed.
    size_t numBusyReplayRpcs = busyReplayRpcs.size();
    for (size_t i = 0; i < numBusyReplayRpcs; i++) {
        Tub<RocksteadyReplayRpc>* replayRpc = busyReplayRpcs.front();

        if ((*replayRpc)->isReady()) {
            // If the replay has completed, update the corresponding
            // partition's state.
            Tub<RocksteadyHashPartition>* partition = (*replayRpc)->partition;
            Tub<Buffer>* responseBuffer = (*replayRpc)->responseBuffer;
            Tub<SideLog>* sideLog = (*replayRpc)->sideLog;
            uint32_t numReplayedBytes = (*responseBuffer)->size();

            LOG(ll, "Replay request replayed %u Bytes in partition[%lu,"
                    " %lu] (Migrating tablet[0x%lx, 0x%lx] in table %lu)."
                    " Partition has %lu buffers scheduled for replay and"
                    " %lu buffers available for pull requests.",
                    numReplayedBytes, (*partition)->startHTBucket,
                    (*partition)->endHTBucket, startKeyHash, endKeyHash,
                    tableId, (*partition)->freeReplayBuffers.size(),
                    (*partition)->freePullBuffers.size() + 1);

            // Free up the response buffer so that it can be used for a pull
            // rpc.
            (*responseBuffer).destroy();
            (*partition)->freePullBuffers.push_back(responseBuffer);
            (*partition)->totalReplayedBytes += numReplayedBytes;
            (*partition)->numReplaysInProgress--;

            // All data within this partition has been pulled and replayed.
            if (((*partition)->allDataPulled == true) &&
                    ((*partition)->freeReplayBuffers.size() == 0) &&
                    ((*partition)->numReplaysInProgress == 0)) {
                LOG(ll, "Finished replaying all data in partition[%lu,"
                        " %lu]. Pulled %lu Bytes and replayed %lu Bytes"
                        " (Migrating tablet[0x%lx, 0x%lx] in table %lu).",
                        (*partition)->startHTBucket, (*partition)->endHTBucket,
                        (*partition)->totalPulledBytes,
                        (*partition)->totalReplayedBytes, startKeyHash,
                        endKeyHash, tableId);

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
    } // End of STEP-2.

    // STEP-3: All partitions have completed pulling and replaying data.
    if (numCompletedPartitions == MAX_NUM_PARTITIONS) {
        LOG(NOTICE, "Migration has completed on all partitions. Changing"
                " state to SIDELOG_COMMIT (Tablet[0x%lx, 0x%lx] in table %lu).",
                startKeyHash, endKeyHash, tableId);

        phase = SIDELOG_COMMIT;
        return workDone;
    } // End of STEP-3.

    // STEP-4: Issue a new batch of pulls if possible.
    if (freePullRpcs.size() != 0 ) {
        // Identify the set of partitions on which a pull rpc can be
        // issued.
        std::deque<Tub<RocksteadyHashPartition>*> candidatePartitions;

        for (uint32_t i = 0; i < MAX_NUM_PARTITIONS; i++) {
            // A partition is eligible for a pull rpc only if
            // - it still exists.
            // - there is no other pull rpc in progress on it.
            // - there is a free buffer on which a pull rpc can be issued.
            // - there is data left to be pulled.
            if (partitions[i] && (partitions[i]->pullRpcInProgress == false) &&
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
                    endHTBucket, 10 * 1024 /* Ask the source for 10 KB */,
                    responseBuffer, partition);

            LOG(ll, "Issued pull on partition[%lu, %lu] starting at"
                    " entry %lu in bucket %lu (Migrating tablet[0x%lx, 0x%lx]"
                    " in table %lu). Partition has %lu buffers scheduled for"
                    " replay and %lu buffers available for pull requests.",
                    (*partition)->startHTBucket, (*partition)->endHTBucket,
                    currentHTBucketEntry, currentHTBucket, startKeyHash,
                    endKeyHash, tableId, (*partition)->freeReplayBuffers.size(),
                    (*partition)->freePullBuffers.size() - 1);

            // Update partition and pull rpc state.
            (*partition)->freePullBuffers.pop_front();
            (*partition)->pullRpcInProgress = true;
            candidatePartitions.pop_front();
            busyPullRpcs.push_back(pullRpc);
            freePullRpcs.pop_front();

            workDone++;
        }
    } // End of STEP-4.

    // STEP-5: Issue a new batch of replays if possible.
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

            // First, retrieve the certificate for this buffer of log entries.
            void* respHdr = (*responseBuffer)->getRange(0, sizeof32(
                    WireFormat::RocksteadyMigrationPullHashes::Response));

            SegmentCertificate certificate =
                    (reinterpret_cast<
                    WireFormat::RocksteadyMigrationPullHashes::Response*>(
                    respHdr))->certificate;

            // Remove the pull rpc's response header.
            (*responseBuffer)->truncateFront(sizeof32(
                    WireFormat::RocksteadyMigrationPullHashes::Response));

            // Construct the rpc and hand it over to the worker manager.
            (*replayRpc).construct(partition, responseBuffer, sideLog,
                    localLocator, certificate);
            context->workerManager->handleRpc(replayRpc->get());

            LOG(ll, "Issued replay on partition[%lu, %lu] (Migrating"
                    " tablet[0x%lx, 0x%lx] in table %lu). Partition has %lu"
                    " buffers scheduled for replay and %lu buffers available"
                    " for pull requests.", (*partition)->startHTBucket,
                    (*partition)->endHTBucket, startKeyHash, endKeyHash,
                    tableId, (*partition)->freeReplayBuffers.size() - 1,
                    (*partition)->freePullBuffers.size());

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
    } // End of STEP-5.

#ifdef ROCKSTEADY_CHECK_WORK_CONSERVING
    size_t numIdleWorkers = context->workerManager->testingNumIdleWorkers();
    if (numIdleWorkers > 0) {
        uint64_t numQueuedBuffers = 0;
        for (uint32_t i = 0; i < MAX_NUM_PARTITIONS; i++) {
            if (!partitions[i]) {
                continue;
            }

            if ((partitions[i]->freeReplayBuffers).size() != 0) {
                numQueuedBuffers += (partitions[i]->freeReplayBuffers).size();
            }
        }

        if (numQueuedBuffers > 0) {
            LOG(WARNING, "Migration manager is not work conserving. There are"
                    " %lu data buffers waiting for replay inspite of there"
                    " being %lu idle worker threads.", numQueuedBuffers,
                    numIdleWorkers);
        }
    }
#endif

#ifdef ROCKSTEADY_RPC_UTILIZATION
    if (freeReplayRpcs.size() != 0) {
        LOG(NOTICE, "There are %zd free replay rpcs", freeReplayRpcs.size());
    }

    if (freePullRpcs.size() != 0) {
        LOG(NOTICE, "There are %zd free pull rpcs", freePullRpcs.size());
    }

    if (freeReplayRpcs.size() != freeSideLogs.size()) {
        LOG(WARNING, "The number of free replay rpcs are not equal to the"
                " number of free sidelogs. Bug?");
    }
#endif

    return workDone == 0 ? 0 : 1;
}

int
RocksteadyMigration::sideLogCommit()
{
    int workDone = 0;

    // Rule 1: If a sidelog commit is in progress, check to see if it has
    // completed.
    if (sideLogCommitRpc) {
        if (sideLogCommitRpc->isReady()) {
            LOG(ll, "Completed commit on sidelog %u (Tablet[0x%lx, 0x%lx]"
                    " in table %lu", nextSideLogCommit, startKeyHash,
                    endKeyHash, tableId);

            sideLogCommitRpc.destroy();
            nextSideLogCommit++;
            workDone++;
        } else {
            return workDone;
        }
    } // End of Rule 1.

    if (!sideLogCommitRpc) {
        // Rule 2: If there is another sidelog to commit, issue the operation
        // to do so.
        if (nextSideLogCommit < MAX_PARALLEL_REPLAY_RPCS) {
            sideLogCommitRpc.construct(&(sideLogs[nextSideLogCommit]),
                    localLocator);
            context->workerManager->handleRpc(sideLogCommitRpc.get());

            LOG(ll, "Issued commit on sidelog %u (Tablet[0x%lx, 0x%lx]"
                    " in table %lu", nextSideLogCommit, startKeyHash,
                    endKeyHash, tableId);
        } // End of Rule 2.

        // Rule 3: If all sidelogs have been committed, change state to
        // TEAR_DOWN.
        else {
            LOG(NOTICE, "All sidelogs have been committed. Changing state"
                " to TEAR_DOWN (Tablet[0x%lx, 0x%lx] in table %lu).",
                startKeyHash, endKeyHash, tableId);

            phase = TEAR_DOWN;
        } // End of Rule 3.
    }

    return workDone;
}

int
RocksteadyMigration::tearDown()
{
    int workDone = 0;

    // TODO: The completion and drop-dependency rpc can be issued together.

    // TODO: Add code to issue a completion rpc to the source here.

    // TODO: Add code to issue a drop-dependency rpc to the coordinator here.

    // TODO: Make sure this change in state takes place only once, and only
    // after the completion and drop-dependency rpcs have returned.
    tabletManager->changeState(tableId, startKeyHash, endKeyHash,
            TabletManager::ROCKSTEADY_MIGRATING, TabletManager::NORMAL);

    phase = COMPLETED;
    workDone++;

    LOG(NOTICE, "Completed migration of tablet[0x%lx, 0x%lx] in table %lu.",
            startKeyHash, endKeyHash, tableId);

    return workDone;
}

}  // namespace RAMCloud
