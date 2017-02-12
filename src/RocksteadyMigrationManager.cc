#include <new>

#include "OptionParser.h"
#include "MasterService.h"
#include "RocksteadyMigrationManager.h"

namespace RAMCloud {

RocksteadyMigrationManager::RocksteadyMigrationManager(Context* context)
    : Dispatch::Poller(context->dispatch, "RocksteadyMigrationManager")
    , context(context)
    , localLocator(context->options->getLocalLocator())
    , migrationsInProgress()
{
    ;
}

RocksteadyMigrationManager::~RocksteadyMigrationManager()
{
    // Iterate through the set of in-progress migrations and make sure all
    // of them complete.
    for (auto migration = migrationsInProgress.begin();
        migration != migrationsInProgress.end(); ) {
        RocksteadyMigration* currentMigration = *migration;

        while (currentMigration->phase != RocksteadyMigration::COMPLETED) {
            currentMigration->poll();
        }

        migration = migrationsInProgress.erase(migration);
        delete currentMigration;
    }
}

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

bool
RocksteadyMigrationManager::startMigration(ServerId sourceServerId,
                            uint64_t tableId, uint64_t startKeyHash,
                            uint64_t endKeyHash)
{
    // First check if we have another in-progress migration from the
    // same source.
    for (auto& migration : migrationsInProgress) {
        if (migration->sourceServerId == sourceServerId) {
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
    , pullRpcs()
    , freePullRpcs()
    , busyPullRpcs()
    , replayRpcs()
    , freeReplayRpcs()
    , busyReplayRpcs()
    , sideLogs()
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
            for (uint32_t i = 0; i < MAX_PARALLEL_PULL_RPCS; i++) {
                uint64_t partitionStartHTBucket =
                        i * (sourceNumHTBuckets / MAX_PARALLEL_PULL_RPCS);
                uint64_t partitionEndHTBucket =
                        ((i + 1) * (sourceNumHTBuckets /
                        MAX_PARALLEL_PULL_RPCS)) - 1;

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
    return 0;
}

int
RocksteadyMigration::tearDown()
{
    return 0;
}

}  // namespace RAMCloud
