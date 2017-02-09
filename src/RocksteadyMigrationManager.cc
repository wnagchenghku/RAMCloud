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
    , prepareSourceRpc()
    , pullRpcs()
    , replayTasks()
    , pullBuffers()
    , sideLogs()
    , partitions()
{
    objectManager = &((context->getMasterService())->objectManager);

    // Construct all the sidelogs.
    for (uint32_t i = 0; i < MAX_PARALLEL_PULL_RPCS * PARTITION_PIPELINE_DEPTH;
        i++) {
        sideLogs[i].construct(objectManager->getLog());
    }
}

int
RocksteadyMigration::poll()
{
    return 0;
}

int
RocksteadyMigration::prepare()
{
    return 0;
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
