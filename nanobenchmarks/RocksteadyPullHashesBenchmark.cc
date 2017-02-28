#include "Util.h"
#include "MasterService.h"

namespace RAMCloud {

/**
 * A class that benchmarks the performance of
 * ObjectManager::rocksteadyMigrationPullHashes().
 */
class RocksteadyPullHashesBenchmark {
  public:
    RocksteadyPullHashesBenchmark(
        string logSize, string hashTableSize)
        : context()
        , config(ServerConfig::forTesting())
        , serverList(&context)
        , service(NULL)
        , startMigration(false)
        , numReady(0)
        , numDone(0)
        , pulledBytes(0)
    {
        Logger::get().setLogLevels(WARNING);
        config.localLocator = "rocksteady";
        config.coordinatorLocator = "rocksteady";
        config.setLogAndHashTableSize(logSize, hashTableSize);
        config.services = {WireFormat::MASTER_SERVICE};
        config.master.numReplicas = 0;
        config.segmentSize = Segment::DEFAULT_SEGMENT_SIZE;
        config.segletSize = Seglet::DEFAULT_SEGLET_SIZE;
        service = new MasterService(&context, &config);
        service->setServerId({1, 0});
    }

    ~RocksteadyPullHashesBenchmark()
    {
        delete service;
    }

    void
    migrateHTPartition(uint64_t tableId, uint64_t startHTBucket,
            uint64_t endHTBucket, uint64_t* ret, size_t threadId)
    {
        // Pin the thread.
        Util::pinThreadToCore(static_cast<int>(threadId));

        uint64_t totalPulledBytes = 0;
        uint64_t currentHTBucket = startHTBucket;
        uint64_t currentHTBucketEntry = 0;

        numReady++;

        while (!startMigration);

        // Pull all the data.
        while (currentHTBucket <= endHTBucket) {
            uint64_t nextHTBucket;
            uint64_t nextHTBucketEntry;
            Buffer respBuffer;

            totalPulledBytes +=
                    service->objectManager.rocksteadyMigrationPullHashes(
                    tableId, 0UL, ~0UL, currentHTBucket, currentHTBucketEntry,
                    endHTBucket, 0, 10 * 1024, &respBuffer, &nextHTBucket,
                    &nextHTBucketEntry);

            currentHTBucket = nextHTBucket;
            currentHTBucketEntry = nextHTBucketEntry;
        }

        *ret = totalPulledBytes;
        numDone++;

        return;
    }

    double
    run(size_t numThreads, uint32_t numObjects, uint32_t objectSize)
    {
        service->tabletManager.addTablet(99, 0UL, ~0UL, TabletManager::NORMAL);

        // Populate the table with data.
        for (uint32_t i = 0; i < numObjects; i++) {
            char primaryKey[30];
            snprintf(primaryKey, 30, "p%0*d", 28, i);

            Key key(99, primaryKey, 30);

            char value[1024];
            snprintf(value, objectSize, "v%0*d", objectSize - 2, i);

            Buffer buffer;
            Object::appendKeysAndValueToBuffer(key, value, objectSize, &buffer);
            Object object(99, 0, 0, buffer);

            service->objectManager.writeObject(object, NULL, NULL);
        }

        uint64_t numHTBuckets = service->objectManager.getNumHashTableBuckets();

        // Each thread works on a unique partition of the master's hash table.
        std::deque<std::thread> threads{};
        pulledBytes.resize(numThreads);
        for (size_t i = 0; i < numThreads; i++) {
            threads.emplace_back(
                    &RocksteadyPullHashesBenchmark::migrateHTPartition,
                    this, 99, i * (numHTBuckets / numThreads),
                    ((i + 1) * (numHTBuckets / numThreads)) - 1,
                    &(pulledBytes[i]), i);
        }

        while (numReady < numThreads) {
            usleep(100);
        }
        startMigration = true;

        // Timestamp at which all threads were ready.
        uint64_t migrationStartTS = Cycles::rdtsc();

        while (numDone < numThreads) {
            usleep(10000);
        }

        // Timestamp at which all threads completed pulling data from their
        // respective partitions.
        uint64_t migrationStopTS = Cycles::rdtsc();

        for (auto& thread : threads) {
            thread.join();
        }

        uint64_t totalPulledBytes = 0;
        for (size_t i = 0; i < numThreads; i++) {
            totalPulledBytes += pulledBytes[i];
        }

        double totalPulledBytesGB = static_cast<double>(totalPulledBytes) /
                (1024 * 1024 * 1024);
        double completionTimeSecs = Cycles::toSeconds(migrationStopTS -
                migrationStartTS);

        // Generate output on stderr (purely for visual purposes).
        fprintf(stderr, "==================================================\n");
        fprintf(stderr, "%zu threads and %u Byte objects:\n", numThreads,
                objectSize);
        fprintf(stderr, "Pulled: %.2f GB\n", totalPulledBytesGB);
        fprintf(stderr, "Time: %.2f sec\n", completionTimeSecs);
        fprintf(stderr, "Throughput: %.2f GB/sec\n", totalPulledBytesGB /
                completionTimeSecs);

        return (totalPulledBytesGB / completionTimeSecs);
    }

    Context context;
    ServerConfig config;
    ServerList serverList;
    MasterService *service;
    std::atomic<bool> startMigration;
    std::atomic<size_t> numReady;
    std::atomic<size_t> numDone;
    std::vector<uint64_t> pulledBytes; // The number of bytes pulled by each
                                       // thread.

    DISALLOW_COPY_AND_ASSIGN(RocksteadyPullHashesBenchmark);
};

} // namespace RAMCloud

int
main(void)
{
    size_t numThreads[] = { 1, 2, 4, 8 };
    // Approximately 3.48 GB of data including log entry headers.
    uint32_t numObjects[] = { 30400000, 20000000, 11900000, 6550000, 3450000 };
    uint32_t objectSize[] = { 64, 128, 256, 512, 1024 };
    std::vector<double> pullThroughput;

    // First run all tests and save the throughput obtained in each run.
    for (auto nThreads : numThreads) {
        for (uint32_t i = 0; i < sizeof(numObjects) / sizeof(uint32_t); i++) {
            RAMCloud::RocksteadyPullHashesBenchmark rphb("5120", "10%");
            pullThroughput.push_back(
                    rphb.run(nThreads, numObjects[i], objectSize[i]));
        }
    }

    // Generate output (for gnuplot/R).
    uint32_t j = 0;
    fprintf(stdout, "numThreads objectSize throughput\n");
    for (auto nThreads : numThreads) {
        for (uint32_t i = 0; i < sizeof(numObjects) / sizeof(uint32_t); i++) {
            fprintf(stdout, "%zd %u %.2f\n", nThreads, objectSize[i],
                    pullThroughput[j++]);
        }
    }

    return 0;
}
