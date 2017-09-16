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
            SegmentCertificate certificate;

            totalPulledBytes +=
                    service->objectManager.rocksteadyMigrationPullHashes(
                    tableId, 0UL, ~0UL, currentHTBucket, currentHTBucketEntry,
                    endHTBucket, 0, 10 * 1024, &respBuffer, &nextHTBucket,
                    &nextHTBucketEntry, &certificate);

            currentHTBucket = nextHTBucket;
            currentHTBucketEntry = nextHTBucketEntry;
        }

        *ret = totalPulledBytes;
        numDone++;

        return;
    }

    void
    run(size_t numThreads, uint32_t numObjects, uint32_t objectSize)
    {
        service->tabletManager.addTablet(99, 0UL, ~0UL, TabletManager::NORMAL);

        // Populate the table with data.
        for (uint32_t i = 0; i < numObjects; i++) {
            char primaryKey[30];
            snprintf(primaryKey, 30, "p%0*d", 28, i);

            Key key(99, primaryKey, 30);

            char value[10240];
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

        // Generate output on stdout.
        fprintf(stdout, "%zu threads and %u Byte objects:\n", numThreads,
                objectSize);
        fprintf(stdout, "Pulled: %.2f GB\n", totalPulledBytesGB);
        fprintf(stdout, "Time: %.2f sec\n", completionTimeSecs);
        fprintf(stdout, "Throughput: %.2f MB/sec\n", (totalPulledBytesGB *
                1024) / completionTimeSecs);

        return;
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
main(int argc, char* argv[])
{
    long int numRuns = 10;
    std::vector<size_t> numThreads= { 1, 2, 4, 8, 16 };
    // Approximately 3.48 GB of data including log entry headers.
    std::vector<uint32_t> numObjects = { 30400000, 20000000, 11900000, 6550000,
            3450000, 1775000, 453000 };
    std::vector<uint32_t> objectSize = { 64, 128, 256, 512, 1024, 2048, 8192 };

    int arg;
    while ((arg = getopt(argc, argv, "t:s:r:")) != -1) {
        switch (arg) {
        // Number of threads specified on the command line.
        case 't':
            numThreads.clear();
            numThreads.emplace_back(atol(optarg));
            break;

        // Object size specified on the command line.
        case 's':
            objectSize.clear();
            objectSize.emplace_back(atol(optarg));
            numObjects.clear();
            numObjects.emplace_back((3.48 * 1024 * 1024 * 1024) /
                    (30 + 29 + objectSize[0]));
            break;

        // Number of runs specified on the command line.
        case 'r':
            numRuns = atol(optarg);
            break;
        }
    }

    // Run all tests.
    for (long int run = 0; run < numRuns; run++) {
        for (auto nThreads : numThreads) {
            for (uint32_t i = 0; i < numObjects.size(); i++) {
                fprintf(stdout, "============================================"
                        "======\n");
                RAMCloud::RocksteadyPullHashesBenchmark rphb("8192", "10%");
                rphb.run(nThreads, numObjects[i], objectSize[i]);
            }
        }
    }

    return 0;
}
