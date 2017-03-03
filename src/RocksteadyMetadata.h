#ifndef RAMCLOUD_ROCKSTEADYMETADATA_H
#define RAMCLOUD_ROCKSTEADYMETADATA_H

#include "LogMetadata.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

class RocksteadySegmentCertificate {
  public:
    explicit RocksteadySegmentCertificate()
        : checksum()
    {}

    ~RocksteadySegmentCertificate()
    {}

    void
    updateChecksum(LogEntryType type, uint32_t length)
    {
        // TODO: Update checksum here.
        ;
    }

    void
    createSegmentCertificate(uint64_t length, SegmentCertificate* certificate)
    {
        // TODO: Update checksum and certificate here.
        ;
    }

  PRIVATE:
    Crc32C checksum;

    DISALLOW_COPY_AND_ASSIGN(RocksteadySegmentCertificate);
};

}

#endif // RAMCLOUD_ROCKSTEADYMETADATA_H
