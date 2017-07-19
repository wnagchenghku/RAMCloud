#ifndef RAMCLOUD_NATIVEFRONTEND_H
#define RAMCLOUD_NATIVEFRONTEND_H

#include "Key.h"

#include "NativeBackEnd.h"
#include "RuntimeBackEnd.h"
#include "RuntimeFrontEnd.h"

namespace RAMCloud {

class NativeFrontEnd : public RuntimeFrontEnd {
  public:
    NativeFrontEnd(int workerId, std::shared_ptr<RuntimeBackEnd> backend)
        : RuntimeFrontEnd(workerId, backend)
    {}

    ~NativeFrontEnd() {};

    void installProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                          Buffer* procedure, Buffer* binary);
    void invokeProcedure(uint64_t tableId, Key& key, TenantId tenantId,
                         Buffer* binary, Buffer* clientResponse);

  private:
    DISALLOW_COPY_AND_ASSIGN(NativeFrontEnd);
};

} // namespace

#endif // RAMCLOUD_NATIVEFRONTEND_H
