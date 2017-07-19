#ifndef RAMCLOUD_NATIVEBACKEND_H
#define RAMCLOUD_NATIVEBACKEND_H

#include <unordered_map>

#include "Key.h"
#include "Buffer.h"

#include "RuntimeBackEnd.h"

namespace RAMCloud {

class NativeBackEnd : public RuntimeBackEnd {
  public:
    NativeBackEnd()
        : procedureMap()
    {}
    ~NativeBackEnd() {}

    std::weak_ptr<Container>
    getContainer(void* cookie)
    {
        return std::weak_ptr<Container>();
    }

    void
    freeContainer(std::weak_ptr<Container> container, void* cookie)
    {
        ;
    }

    void putProcedure(Key& key, TenantId tenantId, void* procedure);
    void* getProcedure(Key& key, TenantId tenantId);

  private:
    /// Map to store virtual addresses of procedures dynamically linked into
    /// RAMCloud by the native frontend.
    std::unordered_map<std::string, void*> procedureMap;

    DISALLOW_COPY_AND_ASSIGN(NativeBackEnd);
};

} // namespace

#endif // RAMCLOUD_NATIVEBACKEND_H
