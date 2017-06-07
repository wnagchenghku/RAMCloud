#ifndef RAMCLOUD_RUNTIMEFRONTEND_H
#define RAMCLOUD_RUNTIMEFRONTEND_H

#include "Util.h"
#include "Buffer.h"

#include "TenantId.h"

namespace RAMCloud {

class RuntimeFrontEnd {
  protected:
    RuntimeFrontEnd();

  public:
    virtual ~RuntimeFrontEnd() {}

    class Binding {
      protected:
        Binding();

      public:
        virtual ~Binding() {}

        virtual bool invoke();

      private:
        Key key;

        TenantId tenantId;

        Buffer* request;

        Buffer* response;

        DISALLOW_COPY_AND_ASSIGN(Binding);
    };

    virtual Binding getBinding(Key key, TenantId tenantId, Buffer* request,
                            Buffer* response);

    virtual void invokeBinding(Binding* binding);

  private:
    //RuntimeBackEnd* backend;
    DISALLOW_COPY_AND_ASSIGN(RuntimeFrontEnd);
};

} // namespace

#endif // RAMCLOUD_RUNTIMEFRONTEND_H
