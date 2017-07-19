#ifndef RAMCLOUD_RUNTIMEBACKEND_H
#define RAMCLOUD_RUNTIMEBACKEND_H

#include <memory>

#include "Key.h"
#include "Util.h"
#include "SpinLock.h"

#include "TenantId.h"

namespace RAMCloud {

/**
 * An interface that allows runtime state to be shared between core-local
 * frontends.
 *
 * Implementations manage shared runtime state between core-local frontends.
 * This state could include containers that help safely isolate and execute
 * untrusted code inside RAMCloud. A container can be requested for by calling
 * getContainer(). It can later be returned by calling freeContainer().
 */
class RuntimeBackEnd {
  protected:
    /**
     * Constructor for RuntimeBackEnd.
     */
    RuntimeBackEnd()
        : backendLock("backendLock")
    {}

  public:
    /**
     * Destructor for RuntimeBackEnd.
     */
    virtual ~RuntimeBackEnd() {}

    /**
     * An interface that allows for isolation of untrusted code.
     *
     * Implementations provide a sandbox/container within which untrusted code
     * can be safely isolated and executed inside RAMCloud.
     */
    class Container {
      protected:
        /**
         * Constructor for Container.
         */
        Container(uint64_t containerId)
            : containerId(containerId)
        {}

      public:
        /**
         * Destructor for Container.
         */
        virtual ~Container() {}

      private:
        /// Unique identifier for the container.
        uint64_t containerId;

        DISALLOW_COPY_AND_ASSIGN(Container);
    };

    /**
     * Get a container/sandbox within which an extension/procedure can be
     * invoked.
     *
     * \param cookie
     *      An opaque argument to be passed in.
     *
     * \return
     *      An std::weak_ptr to a container within which a procedure can be
     *      invoked or installed.
     */
    virtual std::weak_ptr<Container> getContainer(void* cookie) = 0;

    /**
     * Return a container to the backend.
     *
     * \param container
     *      An std::weak_ptr to a container that was previously obtained
     *      through a call to getContainer().
     * \param cookie
     *      An opaque argument to be passed in.
     */
    virtual void freeContainer(std::weak_ptr<Container> container,
                               void* cookie) = 0;

    /// A spinlock required to access the backend.
    SpinLock backendLock;

  private:
    DISALLOW_COPY_AND_ASSIGN(RuntimeBackEnd);
};

} // namespace

#endif // RAMCLOUD_RUNTIMEBACKEND_H
