#ifndef RAMCLOUD_RUNTIMEBACKEND_H
#define RAMCLOUD_RUNTIMEBACKEND_H

namespace RAMCloud {

template<class T> class RuntimeBackEnd {
  public:
    RuntimeBackEnd();
    ~RuntimeBackEnd();

    T* getContainer(Key key, TenantId tenantId);

    void returnContainer(T*, TenantId tenantId);

  private:
    std::vector<T> containerPool;
    DISALLOW_COPY_AND_ASSIGN(RuntimeBackEnd);
};

} // namespace

#endif // RAMCLOUD_RUNTIMEBACKEND_H
