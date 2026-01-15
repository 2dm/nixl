#include <vector>
#include <cstring>

#include "configs.cuh"
#include "exception.cuh"
#include "launch.cuh"
#include "utils.cuh"

#include <cuda_runtime.h>

//TODO Micha: Move to internode.cu?

namespace nixl_ep {


namespace internode {

void* alloc(size_t size, size_t alignment) {
    void *ptr;
    CUDA_CHECK(cudaMalloc(&ptr, size));
    return ptr;
}

void free(void* ptr) {
    CUDA_CHECK(cudaFree(ptr));
}

} // namespace internode

} // namespace nixl_ep
