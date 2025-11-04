#pragma once

// Forcibly disable NDEBUG
#ifdef NDEBUG
#undef NDEBUG
#endif

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <torch/types.h>
#include <tuple>
#include <vector>
#include <string>

#include <memory>
#include "config.hpp"
#include "event.hpp"
#include "kernels/configs.cuh"
#include "kernels/exception.cuh"

#include "nixl.h"

#define EP_EXECUTE_ONCE(func) do { static bool _ = ((func), true); } while(0)

#ifndef TORCH_EXTENSION_NAME
#define TORCH_EXTENSION_NAME nixl_ep_cpp
#endif

namespace nixl_ep {

#define MAX_IP_LENGTH 16
#define MAX_BOOT_ID_LENGTH 37

struct NixlPeerInfo {
    char ip[MAX_IP_LENGTH];
    char boot_id[MAX_BOOT_ID_LENGTH];
    ino_t ipc_namespace_inode;
    void *rdma_buffer_ptr;
    uint64_t *counters_buffer_ptr;
    uint64_t *wireup_ptr;
    cudaIpcMemHandle_t rdma_ipc_handle;
    cudaIpcMemHandle_t counters_ipc_handle;
    int device_id;
    int rank;
};

struct NixlAgentInfo
{
    NixlAgentInfo(std::shared_ptr<nixlAgent> agent, nixlBackendH* backend, int max_num_ranks): agent(agent), backend(backend) {
        wire_up_done.resize(max_num_ranks, false);
    }

    std::shared_ptr<nixlAgent> agent;
    std::string agent_name;
    nixl_opt_args_t extra_params;
    nixlBackendH* backend;
    std::vector<bool> wire_up_done; // [num_peers]
};

struct nixl_low_latency_ctx {
    std::vector<nixlXferReqH *> cpu_remote_counter_reqs_0; // [dest_expert_id,remote_rank], cpu ptrs to nixlXferReqH
    std::vector<nixlXferReqH *> cpu_remote_counter_reqs_1; // [dest_expert_id,remote_rank], cpu ptrs to nixlXferReqH
    std::vector<nixlGpuXferReqH> gpu_remote_counter_reqs_0; // [dest_expert_id,remote_rank], gpu ptrs to nixlGpuXferReqH
    std::vector<nixlGpuXferReqH> gpu_remote_counter_reqs_1; // [dest_expert_id,remote_rank], gpu ptrs to nixlGpuXferReqH
    std::vector<std::vector<nixlXferReqH*>> cpu_batch_reqs; // [num_local_experts][num_peers]
    std::vector<std::vector<nixlGpuXferReqH>> gpu_batch_reqs; // [num_local_experts][num_peers]
    std::vector<nixlXferReqH *> cpu_sync_counters;
    std::vector<nixlGpuXferReqH> gpu_sync_counters;
    std::vector<void *> rdma_p2p_ptrs; // [num_ranks]
    std::vector<uint64_t *> counters_p2p_ptrs; // [num_ranks]
    internode_ll::gpu_nixl_ctx nixl_ctx[2]; // Double buffering
};

struct Buffer {
private:
    // Low-latency mode buffer
    int low_latency_buffer_idx = 0;

    // RDMA Buffer
    int64_t num_rdma_bytes;
    void* rdma_buffer_ptr = nullptr;

    // Shrink mode buffer
    bool enable_shrink = false;
    int *mask_buffer_ptr = nullptr;
    int *sync_buffer_ptr = nullptr;

    // Device info and communication
    int device_id;
    int num_device_sms;
    int rank;
    int num_ranks;
    std::vector<int> remote_ranks; /* global ranks */

    // Stream for communication
    at::cuda::CUDAStream comm_stream;

    // After synchronization, this flag will be true
    bool available = false;

    // Whether explicit `destroy()` is required.
    bool explicitly_destroy;
    // After `destroy()` be called, this flag will be true
    bool destroyed = false;

    // Workspace
    void* workspace = nullptr;

    std::unique_ptr<NixlAgentInfo> nixl_agent_info;
    std::vector<NixlPeerInfo> nixl_peer_info;
    uint64_t *counters_buffer_ptr = nullptr;
    uint64_t *wireup_buffer_ptr = nullptr;
    NixlPeerInfo my_peer_info;
    uint64_t num_counters;
    uint64_t max_num_ranks;
    int env_num_channels;
    nixl_xfer_dlist_t dummy_src_dlist; // TODO: Remove once NIXL supports null src dlist for signals
    std::unique_ptr<nixl_low_latency_ctx> low_latency_ctx = nullptr;

    /* Common private funcs */
    void _nixl_agent_init();
    void _nixl_agents_connect(const std::vector<int>& ranks);
    void _nixl_agents_disconnect(const std::vector<int>& ranks);
    void _nixl_agents_peer_info_gather(std::vector<int>& ranks);
    void _nixl_agents_peer_info_cleanup(const std::vector<int>& ranks);
    void _nixl_agents_wireup(std::vector<int>& ranks);

    /* Low-latency mode private funcs */
    void _nixl_ll_init(const std::vector<int>& ranks_to_setup);
    void _nixl_ll_context_init();
    void _nixl_ll_counters_prepare(const std::vector<int>& ranks_to_setup);
    void _nixl_ll_batches_prepare(const std::vector<int>& ranks_to_setup);
    void _nixl_ll_p2p_ptrs_prepare(const std::vector<int>& ranks_to_setup);
    void _nixl_ll_gpu_ctx_update();
    
    /* Low-latency mode cleanup funcs */
    void _nixl_ll_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ll_counters_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ll_batches_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ll_p2p_ptrs_cleanup(const std::vector<int>& ranks_to_remove);

public:
    Buffer(int rank, bool explicitly_destroy, bool enable_shrink);

    void update_memory_buffers(int num_ranks, int64_t num_rdma_bytes);

    void connect_ranks(const std::vector<int>& remote_ranks_list);

    void remove_ranks(const std::vector<int>& remote_ranks_list);

    void init(int num_ranks, int64_t num_rdma_bytes);

    ~Buffer() noexcept(false);

    bool is_available() const;

    int get_local_device_id() const;

    torch::Tensor get_local_buffer_tensor(const pybind11::object& dtype, int64_t offset) const;

    torch::Stream get_comm_stream() const;

    void destroy();

    void clean_low_latency_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts);

    std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, torch::Tensor, torch::Tensor, std::optional<EventHandle>, std::optional<std::function<void()>>>
    low_latency_dispatch(const torch::Tensor& x, const torch::Tensor& topk_idx,
                         const std::optional<torch::Tensor>& cumulative_local_expert_recv_stats,
                         const std::optional<torch::Tensor>& dispatch_wait_recv_cost_stats,
                         int num_max_dispatch_tokens_per_rank, int num_experts,
                         bool use_fp8, bool round_scale, bool use_ue8m0,
                         bool async, bool return_recv_hook);

    std::tuple<torch::Tensor, std::optional<EventHandle>, std::optional<std::function<void()>>>
    low_latency_combine(const torch::Tensor& x, const torch::Tensor& topk_idx, const torch::Tensor& topk_weights,
                        const torch::Tensor& src_info, const torch::Tensor& layout_range,
                        const std::optional<torch::Tensor>& combine_wait_recv_cost_stats,
                        int num_max_dispatch_tokens_per_rank, int num_experts,
                        bool use_logfmt, bool zero_copy, bool async, bool return_recv_hook,
                        const std::optional<torch::Tensor>& out = std::nullopt);

    void low_latency_sync();

    torch::Tensor
    get_next_low_latency_combine_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) const;

    void low_latency_update_mask_buffer(int rank_to_mask, bool mask);

    void low_latency_query_mask_buffer(const torch::Tensor& mask_status);

    void low_latency_clean_mask_buffer();
};

} // namespace nixl_ep
