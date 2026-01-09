/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 DeepSeek
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * This file incorporates material from the DeepSeek project, licensed under the MIT License.
 * The modifications made by NVIDIA are licensed under the Apache License, Version 2.0.
 *
 * SPDX-License-Identifier: MIT AND Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    int* sync_buffer_ptr;
    uint64_t* barrier_ptr;  // For internode barrier
    int device_id;
    int rank;
};

struct NixlAgentInfo
{
    NixlAgentInfo(std::shared_ptr<nixlAgent> agent, nixlBackendH* backend, int max_num_ranks): agent(agent), backend(backend) {
        wire_up_done.resize(max_num_ranks, false);
        remote_agent_names.resize(max_num_ranks);
    }

    std::shared_ptr<nixlAgent> agent;
    std::string agent_name;
    std::vector<std::string> remote_agent_names;
    nixl_opt_args_t extra_params;
    nixlBackendH* backend;
    std::vector<bool> wire_up_done; // [num_peers]
};

struct nixl_ep_ctx {
    std::vector<nixlXferReqH *> cpu_remote_counter_reqs_0; // [dest_expert_id,remote_rank], cpu ptrs to nixlXferReqH
    std::vector<nixlXferReqH *> cpu_remote_counter_reqs_1; // [dest_expert_id,remote_rank], cpu ptrs to nixlXferReqH
    std::vector<nixlGpuXferReqH> gpu_remote_counter_reqs_0; // [dest_expert_id,remote_rank], gpu ptrs to nixlGpuXferReqH
    std::vector<nixlGpuXferReqH> gpu_remote_counter_reqs_1; // [dest_expert_id,remote_rank], gpu ptrs to nixlGpuXferReqH
    std::vector<nixlXferReqH*> cpu_batch_reqs; // [num_peers]
    std::vector<nixlGpuXferReqH> gpu_batch_reqs; // [num_peers]
    std::vector<nixlXferReqH*> cpu_barrier_reqs; // [num_peers]
    std::vector<nixlGpuXferReqH> gpu_barrier_reqs; // [num_peers]
    std::vector<void *> rdma_p2p_ptrs; // [num_ranks]
    std::vector<uint64_t *> counters_p2p_ptrs; // [num_ranks]
    ep_kernels::gpu_ep_ctx gpu_ep_ctx[2]; // Double buffering

    ~nixl_ep_ctx() noexcept(false) {
        // Free GPU memory allocated in _nixl_ep_gpu_ctx_update
        if (gpu_ep_ctx[0].remote_counter_reqs) CUDA_CHECK(cudaFree(gpu_ep_ctx[0].remote_counter_reqs));
        if (gpu_ep_ctx[1].remote_counter_reqs) CUDA_CHECK(cudaFree(gpu_ep_ctx[1].remote_counter_reqs));
        if (gpu_ep_ctx[0].batch_reqs) CUDA_CHECK(cudaFree(gpu_ep_ctx[0].batch_reqs));
        // gpu_ep_ctx[1].batch_reqs shares pointer with [0], don't double-free
        if (gpu_ep_ctx[0].remote_barrier_reqs) CUDA_CHECK(cudaFree(gpu_ep_ctx[0].remote_barrier_reqs));
        // gpu_ep_ctx[1].remote_barrier_reqs shares pointer with [0], don't double-free
        if (gpu_ep_ctx[0].counters_p2p_ptrs) CUDA_CHECK(cudaFree(gpu_ep_ctx[0].counters_p2p_ptrs));
        if (gpu_ep_ctx[1].counters_p2p_ptrs) CUDA_CHECK(cudaFree(gpu_ep_ctx[1].counters_p2p_ptrs));
        if (gpu_ep_ctx[0].rdma_p2p_ptrs) CUDA_CHECK(cudaFree(gpu_ep_ctx[0].rdma_p2p_ptrs));
        if (gpu_ep_ctx[1].rdma_p2p_ptrs) CUDA_CHECK(cudaFree(gpu_ep_ctx[1].rdma_p2p_ptrs));
    }
};

/// @brief nixl_internode_ctx manages the GPU context for high-throughput internode communication
class nixl_internode_ctx {
public:
    internode::gpu_internode_ctx gpu_internode_ctx;
    
    // CPU-side storage for handles that will be copied to GPU arrays
    std::vector<nixlXferReqH*> cpu_data_request_reqs;           // [num_rdma_ranks]
    std::vector<nixlGpuXferReqH> cpu_data_request_handles;      // [num_rdma_ranks]
    std::vector<nixlXferReqH*> cpu_head_counter_reqs;           // [num_rdma_ranks]
    std::vector<nixlGpuXferReqH> cpu_head_counter_handles;      // [num_rdma_ranks]
    std::vector<nixlXferReqH*> cpu_barrier_reqs;                // [num_rdma_ranks]
    std::vector<nixlGpuXferReqH> cpu_barrier_handles;           // [num_rdma_ranks]
    
    int num_rdma_ranks = 0;
    int num_channels = 0;
    int rank = 0;

    nixl_internode_ctx(int num_channels = 0, int num_rdma_ranks = 0, int rank = 0)
        : num_channels(num_channels), num_rdma_ranks(num_rdma_ranks), rank(rank) {
        // Initialize CPU-side vectors
        cpu_data_request_reqs.resize(num_rdma_ranks, nullptr);
        cpu_data_request_handles.resize(num_rdma_ranks, nullptr);
        cpu_head_counter_reqs.resize(num_rdma_ranks, nullptr);
        cpu_head_counter_handles.resize(num_rdma_ranks, nullptr);
        cpu_barrier_reqs.resize(num_rdma_ranks, nullptr);
        cpu_barrier_handles.resize(num_rdma_ranks, nullptr);

        // Allocate GPU arrays for handles
        CUDA_CHECK(cudaMalloc(&gpu_internode_ctx.data_request_handles, sizeof(nixlGpuXferReqH) * num_rdma_ranks));
        CUDA_CHECK(cudaMalloc(&gpu_internode_ctx.remote_head_counter_handles, sizeof(nixlGpuXferReqH) * num_rdma_ranks));
        CUDA_CHECK(cudaMalloc(&gpu_internode_ctx.remote_barrier_handles, sizeof(nixlGpuXferReqH) * num_rdma_ranks));
        
        // Initialize to zeros
        CUDA_CHECK(cudaMemset(gpu_internode_ctx.data_request_handles, 0, sizeof(nixlGpuXferReqH) * num_rdma_ranks));
        CUDA_CHECK(cudaMemset(gpu_internode_ctx.remote_head_counter_handles, 0, sizeof(nixlGpuXferReqH) * num_rdma_ranks));
        CUDA_CHECK(cudaMemset(gpu_internode_ctx.remote_barrier_handles, 0, sizeof(nixlGpuXferReqH) * num_rdma_ranks));

        // Note: local_head_counters, local_tail_counters, last_barrier_counter, and 
        // local_barrier_counter_ptr are set in _nixl_internode_local_data_init() to point
        // into the existing counters_buffer_ptr (not allocated here)
        gpu_internode_ctx.local_head_counters = nullptr;
        gpu_internode_ctx.local_tail_counters = nullptr;
        gpu_internode_ctx.last_barrier_counter = nullptr;
        gpu_internode_ctx.local_barrier_counter_ptr = nullptr;

        gpu_internode_ctx.num_channels = num_channels;
        gpu_internode_ctx.num_rdma_ranks = num_rdma_ranks;
        gpu_internode_ctx.rank = rank;
    }

    ~nixl_internode_ctx() noexcept(false) {
        // Only free GPU memory that was allocated by this class (handles only)
        // Counter pointers point into counters_buffer_ptr which is managed elsewhere
        if (gpu_internode_ctx.data_request_handles) CUDA_CHECK(cudaFree(gpu_internode_ctx.data_request_handles));
        if (gpu_internode_ctx.remote_head_counter_handles) CUDA_CHECK(cudaFree(gpu_internode_ctx.remote_head_counter_handles));
        if (gpu_internode_ctx.remote_barrier_handles) CUDA_CHECK(cudaFree(gpu_internode_ctx.remote_barrier_handles));
    }

    void copy_to_gpu() {
        // Copy handles from CPU vectors to GPU arrays
        CUDA_CHECK(cudaMemcpy(gpu_internode_ctx.data_request_handles, cpu_data_request_handles.data(),
                              sizeof(nixlGpuXferReqH) * num_rdma_ranks, cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(gpu_internode_ctx.remote_head_counter_handles, cpu_head_counter_handles.data(),
                              sizeof(nixlGpuXferReqH) * num_rdma_ranks, cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(gpu_internode_ctx.remote_barrier_handles, cpu_barrier_handles.data(),
                              sizeof(nixlGpuXferReqH) * num_rdma_ranks, cudaMemcpyHostToDevice));
    }
};

struct Buffer {
    EP_STATIC_ASSERT(NUM_MAX_NVL_PEERS == 8, "The number of maximum NVLink peers must be 8");

private:
    // Low-latency mode buffer
    int buffer_idx = 0;
    bool low_latency_mode = false;

    // NVLink Buffer
    int64_t num_nvl_bytes = 0;
    void* buffer_ptrs[NUM_MAX_NVL_PEERS] = {nullptr};
    void** buffer_ptrs_gpu = nullptr;
    cudaIpcMemHandle_t ipc_handles[NUM_MAX_NVL_PEERS];

    // RDMA Buffer
    int64_t num_rdma_bytes;
    void* rdma_buffer_ptr = nullptr;

    // Barrier signals (for high-throughput internode)
    int* barrier_signal_ptrs[NUM_MAX_NVL_PEERS] = {nullptr};
    int** barrier_signal_ptrs_gpu = nullptr;

    // Shrink mode buffer
    bool enable_shrink = false;
    int *mask_buffer_ptr = nullptr;
    int *sync_buffer_ptr = nullptr;

    // Device info and communication
    int device_id;
    int num_device_sms;
    int rank, rdma_rank = 0, nvl_rank = 0;
    int num_ranks, num_rdma_ranks = 1, num_nvl_ranks = 1;
    std::vector<int> remote_ranks; /* global ranks */

    // Stream for communication
    at::cuda::CUDAStream comm_stream;

    // Host-side MoE counters (for high-throughput internode)
    volatile int* moe_recv_counter = nullptr;
    int* moe_recv_counter_mapped = nullptr;
    volatile int* moe_recv_expert_counter = nullptr;
    int* moe_recv_expert_counter_mapped = nullptr;
    volatile int* moe_recv_rdma_counter = nullptr;
    int* moe_recv_rdma_counter_mapped = nullptr;

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
    uint64_t* last_barrier_counter = nullptr;
    uint64_t* local_barrier_counter = nullptr;
    nixl_xfer_dlist_t dummy_src_dlist; // TODO: Remove once NIXL supports null src dlist for signals

    std::unique_ptr<nixl_internode_ctx> internode_ctx = nullptr;
    std::unique_ptr<nixl_ep_ctx> ep_ctx = nullptr;

    /* Common private funcs */
    void _nixl_agent_init();
    void _nixl_agents_connect(const std::vector<int>& ranks);
    void _nixl_agents_disconnect(const std::vector<int>& ranks);
    void _nixl_agents_peer_info_gather(std::vector<int>& ranks);
    void _nixl_agents_peer_info_cleanup(const std::vector<int>& ranks);
    void _nixl_agents_wireup(std::vector<int>& ranks);

    /* NIXL EP (low-latency mode) private funcs */
    void _nixl_ep_init(const std::vector<int>& ranks);
    void _nixl_ep_context_init();
    void _nixl_ep_clear_sync_buffer();
    void _nixl_ep_counters_prepare(const std::vector<int>& ranks);
    void _nixl_ep_batches_prepare(const std::vector<int>& ranks);
    void _nixl_ep_p2p_ptrs_prepare(const std::vector<int>& ranks);
    void _nixl_ep_gpu_ctx_update();

    /* NIXL EP cleanup funcs */
    void _nixl_ep_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ep_counters_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ep_batches_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ep_p2p_ptrs_cleanup(const std::vector<int>& ranks_to_remove);
    void _nixl_ep_barrier_buffer_clear(int rank);

    /* Internode mode private funcs */
    void _nixl_internode_init();
    void _nixl_internode_local_data_init();
    void _nixl_remote_counters_prepare();
    void _nixl_internode_batches_prepare();
    void _nixl_internode_barrier_prepare();

public:
    Buffer(int rank, bool low_latency_mode, bool explicitly_destroy, bool enable_shrink);

    void update_memory_buffers(int num_ranks, int64_t num_nvl_bytes, int64_t num_rdma_bytes);

    void connect_ranks(const std::vector<int>& remote_ranks_list);

    void disconnect_ranks(const std::vector<int>& remote_ranks_list);

    void init(int num_ranks, int64_t num_nvl_bytes, int64_t num_rdma_bytes);
    
    pybind11::bytearray get_local_ipc_handle() const;

    ~Buffer() noexcept(false);

    bool is_available() const;

    int get_local_device_id() const;

    torch::Tensor get_local_buffer_tensor(const pybind11::object& dtype, int64_t offset) const;

    torch::Stream get_comm_stream() const;

    void destroy();

    void clean_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts);

    std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, torch::Tensor, torch::Tensor, std::optional<EventHandle>, std::optional<std::function<void()>>>
    dispatch(const torch::Tensor& x, const torch::Tensor& topk_idx,
                         const std::optional<torch::Tensor>& cumulative_local_expert_recv_stats,
                         const std::optional<torch::Tensor>& dispatch_wait_recv_cost_stats,
                         int num_max_dispatch_tokens_per_rank, int num_experts,
                         bool use_fp8, bool round_scale, bool use_ue8m0,
                         bool async, bool return_recv_hook);

    std::tuple<torch::Tensor, std::optional<EventHandle>, std::optional<std::function<void()>>>
    combine(const torch::Tensor& x, const torch::Tensor& topk_idx, const torch::Tensor& topk_weights,
                        const torch::Tensor& src_info, const torch::Tensor& layout_range,
                        const std::optional<torch::Tensor>& combine_wait_recv_cost_stats,
                        int num_max_dispatch_tokens_per_rank, int num_experts,
                        bool use_logfmt, bool zero_copy, bool async, bool return_recv_hook,
                        const std::optional<torch::Tensor>& out = std::nullopt);

    void barrier();

    torch::Tensor
    get_next_combine_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) const;

    void update_mask_buffer(int rank_to_mask, bool mask);

    void query_mask_buffer(const torch::Tensor& mask_status);

    void clean_mask_buffer();

    void clean_low_latency_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts);

    // Get the number of RDMA ranks (for determining internode vs intranode mode)
    int get_num_rdma_ranks() const;

    // Get dispatch layout for high-throughput mode
    std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, torch::Tensor, std::optional<EventHandle>>
    get_dispatch_layout(const torch::Tensor& topk_idx, int num_experts,
                        const std::optional<EventHandle>& previous_event,
                        bool async_finish, bool allocate_on_comm_stream);

    // High-throughput internode dispatch
    std::tuple<torch::Tensor, std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::vector<int>,
               torch::Tensor, torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
               std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::optional<EventHandle>>
    internode_dispatch(const torch::Tensor& x, const std::optional<torch::Tensor>& x_scales,
                       const std::optional<torch::Tensor>& topk_idx, const std::optional<torch::Tensor>& topk_weights,
                       const std::optional<torch::Tensor>& num_tokens_per_rank, const std::optional<torch::Tensor>& num_tokens_per_rdma_rank,
                       const torch::Tensor& is_token_in_rank, const std::optional<torch::Tensor>& num_tokens_per_expert,
                       int cached_num_recv_tokens, int cached_num_rdma_recv_tokens,
                       const std::optional<torch::Tensor>& cached_rdma_channel_prefix_matrix, const std::optional<torch::Tensor>& cached_recv_rdma_rank_prefix_sum,
                       const std::optional<torch::Tensor>& cached_gbl_channel_prefix_matrix, const std::optional<torch::Tensor>& cached_recv_gbl_rank_prefix_sum,
                       int expert_alignment, const Config& config, std::optional<EventHandle>& previous_event, bool async, bool allocate_on_comm_stream);

    // High-throughput internode combine
    std::tuple<torch::Tensor, std::optional<torch::Tensor>, std::optional<EventHandle>>
    internode_combine(const torch::Tensor& x, const std::optional<torch::Tensor>& topk_weights,
                      const std::optional<torch::Tensor>& bias_0, const std::optional<torch::Tensor>& bias_1,
                      const torch::Tensor& src_meta, const torch::Tensor& is_combined_token_in_rank,
                      const torch::Tensor& rdma_channel_prefix_matrix, const torch::Tensor& rdma_rank_prefix_sum, const torch::Tensor& gbl_channel_prefix_matrix,
                      const torch::Tensor& combined_rdma_head, const torch::Tensor& combined_nvl_head,
                      const Config& config, std::optional<EventHandle>& previous_event, bool async, bool allocate_on_comm_stream);
};

} // namespace nixl_ep
