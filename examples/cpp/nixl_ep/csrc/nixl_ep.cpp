/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 DeepSeek
 * SPDX-License-Identifier: MIT
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDADataType.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <cuda_runtime.h>
#include <memory>
#include <pybind11/functional.h>
#include <torch/python.h>

#include "nixl_ep.hpp"
#include "kernels/api.cuh"
#include "kernels/configs.cuh"
#include <cstdio>
#include <fstream>
#include <unistd.h>
#include <stdio.h>
#include "nixl.h"
#include <ifaddrs.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <sstream>

#define NIXL_ETCD_WATCH_TIMEOUT std::chrono::microseconds(1000000000) // 1000 seconds

#ifdef ENABLE_DEBUG_LOGS
#define HOST_LOG_DEBUG(fmt, ...) printf("[DEBUG] " fmt "\n", ##__VA_ARGS__)
#else
#define HOST_LOG_DEBUG(...)
#endif

namespace nixl_ep {

static void sleep_ms(int milliseconds) {
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}

static std::string _get_local_ip() {
    struct ifaddrs *ifaddr, *ifa;
    char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return "127.0.0.1";
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL || ifa->ifa_addr->sa_family != AF_INET)
            continue;

        if ((ifa->ifa_flags & IFF_UP) && !(ifa->ifa_flags & IFF_LOOPBACK)) {
            if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST) == 0) {
                freeifaddrs(ifaddr);
                return std::string(host);
            }
        }
    }

    freeifaddrs(ifaddr);
    return "127.0.0.1";
}

static std::string boot_id_get() {
    std::ifstream boot_id_file("/proc/sys/kernel/random/boot_id");
    if (!boot_id_file.is_open()) {
        return "";
    }

    std::string boot_id;
    std::getline(boot_id_file, boot_id);

    if (!boot_id.empty() && boot_id.back() == '\n') {
        boot_id.pop_back();
    }

    return boot_id;
}

static ino_t ipc_namespace_inode_get() {
    struct stat st;
    if (stat("/proc/self/ns/ipc", &st) != 0) {
        return 0;
    }
    return st.st_ino;
}


void Buffer::update_memory_buffers(int num_ranks, int64_t num_nvl_bytes, int64_t num_rdma_bytes)
{
    if (!available) {
        init(num_ranks, num_nvl_bytes, num_rdma_bytes);
        available = true;
    } else {
        throw std::runtime_error("Multiple calls to update_memory_buffers are not supported");
    }
}

Buffer::Buffer(int rank, bool explicitly_destroy, bool enable_shrink):
        rank(rank), num_ranks(1),
        explicitly_destroy(explicitly_destroy),
        comm_stream(at::cuda::getStreamFromPool(true)),
        dummy_src_dlist(VRAM_SEG),
        enable_shrink(enable_shrink) {}

void Buffer::init(int num_ranks, int64_t num_nvl_bytes, int64_t num_rdma_bytes)
{
    // Update buffer attributes
    this->max_num_ranks = num_ranks;
    this->num_nvl_bytes = num_nvl_bytes;
    this->num_rdma_bytes = num_rdma_bytes;

    // Common checks
    EP_STATIC_ASSERT(NUM_BUFFER_ALIGNMENT_BYTES % sizeof(int4) == 0, "Invalid alignment");
    EP_HOST_ASSERT(num_rdma_bytes % NUM_BUFFER_ALIGNMENT_BYTES == 0);
    EP_HOST_ASSERT(num_rdma_bytes / sizeof(int4) < std::numeric_limits<int>::max());
    EP_HOST_ASSERT(0 <= rank and rank < num_ranks);

    // Get ranks
    CUDA_CHECK(cudaGetDevice(&device_id));
    // Get device info
    cudaDeviceProp device_prop = {};
    CUDA_CHECK(cudaGetDeviceProperties(&device_prop, device_id));
    num_device_sms = device_prop.multiProcessorCount;
    int denom_sms = std::max(1, num_device_sms / 2);
    auto per_channel_bytes = ceil_div<int64_t>(num_rdma_bytes, denom_sms);
    EP_HOST_ASSERT(per_channel_bytes < std::numeric_limits<int>::max());

    // Create 32 MiB workspace
    CUDA_CHECK(cudaMalloc(&workspace, NUM_WORKSPACE_BYTES));
    CUDA_CHECK(cudaMemsetAsync(workspace, 0, NUM_WORKSPACE_BYTES, comm_stream));

    env_num_channels = std::getenv("NIXL_EP_NUM_CHANNELS") ? std::stoi(std::getenv("NIXL_EP_NUM_CHANNELS")) : 1;
    EP_HOST_ASSERT(env_num_channels > 0);
    num_counters = env_num_channels * max_num_ranks * 2 + max_num_ranks;
    CUDA_CHECK(cudaMalloc(&rdma_buffer_ptr, num_rdma_bytes));
    CUDA_CHECK(cudaMemset(rdma_buffer_ptr, 0, num_rdma_bytes));
    CUDA_CHECK(cudaMalloc(&counters_buffer_ptr, num_counters * sizeof(uint64_t)));
    CUDA_CHECK(cudaMemset(counters_buffer_ptr, 0, num_counters * sizeof(uint64_t)));
    CUDA_CHECK(cudaMalloc(&wireup_buffer_ptr, sizeof(uint64_t)));
    CUDA_CHECK(cudaMemset(wireup_buffer_ptr, 0, sizeof(uint64_t)));

    /* Initialize dummy src dlist with a dummy device address */
    dummy_src_dlist.addDesc(nixlBlobDesc((uintptr_t)counters_buffer_ptr, sizeof(uint64_t), device_id, ""));
    // Allocate and clean shrink buffer
    mask_buffer_ptr = nullptr;
    sync_buffer_ptr = nullptr;
    if (enable_shrink) {
        int num_mask_buffer_bytes = max_num_ranks * sizeof(int);
        CUDA_CHECK(cudaMalloc(&mask_buffer_ptr, num_mask_buffer_bytes));
        CUDA_CHECK(cudaMemset(mask_buffer_ptr, 0xff, num_mask_buffer_bytes));
        CUDA_CHECK(cudaMemset(mask_buffer_ptr + rank, 0, sizeof(int)));
    }
    int num_sync_buffer_bytes = max_num_ranks * sizeof(int);
    CUDA_CHECK(cudaMalloc(&sync_buffer_ptr, num_sync_buffer_bytes));
    CUDA_CHECK(cudaMemset(sync_buffer_ptr, 0, num_sync_buffer_bytes));
    CUDA_CHECK(cudaDeviceSynchronize());

    strncpy(my_peer_info.ip, _get_local_ip().c_str(), MAX_IP_LENGTH - 1);
    my_peer_info.ip[MAX_IP_LENGTH - 1] = '\0';
    my_peer_info.rdma_buffer_ptr = rdma_buffer_ptr;
    my_peer_info.counters_buffer_ptr = counters_buffer_ptr;
    my_peer_info.wireup_ptr = wireup_buffer_ptr;
    my_peer_info.device_id = get_local_device_id();
    my_peer_info.sync_buffer_ptr = sync_buffer_ptr;
    my_peer_info.rank = rank;

    // Create IPC handles for rdma buffer and counters
    CUDA_CHECK(cudaIpcGetMemHandle(&my_peer_info.rdma_ipc_handle, rdma_buffer_ptr));
    CUDA_CHECK(cudaIpcGetMemHandle(&my_peer_info.counters_ipc_handle, counters_buffer_ptr));

    strncpy(my_peer_info.boot_id, boot_id_get().c_str(), MAX_BOOT_ID_LENGTH - 1);
    my_peer_info.boot_id[MAX_BOOT_ID_LENGTH - 1] = '\0';
    my_peer_info.ipc_namespace_inode = ipc_namespace_inode_get();

    nixl_peer_info.resize(max_num_ranks);
    nixl_peer_info[rank] = my_peer_info;

    _nixl_agent_init();

    _nixl_ep_init(std::vector<int>{rank});
}

Buffer::~Buffer() noexcept(false) {
    if (not explicitly_destroy) {
        destroy();
    } else if (not destroyed) {
        printf("WARNING: destroy() was not called before NIXL_EP buffer destruction, which can leak resources.\n");
        fflush(stdout);
    }
}

bool Buffer::is_available() const {
    return available;
}

int Buffer::get_local_device_id() const {
    return device_id;
}

torch::Tensor Buffer::get_local_buffer_tensor(const pybind11::object& dtype, int64_t offset, bool use_rdma_buffer) const {
    torch::ScalarType casted_dtype = torch::python::detail::py_object_to_dtype(dtype);
    auto element_bytes = static_cast<int64_t>(elementSize(casted_dtype));
    auto base_ptr = static_cast<uint8_t*>(use_rdma_buffer ? rdma_buffer_ptr : buffer_ptrs[nvl_rank]) + offset;
    auto num_bytes = use_rdma_buffer ? num_rdma_bytes : num_nvl_bytes;
    return torch::from_blob(base_ptr, num_bytes / element_bytes, torch::TensorOptions().dtype(casted_dtype).device(at::kCUDA));
}

torch::Stream Buffer::get_comm_stream() const {
    return comm_stream;
}

void Buffer::destroy() {
    EP_HOST_ASSERT(not destroyed);

    // Synchronize
    CUDA_CHECK(cudaDeviceSynchronize());

    cudaFree(counters_buffer_ptr);
    cudaFree(wireup_buffer_ptr);
    cudaFree(rdma_buffer_ptr);

    if (nixl_agent_info and nixl_agent_info->agent != nullptr) {
        nixl_agent_info->agent->invalidateLocalMD();
    }

    rdma_buffer_ptr = nullptr;
    counters_buffer_ptr = nullptr;

    if (enable_shrink) {
        cudaFree(mask_buffer_ptr);
        cudaFree(sync_buffer_ptr);
    }

    // Free workspace
    CUDA_CHECK(cudaFree(workspace));

    destroyed = true;
    available = false;
}

void Buffer::barrier() {
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    ep_kernels::barrier(nixl_ctx->gpu[0],mask_buffer_ptr, sync_buffer_ptr, compute_stream);
}

void Buffer::_nixl_agents_connect(const std::vector<int>& ranks) {
    EP_HOST_ASSERT(!ranks.empty());

    // Assuming ranks vector does not include current rank and has only new ranks
    remote_ranks.insert(remote_ranks.end(), ranks.begin(), ranks.end());
    for (int remote_rank : ranks) {
        nixl_agent_info->remote_agent_names[remote_rank] = std::to_string(remote_rank);
    }

    for (int remote_rank : ranks) {
        nixl_status_t fetch_status = nixl_agent_info->agent->fetchRemoteMD(nixl_agent_info->remote_agent_names[remote_rank]);
        if (fetch_status != NIXL_SUCCESS) {
            throw std::runtime_error("Failed to fetch metadata for remote agent " + std::to_string(remote_rank) +
                                    ", status: " + std::to_string(fetch_status));
        }

        // Wait for remote metadata to be available
        nixl_xfer_dlist_t empty_descs(VRAM_SEG);
        while (nixl_agent_info->agent->checkRemoteMD(std::to_string(remote_rank), empty_descs) != NIXL_SUCCESS) {
            sleep_ms(10);
        }
    }
}

void Buffer::_nixl_agents_peer_info_gather(std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        std::string my_peer_info_str(reinterpret_cast<const char*>(&my_peer_info), sizeof(NixlPeerInfo));
        nixl_agent_info->agent->genNotif(std::to_string(remote_rank), my_peer_info_str);
    }

    for (int remote_rank : ranks) {
        do {
            nixl_notifs_t notif_map;
            nixl_agent_info->agent->getNotifs(notif_map);
            for (auto &notif : notif_map) {
                std::string my_peer_info_str = notif.second[0];
                NixlPeerInfo remote_peer_info;
                memcpy(&remote_peer_info, my_peer_info_str.c_str(), sizeof(NixlPeerInfo));
                nixl_peer_info[remote_peer_info.rank] = remote_peer_info;
                nixl_agent_info->wire_up_done[remote_peer_info.rank] = true;
            }
        } while (!nixl_agent_info->wire_up_done[remote_rank]);
    }
}

// This is a workaround to NIXL/UCX wireup issue and should be removed once it is fixed
void Buffer::_nixl_agents_wireup(std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        nixl_opt_args_t wireup_params = {};
        wireup_params.backends.push_back(nixl_agent_info->backend);

        nixlXferReqH *wireup_req = nullptr;
        nixl_xfer_dlist_t dummy_dst_dlist(VRAM_SEG);
        dummy_dst_dlist.addDesc(nixlBlobDesc((uintptr_t)nixl_peer_info[remote_rank].wireup_ptr, sizeof(uint64_t), nixl_peer_info[remote_rank].device_id, ""));
        EP_HOST_ASSERT(nixl_agent_info->agent->createXferReq(
            NIXL_WRITE, dummy_src_dlist, dummy_dst_dlist,
            nixl_agent_info->remote_agent_names[remote_rank], wireup_req, &wireup_params) == NIXL_SUCCESS);

        nixl_status_t status = nixl_agent_info->agent->postXferReq(wireup_req);
        EP_HOST_ASSERT(status == NIXL_SUCCESS || status == NIXL_IN_PROG);

        while ((status = nixl_agent_info->agent->getXferStatus(wireup_req)) == NIXL_IN_PROG) {
            sleep_ms(1);
        }

        EP_HOST_ASSERT(status == NIXL_SUCCESS);
        EP_HOST_ASSERT(nixl_agent_info->agent->releaseXferReq(wireup_req) == NIXL_SUCCESS);
    }
}

void Buffer::_nixl_ep_barrier_buffer_clear(int rank) {
    CUDA_CHECK(cudaMemset(sync_buffer_ptr + rank, 0, sizeof(int)));
}

void Buffer::connect_ranks(const std::vector<int>& remote_ranks_list) {
    EP_HOST_ASSERT(!remote_ranks_list.empty());
    std::vector<int> new_ranks;
    int max_added_rank = std::max(rank, *std::max_element(remote_ranks_list.begin(), remote_ranks_list.end()));
    num_ranks = std::max(num_ranks, max_added_rank + 1);

    for (int remote_rank : remote_ranks_list) {
        // Skip self and ranks we are already connected to
        if (remote_rank == rank or std::find(remote_ranks.begin(), remote_ranks.end(), remote_rank) != remote_ranks.end())
            continue;

        new_ranks.push_back(remote_rank);
        if (enable_shrink) {
            _nixl_ep_barrier_buffer_clear(remote_rank);
        }
    }

    if (new_ranks.empty())
        return;
    _nixl_agents_connect(new_ranks);

    _nixl_agents_peer_info_gather(new_ranks);

    _nixl_agents_wireup(new_ranks);

    _nixl_ep_init(new_ranks);

    CUDA_CHECK(cudaDeviceSynchronize());

    // Ready to use
    available = true;
}

void Buffer::disconnect_ranks(const std::vector<int>& remote_ranks_list) {
    EP_HOST_ASSERT(!remote_ranks_list.empty());
    EP_HOST_ASSERT(remote_ranks_list.size() <= remote_ranks.size());
    
    CUDA_CHECK(cudaDeviceSynchronize());
    
    // Update mask buffer to mark ranks as inactive
    if (enable_shrink) {
        for (int removed_rank : remote_ranks_list) {
            update_mask_buffer(removed_rank, true);  // mask=true
        }
    }
    _nixl_ep_cleanup(remote_ranks_list);

    _nixl_agents_peer_info_cleanup(remote_ranks_list);

    _nixl_agents_disconnect(remote_ranks_list);

    // Remove ranks from remote_ranks vector (arbitrary order)
    for (int removed_rank : remote_ranks_list) {
        remote_ranks.erase(
            std::remove(remote_ranks.begin(), remote_ranks.end(), removed_rank),
            remote_ranks.end()
        );
    }
    
    int max_rank = rank;  // Include self
    if (!remote_ranks.empty()) {
        max_rank = std::max(max_rank, 
                           *std::max_element(remote_ranks.begin(), remote_ranks.end()));
    }
    num_ranks = max_rank + 1;  // Sparse indexing maintained
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, torch::Tensor, torch::Tensor, std::optional<EventHandle>, std::optional<std::function<void()>>>
Buffer::low_latency_dispatch(const torch::Tensor& x, const torch::Tensor& topk_idx,
                             const std::optional<torch::Tensor>& cumulative_local_expert_recv_stats,
                             const std::optional<torch::Tensor>& dispatch_wait_recv_cost_stats,
                             int num_max_dispatch_tokens_per_rank, int num_experts,
                             bool use_fp8, bool round_scale, bool use_ue8m0,
                             bool async, bool return_recv_hook) {
    // Tensor checks
    // By default using `ptp128c` FP8 cast
    EP_HOST_ASSERT(x.dim() == 2 and x.is_contiguous() and x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(1) % sizeof(int4) == 0 and x.size(1) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(x.size(0) == topk_idx.size(0) and x.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_idx.scalar_type() == c10::CppTypeToScalarType<topk_idx_t>::value);
    EP_HOST_ASSERT(num_experts % num_ranks == 0);

    // Diagnosis tensors
    if (cumulative_local_expert_recv_stats.has_value()) {
        EP_HOST_ASSERT(cumulative_local_expert_recv_stats->scalar_type() == torch::kInt);
        EP_HOST_ASSERT(cumulative_local_expert_recv_stats->dim() == 1 and cumulative_local_expert_recv_stats->is_contiguous());
        EP_HOST_ASSERT(cumulative_local_expert_recv_stats->size(0) == num_experts / num_ranks);
    }
    if (dispatch_wait_recv_cost_stats.has_value()) {
        EP_HOST_ASSERT(dispatch_wait_recv_cost_stats->scalar_type() == torch::kInt64);
        EP_HOST_ASSERT(dispatch_wait_recv_cost_stats->dim() == 1 and dispatch_wait_recv_cost_stats->is_contiguous());
        EP_HOST_ASSERT(dispatch_wait_recv_cost_stats->size(0) == num_ranks);
    }

    auto num_tokens = static_cast<int>(x.size(0)), hidden = static_cast<int>(x.size(1));
    auto num_topk = static_cast<int>(topk_idx.size(1));
    int num_local_experts = num_experts / num_ranks;

    // Buffer control
    LowLatencyLayout layout(rdma_buffer_ptr, num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_rdma_bytes);
    ep_kernels::gpu_nixl_ctx gpu_nixl_ctx = nixl_ctx->gpu[buffer_idx];
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not (async and return_recv_hook));
    if (not return_recv_hook)
        stream_wait(launch_stream, compute_stream);

    // Allocate packed tensors
    auto packed_recv_x = torch::empty({num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank, hidden},
                                      x.options().dtype(use_fp8 ? torch::kFloat8_e4m3fn: torch::kBFloat16));
    auto packed_recv_src_info = torch::empty({num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank}, torch::dtype(torch::kInt32).device(torch::kCUDA));
    auto packed_recv_layout_range = torch::empty({num_local_experts, num_ranks}, torch::dtype(torch::kInt64).device(torch::kCUDA));
    auto packed_recv_count = torch::empty({num_local_experts}, torch::dtype(torch::kInt32).device(torch::kCUDA));

    // Allocate column-majored scales
    auto packed_recv_x_scales = std::optional<torch::Tensor>();
    void* packed_recv_x_scales_ptr = nullptr;
    EP_HOST_ASSERT((num_ranks * num_max_dispatch_tokens_per_rank) % 4 == 0 and "TMA requires the number of tokens to be multiple of 4");

    if (use_fp8) {
        // TODO: support unaligned cases
        EP_HOST_ASSERT(hidden % 512 == 0);
        if (not use_ue8m0) {
            packed_recv_x_scales = torch::empty({num_local_experts, hidden / 128, num_ranks * num_max_dispatch_tokens_per_rank},
                                                torch::dtype(torch::kFloat32).device(torch::kCUDA));
        } else {
            EP_HOST_ASSERT(round_scale);
            packed_recv_x_scales = torch::empty({num_local_experts, hidden / 512, num_ranks * num_max_dispatch_tokens_per_rank},
                                                torch::dtype(torch::kInt).device(torch::kCUDA));
        }
        packed_recv_x_scales = torch::transpose(packed_recv_x_scales.value(), 1, 2);
        packed_recv_x_scales_ptr = packed_recv_x_scales->data_ptr();
    }

    // Kernel launch
    auto next_clean_meta = next_buffer.clean_meta();
    auto launcher = [=](int phases) {
        ep_kernels::dispatch(packed_recv_x.data_ptr(), packed_recv_x_scales_ptr,
                               packed_recv_src_info.data_ptr<int>(), packed_recv_layout_range.data_ptr<int64_t>(),
                               packed_recv_count.data_ptr<int>(),
                               mask_buffer_ptr,
                               cumulative_local_expert_recv_stats.has_value() ? cumulative_local_expert_recv_stats->data_ptr<int>() : nullptr,
                               dispatch_wait_recv_cost_stats.has_value() ? dispatch_wait_recv_cost_stats->data_ptr<int64_t>() : nullptr,
                               buffer.dispatch_rdma_recv_data_buffer, buffer.dispatch_rdma_recv_count_buffer,
                               buffer.dispatch_rdma_send_buffer,
                              x.data_ptr(), topk_idx.data_ptr<topk_idx_t>(),
                               next_clean_meta.first, next_clean_meta.second,
                               num_tokens, hidden, num_max_dispatch_tokens_per_rank,
                               num_topk, num_experts, rank, num_ranks,
                               use_fp8, round_scale, use_ue8m0,
                               workspace, num_device_sms,
                               launch_stream, phases, gpu_nixl_ctx);
    };
    launcher(return_recv_hook ? EP_SEND_PHASE : (EP_SEND_PHASE | EP_RECV_PHASE));

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated before the stream-wait happens,
        // so in Python API, we must wrap all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() { launcher(EP_RECV_PHASE); };

    // Return values
    return {packed_recv_x, packed_recv_x_scales, packed_recv_count, packed_recv_src_info, packed_recv_layout_range, event, recv_hook};
}

std::tuple<torch::Tensor, std::optional<EventHandle>, std::optional<std::function<void()>>>
Buffer::low_latency_combine(const torch::Tensor& x, const torch::Tensor& topk_idx, const torch::Tensor& topk_weights,
                            const torch::Tensor& src_info, const torch::Tensor& layout_range,
                            const std::optional<torch::Tensor>& combine_wait_recv_cost_stats,
                            int num_max_dispatch_tokens_per_rank, int num_experts,
                            bool use_logfmt, bool zero_copy, bool async, bool return_recv_hook,
                            const std::optional<torch::Tensor>& out) {
    // Tensor checks
    EP_HOST_ASSERT(x.dim() == 3 and x.is_contiguous() and x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(0) == num_experts / num_ranks);
    EP_HOST_ASSERT(x.size(1) == num_ranks * num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(x.size(2) % sizeof(int4) == 0 and x.size(2) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(topk_idx.size(0) == topk_weights.size(0) and topk_idx.size(1) == topk_weights.size(1));
    EP_HOST_ASSERT(topk_idx.scalar_type() == c10::CppTypeToScalarType<topk_idx_t>::value);
    EP_HOST_ASSERT(topk_weights.dim() == 2 and topk_weights.is_contiguous());
    EP_HOST_ASSERT(topk_weights.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_weights.scalar_type() == torch::kFloat32);
    EP_HOST_ASSERT(src_info.dim() == 2 and src_info.is_contiguous());
    EP_HOST_ASSERT(src_info.scalar_type() == torch::kInt32 and x.size(0) == src_info.size(0));
    EP_HOST_ASSERT(layout_range.dim() == 2 and layout_range.is_contiguous());
    EP_HOST_ASSERT(layout_range.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(layout_range.size(0) == num_experts / num_ranks and layout_range.size(1) == num_ranks);

    if (combine_wait_recv_cost_stats.has_value()) {
        EP_HOST_ASSERT(combine_wait_recv_cost_stats->scalar_type() == torch::kInt64);
        EP_HOST_ASSERT(combine_wait_recv_cost_stats->dim() == 1 and combine_wait_recv_cost_stats->is_contiguous());
        EP_HOST_ASSERT(combine_wait_recv_cost_stats->size(0) == num_ranks);
    }

    auto hidden = static_cast<int>(x.size(2));
    auto num_topk = static_cast<int>(topk_weights.size(1));
    auto num_combined_tokens = static_cast<int>(topk_weights.size(0));

    // Buffer control
    LowLatencyLayout layout(rdma_buffer_ptr, num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_rdma_bytes);
    ep_kernels::gpu_nixl_ctx gpu_nixl_ctx = nixl_ctx->gpu[buffer_idx];
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not (async and return_recv_hook));
    if (not return_recv_hook)
        stream_wait(launch_stream, compute_stream);

    // Allocate output tensor
    torch::Tensor combined_x;
    if (out.has_value()) {
        EP_HOST_ASSERT(out->dim() == 2 and out->is_contiguous());
        EP_HOST_ASSERT(out->size(0) == num_combined_tokens and out->size(1) == hidden);
        EP_HOST_ASSERT(out->scalar_type() == x.scalar_type());
        combined_x = out.value();
    } else {
        combined_x = torch::empty({num_combined_tokens, hidden}, x.options());
    }

    // Kernel launch
    auto next_clean_meta = next_buffer.clean_meta();
    auto launcher = [=](int phases) {
        ep_kernels::combine(combined_x.data_ptr(),
                              buffer.combine_rdma_recv_data_buffer, buffer.combine_rdma_recv_flag_buffer,
                              buffer.combine_rdma_send_buffer,
                              x.data_ptr(), topk_idx.data_ptr<topk_idx_t>(), topk_weights.data_ptr<float>(),
                              src_info.data_ptr<int>(), layout_range.data_ptr<int64_t>(),
                              mask_buffer_ptr,
                              combine_wait_recv_cost_stats.has_value() ? combine_wait_recv_cost_stats->data_ptr<int64_t>() : nullptr,
                              next_clean_meta.first, next_clean_meta.second,
                              num_combined_tokens, hidden, num_max_dispatch_tokens_per_rank,
                              num_topk, num_experts, rank, num_ranks,
                              use_logfmt,
                              workspace, num_device_sms,
                              launch_stream, phases, zero_copy, gpu_nixl_ctx);
    };
    launcher(return_recv_hook ? EP_SEND_PHASE : (EP_SEND_PHASE | EP_RECV_PHASE));

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated before the stream-wait happens,
        // so in Python API, we must wrap all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() { launcher(EP_RECV_PHASE); };

    // Return values
    return {combined_x, event, recv_hook};
}

torch::Tensor
Buffer::get_next_low_latency_combine_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) const {
    LowLatencyLayout layout(rdma_buffer_ptr, num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts);

    auto buffer = layout.buffers[buffer_idx];
    auto dtype = torch::kBFloat16;
    auto num_msg_elems = static_cast<int>(buffer.num_bytes_per_combine_msg / elementSize(torch::kBFloat16));

    EP_HOST_ASSERT(buffer.num_bytes_per_combine_msg % elementSize(torch::kBFloat16) == 0);
    return torch::from_blob(buffer.combine_rdma_send_buffer_data_start,
                            {num_experts / num_ranks, num_ranks * num_max_dispatch_tokens_per_rank, hidden},
                            {num_ranks * num_max_dispatch_tokens_per_rank * num_msg_elems, num_msg_elems, 1},
                            torch::TensorOptions().dtype(dtype).device(torch::kCUDA));
}

bool is_sm90_compiled() {
#ifndef DISABLE_SM90_FEATURES
    return true;
#else
    return false;
#endif
}

void Buffer::update_mask_buffer(int rank_to_mask, bool mask) {
    EP_HOST_ASSERT(mask_buffer_ptr != nullptr and "Shrink mode must be enabled");
    EP_HOST_ASSERT(rank_to_mask >= 0 and rank_to_mask < max_num_ranks);
    ep_kernels::update_mask_buffer(mask_buffer_ptr, rank_to_mask, mask, at::cuda::getCurrentCUDAStream());
}

void Buffer::query_mask_buffer(const torch::Tensor& mask_status) {
    EP_HOST_ASSERT(mask_buffer_ptr != nullptr and "Shrink mode must be enabled");
    EP_HOST_ASSERT(mask_status.numel() == max_num_ranks && mask_status.scalar_type() == torch::kInt32);

    ep_kernels::query_mask_buffer(mask_buffer_ptr, max_num_ranks,
                                    reinterpret_cast<int*>(mask_status.data_ptr()),
                                    at::cuda::getCurrentCUDAStream());
}

void Buffer::clean_mask_buffer() {
    EP_HOST_ASSERT(mask_buffer_ptr != nullptr and "Shrink mode must be enabled");
    ep_kernels::clean_mask_buffer(mask_buffer_ptr, max_num_ranks, at::cuda::getCurrentCUDAStream());
}

void Buffer::clean_low_latency_buffer(int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) {
    // Clean the low-latency buffer - required before running low-latency kernels
    // after high-throughput mode has been used
    EP_HOST_ASSERT(nixl_ctx != nullptr && "Low-latency context not initialized");
    
    LowLatencyLayout layout(rdma_buffer_ptr, num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts);
    auto clean_meta_0 = layout.buffers[0].clean_meta();
    auto clean_meta_1 = layout.buffers[1].clean_meta();

    ep_kernels::clean_low_latency_buffer(clean_meta_0.first, clean_meta_0.second,
                                         clean_meta_1.first, clean_meta_1.second,
                                         rank, num_ranks, mask_buffer_ptr, sync_buffer_ptr,
                                         nixl_ctx->gpu[buffer_idx], at::cuda::getCurrentCUDAStream());
}

void Buffer::_nixl_ep_gpu_ctx_update() {
    int num_local_experts = env_num_channels;

    /* Initialize local counter arrays */
    nixl_ctx->gpu[0].local_counters = counters_buffer_ptr;
    nixl_ctx->gpu[1].local_counters = counters_buffer_ptr + max_num_ranks * num_local_experts;

    /* Each context cleans the counters of the other context */
    nixl_ctx->gpu[0].clean_counters = nixl_ctx->gpu[1].local_counters;
    nixl_ctx->gpu[1].clean_counters = nixl_ctx->gpu[0].local_counters;

    /* Copy remote counter reqs to device */
    if (nixl_ctx->gpu[0].remote_counter_reqs == nullptr) {
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[0].remote_counter_reqs, max_num_ranks * sizeof(nixlGpuXferReqH)));
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[1].remote_counter_reqs, max_num_ranks * sizeof(nixlGpuXferReqH)));
    }

    // Always copy the updated arrays to GPU (since new ranks may have been added)
    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[0].remote_counter_reqs, nixl_ctx->gpu_remote_counter_reqs_0.data(), max_num_ranks * sizeof(nixlGpuXferReqH), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[1].remote_counter_reqs, nixl_ctx->gpu_remote_counter_reqs_1.data(), max_num_ranks * sizeof(nixlGpuXferReqH), cudaMemcpyHostToDevice));

    /* Copy batch reqs to device */
    if (nixl_ctx->gpu[0].batch_reqs == nullptr)
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[0].batch_reqs, max_num_ranks * sizeof(nixlGpuXferReqH)));

    // Always copy the updated batch arrays to GPU (since new ranks may have been added)
    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[0].batch_reqs, nixl_ctx->gpu_batch_reqs.data(), max_num_ranks * sizeof(nixlGpuXferReqH), cudaMemcpyHostToDevice));
    nixl_ctx->gpu[1].batch_reqs = nixl_ctx->gpu[0].batch_reqs; // Both contexts share the same batch handles, no need to duplicate them

    if (nixl_ctx->gpu[0].remote_barrier_reqs == nullptr)
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[0].remote_barrier_reqs, max_num_ranks * sizeof(nixlGpuXferReqH*)));

    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[0].remote_barrier_reqs, nixl_ctx->gpu_barrier_reqs.data(), max_num_ranks * sizeof(nixlGpuXferReqH*), cudaMemcpyHostToDevice));
    nixl_ctx->gpu[1].remote_barrier_reqs = nixl_ctx->gpu[0].remote_barrier_reqs; // Both contexts share the same batch handles, no need to duplicate them

    /* Initialize counters P2P pointers */
    if (nixl_ctx->gpu[0].counters_p2p_ptrs == nullptr)
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[0].counters_p2p_ptrs, max_num_ranks * sizeof(uint64_t *)));

    if (nixl_ctx->gpu[1].counters_p2p_ptrs == nullptr)
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[1].counters_p2p_ptrs, max_num_ranks * sizeof(uint64_t *)));

    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[0].counters_p2p_ptrs, nixl_ctx->counters_p2p_ptrs.data(), num_ranks * sizeof(uint64_t *), cudaMemcpyHostToDevice));
    for (int i = 0; i < num_ranks; i++) if (nixl_ctx->counters_p2p_ptrs[i] != 0) nixl_ctx->counters_p2p_ptrs[i] += max_num_ranks * num_local_experts;
    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[1].counters_p2p_ptrs, nixl_ctx->counters_p2p_ptrs.data(), num_ranks * sizeof(uint64_t *), cudaMemcpyHostToDevice));
    for (int i = 0; i < num_ranks; i++) if (nixl_ctx->counters_p2p_ptrs[i] != 0) nixl_ctx->counters_p2p_ptrs[i] -= max_num_ranks * num_local_experts;

    /* Initialize RDMA P2P pointers */
    if (nixl_ctx->gpu[0].rdma_p2p_ptrs == nullptr)
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[0].rdma_p2p_ptrs, max_num_ranks * sizeof(void *)));

    if (nixl_ctx->gpu[1].rdma_p2p_ptrs == nullptr)
        CUDA_CHECK(cudaMalloc(&nixl_ctx->gpu[1].rdma_p2p_ptrs, max_num_ranks * sizeof(void *)));

    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[0].rdma_p2p_ptrs, nixl_ctx->rdma_p2p_ptrs.data(), num_ranks * sizeof(void *), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(nixl_ctx->gpu[1].rdma_p2p_ptrs, nixl_ctx->rdma_p2p_ptrs.data(), num_ranks * sizeof(void *), cudaMemcpyHostToDevice));

    /* Initialize info fields */
    nixl_ctx->gpu[0].rdma_buffer_ptr = rdma_buffer_ptr;
    nixl_ctx->gpu[1].rdma_buffer_ptr = rdma_buffer_ptr;
    nixl_ctx->gpu[0].num_local_experts = num_local_experts;
    nixl_ctx->gpu[1].num_local_experts = num_local_experts;
    nixl_ctx->gpu[0].local_barrier_buffer = sync_buffer_ptr;
    nixl_ctx->gpu[1].local_barrier_buffer = sync_buffer_ptr;
    nixl_ctx->gpu[0].num_ranks = max_num_ranks;
    nixl_ctx->gpu[1].num_ranks = max_num_ranks;
    nixl_ctx->gpu[0].num_channels = env_num_channels;
    nixl_ctx->gpu[1].num_channels = env_num_channels;
    nixl_ctx->gpu[0].rank = rank;
    nixl_ctx->gpu[1].rank = rank;
}

void Buffer::_nixl_ep_context_init() {
    int num_local_experts = env_num_channels;

    nixl_ctx = std::make_unique<nixl_ep_ctx>();
    nixl_ctx->cpu_remote_counter_reqs_0.resize(max_num_ranks);
    nixl_ctx->cpu_remote_counter_reqs_1.resize(max_num_ranks);
    nixl_ctx->gpu_remote_counter_reqs_0.resize(max_num_ranks);
    nixl_ctx->gpu_remote_counter_reqs_1.resize(max_num_ranks);
    nixl_ctx->gpu_batch_reqs.resize(max_num_ranks);
    nixl_ctx->cpu_batch_reqs.resize(max_num_ranks);
    nixl_ctx->cpu_barrier_reqs.resize(max_num_ranks);
    nixl_ctx->gpu_barrier_reqs.resize(max_num_ranks);
    nixl_ctx->rdma_p2p_ptrs.resize(max_num_ranks);
    nixl_ctx->counters_p2p_ptrs.resize(max_num_ranks);
}

void Buffer::_nixl_ep_init(const std::vector<int>& ranks) {
    EP_EXECUTE_ONCE(_nixl_ep_context_init());
    _nixl_ep_counters_prepare(ranks);
    _nixl_ep_batches_prepare(ranks);
    _nixl_ep_p2p_ptrs_prepare(ranks);
    _nixl_ep_gpu_ctx_update();
}

void Buffer::_nixl_internode_init() {
    internode_ctx = std::make_unique<nixl_internode_ctx>(env_num_channels, num_rdma_ranks, rank);
    
    _nixl_internode_local_data_init();
    _nixl_internode_remote_counters_prepare();
    _nixl_internode_batches_prepare();
    internode_ctx->copy_to_gpu();
}

void Buffer::_nixl_internode_local_data_init() {
    // Allocate host-side MoE counters for CPU-GPU synchronization
    // These are used for the CPU busy-wait loop in internode_dispatch
    int num_local_experts = env_num_channels;
    
    // Allocate pinned memory for counters (for fast CPU-GPU sync)
    CUDA_CHECK(cudaMallocHost(&moe_recv_counter, sizeof(int)));
    CUDA_CHECK(cudaMallocHost(&moe_recv_rdma_counter, sizeof(int)));
    CUDA_CHECK(cudaMallocHost(&moe_recv_expert_counter, num_local_experts * sizeof(int)));
    
    // Allocate device-mapped versions
    CUDA_CHECK(cudaMalloc(&moe_recv_counter_mapped, sizeof(int)));
    CUDA_CHECK(cudaMalloc(&moe_recv_rdma_counter_mapped, sizeof(int)));
    CUDA_CHECK(cudaMalloc(&moe_recv_expert_counter_mapped, num_local_experts * sizeof(int)));
    
    // Initialize to -1 (not ready)
    *const_cast<int*>(moe_recv_counter) = -1;
    *const_cast<int*>(moe_recv_rdma_counter) = -1;
    for (int i = 0; i < num_local_experts; ++i) {
        const_cast<int*>(moe_recv_expert_counter)[i] = -1;
    }
    
    // Allocate buffer_ptrs_gpu and barrier_signal_ptrs_gpu
    CUDA_CHECK(cudaMalloc(&buffer_ptrs_gpu, NUM_MAX_NVL_PEERS * sizeof(void*)));
    CUDA_CHECK(cudaMalloc(&barrier_signal_ptrs_gpu, NUM_MAX_NVL_PEERS * sizeof(int*)));
}

void Buffer::_nixl_internode_remote_counters_prepare() {
    // Prepare NIXL transfer requests for remote counter updates
    // This uses a simplified single-handle-per-rank structure (not per-channel like DeepEP)
    if (!internode_ctx) return;
    
    auto agent = nixl_agent_info->agent;
    nixlXferReqH* xfer_req;
    nixlGpuXferReqH gpu_xfer_req;
    
    for (int j = 0; j < num_rdma_ranks; ++j) {
        if (j == rdma_rank) continue;  // Skip self
        
        int remote_global_rank = nvl_rank + j * NUM_MAX_NVL_PEERS;
        
        // Head counter request - point to remote's head counter for channel 0
        // (channel_id is passed dynamically in the NIXL API call)
        uint64_t* remote_head_counter_ptr = nixl_peer_info[remote_global_rank].counters_buffer_ptr + rdma_rank;
        
        nixl_opt_args_t eparams = { .backends = {nixl_agent_info->backend}};
        eparams.customParam = "worker_id=0";  // Default worker
        nixl_xfer_dlist_t dst_dlist(VRAM_SEG);
        dst_dlist.addDesc(nixlBlobDesc((uintptr_t)remote_head_counter_ptr, sizeof(uint64_t), 
                                       nixl_peer_info[remote_global_rank].device_id, ""));
        EP_HOST_ASSERT(agent->createXferReq(NIXL_WRITE, dummy_src_dlist, dst_dlist, 
                       nixl_agent_info->remote_agent_names[remote_global_rank], xfer_req, &eparams) == NIXL_SUCCESS 
                       && "Failed to create counter xfer req");
        internode_ctx->cpu_counter_reqs[j] = xfer_req;
        EP_HOST_ASSERT(agent->createGpuXferReq(*xfer_req, gpu_xfer_req) == NIXL_SUCCESS);
        CUDA_CHECK(cudaMemcpy(&internode_ctx->gpu_ctx.remote_head_counter_handles[j], &gpu_xfer_req, 
                              sizeof(nixlGpuXferReqH), cudaMemcpyHostToDevice));
        
        // Barrier request
        EP_HOST_ASSERT(num_rdma_ranks <= 2 && "Internode barrier is not supported for more than 2 rdma ranks");
        nixlXferReqH* barrier_xfer_req_cpu = nullptr;
        nixlGpuXferReqH barrier_xfer_req_gpu;
        nixl_xfer_dlist_t barrier_dst_dlist(VRAM_SEG);
        barrier_dst_dlist.addDesc(nixlBlobDesc((uintptr_t)nixl_peer_info[remote_global_rank].barrier_ptr, 
                                               sizeof(uint64_t), nixl_peer_info[remote_global_rank].device_id, ""));
        EP_HOST_ASSERT(agent->createXferReq(NIXL_WRITE, dummy_src_dlist, barrier_dst_dlist, 
                       nixl_agent_info->remote_agent_names[remote_global_rank], barrier_xfer_req_cpu, &eparams) == NIXL_SUCCESS);
        internode_ctx->cpu_barrier_reqs[j] = barrier_xfer_req_cpu;
        EP_HOST_ASSERT(agent->createGpuXferReq(*barrier_xfer_req_cpu, barrier_xfer_req_gpu) == NIXL_SUCCESS);
        CUDA_CHECK(cudaMemcpy(&internode_ctx->gpu_ctx.remote_barrier_handles[j], &barrier_xfer_req_gpu, 
                              sizeof(nixlGpuXferReqH), cudaMemcpyHostToDevice));
    }
}

void Buffer::_nixl_internode_batches_prepare() {
    // Prepare NIXL batch transfer requests for data transfers
    // This uses a simplified single-handle-per-rank structure (not per-channel like DeepEP)
    if (!internode_ctx) return;
    
    auto agent = nixl_agent_info->agent;
    
    nixl_xfer_dlist_t src_vram(VRAM_SEG);
    src_vram.addDesc(nixlBlobDesc((uintptr_t)rdma_buffer_ptr, num_rdma_bytes, device_id, ""));
    // TODO: Remove once NIXL supports null src dlist for signals
    src_vram.addDesc(nixlBlobDesc((uintptr_t)counters_buffer_ptr, sizeof(uint64_t), device_id, ""));
    
    for (int j = 0; j < num_rdma_ranks; ++j) {
        if (j == rdma_rank) continue;  // Skip self
        
        int remote_global_rank = nvl_rank + j * NUM_MAX_NVL_PEERS;
        
        nixl_xfer_dlist_t dst_vram(VRAM_SEG);
        dst_vram.addDesc(nixlBlobDesc((uintptr_t)(nixl_peer_info[remote_global_rank].rdma_buffer_ptr), 
                                      num_rdma_bytes, nixl_peer_info[remote_global_rank].device_id, ""));
        // Setting the signal address to the tail counter of the remote rank
        uint64_t* counter_addr = &nixl_peer_info[remote_global_rank].counters_buffer_ptr[num_rdma_ranks + rdma_rank];
        dst_vram.addDesc(nixlBlobDesc((uintptr_t)(counter_addr), sizeof(uint64_t), 
                                      nixl_peer_info[remote_global_rank].device_id, ""));
        
        nixl_opt_args_t extra_params;
        extra_params.backends.push_back(nixl_agent_info->extra_params.backends[0]);
        extra_params.customParam = "worker_id=0";  // Default worker
        
        nixlXferReqH* xfer_req;
        EP_HOST_ASSERT(agent->createXferReq(NIXL_WRITE, src_vram, dst_vram, 
                       nixl_agent_info->remote_agent_names[remote_global_rank], xfer_req, &extra_params) == NIXL_SUCCESS 
                       && "Failed to create xfer req");
        internode_ctx->cpu_data_reqs[j] = xfer_req;
        
        nixlGpuXferReqH gpu_xfer_req;
        EP_HOST_ASSERT(agent->createGpuXferReq(*xfer_req, gpu_xfer_req) == NIXL_SUCCESS);
        CUDA_CHECK(cudaMemcpy(&internode_ctx->gpu_ctx.data_request_handles[j], &gpu_xfer_req, 
                              sizeof(nixlGpuXferReqH), cudaMemcpyHostToDevice));
    }
}


void Buffer::_nixl_agent_init() {
    std::string agent_name = std::to_string(rank);
    nixlAgentConfig cfg(true, false, 0,
                        nixl_thread_sync_t::NIXL_THREAD_SYNC_RW, 1, 0, 100000, false, NIXL_ETCD_WATCH_TIMEOUT);
    auto agent = std::make_shared<nixlAgent>(agent_name, cfg);

    // Create UCX backend
    nixl_mem_list_t mems;
    nixl_b_params_t init_params;

    nixl_status_t status = agent->getPluginParams("UCX", mems, init_params);
    if (status != NIXL_SUCCESS) {
        throw std::runtime_error("Failed to get UCX plugin parameters for agent " + agent_name +
                                ", status: " + std::to_string(status));
    }

    // Set UCX-specific parameters
    init_params["ucx_error_handling_mode"] = "none";
    init_params["num_workers"] = std::to_string(env_num_channels);

    nixlBackendH* ucx_backend = nullptr;
    status = agent->createBackend("UCX", init_params, ucx_backend);
    if (status != NIXL_SUCCESS || !ucx_backend) {
        throw std::runtime_error("Failed to create UCX backend for agent " + agent_name +
                                ", status: " + std::to_string(status));
    }

    nixl_agent_info = std::make_unique<NixlAgentInfo>(agent, ucx_backend, max_num_ranks);
    nixl_agent_info->extra_params.backends.push_back(ucx_backend);
    nixl_agent_info->agent_name = agent_name;

    /* Register RDMA buffer */
    nixl_reg_dlist_t rdma_ptr_dlist(VRAM_SEG);
    rdma_ptr_dlist.addDesc(nixlBlobDesc((uintptr_t)(rdma_buffer_ptr), num_rdma_bytes, get_local_device_id(), ""));
    EP_HOST_ASSERT(agent->registerMem(rdma_ptr_dlist) == NIXL_SUCCESS);

    /* Register counters buffer */
    nixl_reg_dlist_t counters_dlist(VRAM_SEG);
    counters_dlist.addDesc(nixlBlobDesc((uintptr_t)(counters_buffer_ptr), num_counters * sizeof(uint64_t), get_local_device_id(), ""));
    EP_HOST_ASSERT(agent->registerMem(counters_dlist) == NIXL_SUCCESS);

    /* Register sync buffer */
    if (sync_buffer_ptr) {
        nixl_reg_dlist_t sync_dlist(VRAM_SEG);
        sync_dlist.addDesc(nixlBlobDesc((uintptr_t)(sync_buffer_ptr), max_num_ranks * sizeof(int), get_local_device_id(), ""));
        EP_HOST_ASSERT(agent->registerMem(sync_dlist) == NIXL_SUCCESS);
    }

    size_t signal_size = 0;
    EP_HOST_ASSERT(nixl_agent_info->agent->getGpuSignalSize(signal_size, &nixl_agent_info->extra_params) == NIXL_SUCCESS);
    EP_HOST_ASSERT(signal_size == sizeof(uint64_t));
    EP_HOST_ASSERT(agent->prepGpuSignal(counters_dlist, &nixl_agent_info->extra_params) == NIXL_SUCCESS);

    /* Register wireup buffer */
    nixl_reg_dlist_t wireup_dlist(VRAM_SEG);
    wireup_dlist.addDesc(nixlBlobDesc((uintptr_t)(wireup_buffer_ptr), sizeof(uint64_t), get_local_device_id(), ""));
    EP_HOST_ASSERT(agent->registerMem(wireup_dlist) == NIXL_SUCCESS);

    // Send local metadata
    status = nixl_agent_info->agent->sendLocalMD();
    if (status != NIXL_SUCCESS) {
        throw std::runtime_error("Failed to send local metadata for agent " +
                                nixl_agent_info->agent_name + ", status: " + std::to_string(status));
    }
}

void Buffer::_nixl_ep_batches_prepare(const std::vector<int>& ranks) {
    nixl_status_t status;

    for (int j : ranks) {
        if (j == rank) continue; // Skip self
        if (nixl_ctx->gpu_batch_reqs[j]) continue; // Skip if already exported
        nixl_xfer_dlist_t src_vram(VRAM_SEG);
        src_vram.addDesc(nixlBlobDesc((uintptr_t)(rdma_buffer_ptr), num_rdma_bytes, get_local_device_id(), ""));
        nixl_xfer_dlist_t dst_vram(VRAM_SEG);
        dst_vram.addDesc(nixlBlobDesc((uintptr_t)(nixl_peer_info[j].rdma_buffer_ptr), num_rdma_bytes, nixl_peer_info[j].device_id, ""));
        nixl_opt_args_t extra_params = {};
        extra_params.backends.push_back(nixl_agent_info->backend);
        // extra_params.customParam = "worker_id=" + std::to_string(i);
        status = nixl_agent_info->agent->createXferReq(NIXL_WRITE, src_vram, dst_vram, nixl_agent_info->remote_agent_names[j], nixl_ctx->cpu_batch_reqs[j], &extra_params);
        EP_HOST_ASSERT(status == NIXL_SUCCESS);
        EP_HOST_ASSERT(nixl_agent_info->agent->createGpuXferReq(*nixl_ctx->cpu_batch_reqs[j], nixl_ctx->gpu_batch_reqs[j]) == NIXL_SUCCESS);

        nixl_xfer_dlist_t src_vram_ll(VRAM_SEG);
        src_vram_ll.addDesc(nixlBlobDesc((uintptr_t)(sync_buffer_ptr), max_num_ranks * sizeof(int), get_local_device_id(), ""));
        nixl_xfer_dlist_t dst_vram_ll(VRAM_SEG);
        dst_vram_ll.addDesc(nixlBlobDesc((uintptr_t)(nixl_peer_info[j].sync_buffer_ptr), max_num_ranks * sizeof(int), nixl_peer_info[j].device_id, ""));
        EP_HOST_ASSERT(nixl_agent_info->agent->createXferReq(NIXL_WRITE, src_vram_ll, dst_vram_ll, nixl_agent_info->remote_agent_names[j], nixl_ctx->cpu_barrier_reqs[j], &extra_params) == NIXL_SUCCESS);
        EP_HOST_ASSERT(nixl_agent_info->agent->createGpuXferReq(*nixl_ctx->cpu_barrier_reqs[j], nixl_ctx->gpu_barrier_reqs[j]) == NIXL_SUCCESS);
    }
}

void Buffer::_nixl_ep_p2p_ptrs_prepare(const std::vector<int>& ranks) {
    for (int i : ranks) {
        if (i == rank) {
            nixl_ctx->rdma_p2p_ptrs[i] = rdma_buffer_ptr;
            nixl_ctx->counters_p2p_ptrs[i] = counters_buffer_ptr;
        } else if (std::string(nixl_peer_info[i].boot_id) == std::string(my_peer_info.boot_id) &&
                   nixl_peer_info[i].ipc_namespace_inode == my_peer_info.ipc_namespace_inode &&
                   std::string(std::getenv("NIXL_EP_NVLINK_BACKEND_IPC")) == "1") {
            CUDA_CHECK(cudaIpcOpenMemHandle((void **)&nixl_ctx->rdma_p2p_ptrs[i], nixl_peer_info[i].rdma_ipc_handle, cudaIpcMemLazyEnablePeerAccess));
            CUDA_CHECK(cudaIpcOpenMemHandle((void **)&nixl_ctx->counters_p2p_ptrs[i], nixl_peer_info[i].counters_ipc_handle, cudaIpcMemLazyEnablePeerAccess));
        } else {
            nixl_ctx->rdma_p2p_ptrs[i] = nullptr;
            nixl_ctx->counters_p2p_ptrs[i] = nullptr;
        }
    }
}

void Buffer::_nixl_ep_counters_prepare(const std::vector<int>& ranks) {
    int num_local_experts = env_num_channels;
    for (int remote_rank : ranks) {
        if (remote_rank == rank)
            continue;

        int local_counter_idx = remote_rank; // remote_counter_reqs is indexed by [local_expert_idx, dst_rank]

        if (nixl_peer_info[remote_rank].counters_buffer_ptr == nullptr) {
            printf("[ERROR] _nixl_ep_counters_prepare: nixl_peer_info[%d].counters_buffer_ptr is NULL!\n", remote_rank);
            exit(1);
        }

        // Fetch the first counter (double buffering)
        uint64_t *remote_counter_addr = nixl_peer_info[remote_rank].counters_buffer_ptr;
        nixl_opt_args_t eparams = {};
        eparams.backends.push_back(nixl_agent_info->backend);
        // eparams.customParam = "worker_id=" + std::to_string(expert_idx);
        nixl_xfer_dlist_t dst_dlist(VRAM_SEG);
        dst_dlist.addDesc(nixlBlobDesc((uintptr_t)remote_counter_addr, sizeof(uint64_t), nixl_peer_info[remote_rank].device_id, ""));
        EP_HOST_ASSERT(nixl_agent_info->agent->createXferReq(NIXL_WRITE, dummy_src_dlist, dst_dlist, nixl_agent_info->remote_agent_names[remote_rank], nixl_ctx->cpu_remote_counter_reqs_0[local_counter_idx], &eparams) == NIXL_SUCCESS);
        EP_HOST_ASSERT(nixl_agent_info->agent->createGpuXferReq(*nixl_ctx->cpu_remote_counter_reqs_0[local_counter_idx], nixl_ctx->gpu_remote_counter_reqs_0[local_counter_idx]) == NIXL_SUCCESS);

        // Fetch the second counter (double buffering)
        remote_counter_addr += max_num_ranks * num_local_experts;
        nixl_xfer_dlist_t dst_dlist_2(VRAM_SEG);
        dst_dlist_2.addDesc(nixlBlobDesc((uintptr_t)remote_counter_addr, sizeof(uint64_t), nixl_peer_info[remote_rank].device_id, ""));
        EP_HOST_ASSERT(nixl_agent_info->agent->createXferReq(NIXL_WRITE, dummy_src_dlist, dst_dlist_2, nixl_agent_info->remote_agent_names[remote_rank], nixl_ctx->cpu_remote_counter_reqs_1[local_counter_idx], &eparams) == NIXL_SUCCESS);
        EP_HOST_ASSERT(nixl_agent_info->agent->createGpuXferReq(*nixl_ctx->cpu_remote_counter_reqs_1[local_counter_idx], nixl_ctx->gpu_remote_counter_reqs_1[local_counter_idx]) == NIXL_SUCCESS);
    }
}

void Buffer::_nixl_agents_disconnect(const std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        EP_HOST_ASSERT(remote_rank != rank);
        EP_HOST_ASSERT(remote_rank < num_ranks);
        nixl_xfer_dlist_t empty_descs(VRAM_SEG);
        if(nixl_agent_info->agent->checkRemoteMD(nixl_agent_info->remote_agent_names[remote_rank], empty_descs) == NIXL_SUCCESS) {
            nixl_status_t status = nixl_agent_info->agent->invalidateRemoteMD(nixl_agent_info->remote_agent_names[remote_rank]);
            // NIXL watchers might invalidate peer metadata, so we ignore NIXL_ERR_NOT_FOUND errors
            if (status != NIXL_SUCCESS && status != NIXL_ERR_NOT_FOUND) {
                printf("WARNING: rank %d Failed to invalidate remote rank %d metadata for agent %s, status: %d\n", 
                    rank, remote_rank, std::to_string(remote_rank).c_str(), status); fflush(stdout);
            }
        }
    }
}

void Buffer::_nixl_agents_peer_info_cleanup(const std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        nixl_agent_info->wire_up_done[remote_rank] = false;
        // Clear nixl_peer_info for removed ranks (only do this once per rank, on first channel)
        nixl_peer_info[remote_rank] = NixlPeerInfo{};
    }
}

void Buffer::_nixl_ep_cleanup(const std::vector<int>& ranks) {
    _nixl_ep_p2p_ptrs_cleanup(ranks);
    _nixl_ep_batches_cleanup(ranks);
    _nixl_ep_counters_cleanup(ranks);
    _nixl_ep_gpu_ctx_update();
}

void Buffer::_nixl_ep_counters_cleanup(const std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        EP_HOST_ASSERT(remote_rank != rank); 

        // Clean up remote counter requests (double buffering)
        if (nixl_ctx->cpu_remote_counter_reqs_0[remote_rank] != nullptr) {

#ifndef EP_REMOVE_ONCE
            nixl_agent_info->agent->releaseGpuXferReq(nixl_ctx->gpu_remote_counter_reqs_0[remote_rank]);
            nixl_agent_info->agent->releaseXferReq(nixl_ctx->cpu_remote_counter_reqs_0[remote_rank]);
#endif
            nixl_ctx->cpu_remote_counter_reqs_0[remote_rank] = nullptr;
            nixl_ctx->gpu_remote_counter_reqs_0[remote_rank] = nullptr;
        }
        
        if (nixl_ctx->cpu_remote_counter_reqs_1[remote_rank] != nullptr) {
#ifndef EP_REMOVE_ONCE
            nixl_agent_info->agent->releaseGpuXferReq(nixl_ctx->gpu_remote_counter_reqs_1[remote_rank]);
            nixl_agent_info->agent->releaseXferReq(nixl_ctx->cpu_remote_counter_reqs_1[remote_rank]);
#endif
            nixl_ctx->cpu_remote_counter_reqs_1[remote_rank] = nullptr;
            nixl_ctx->gpu_remote_counter_reqs_1[remote_rank] = nullptr;
        }

        if (nixl_ctx->cpu_barrier_reqs[remote_rank] != nullptr) {
#ifndef EP_REMOVE_ONCE
            nixl_agent_info->agent->releaseGpuXferReq(nixl_ctx->gpu_barrier_reqs[remote_rank]);
            nixl_agent_info->agent->releaseXferReq(nixl_ctx->cpu_barrier_reqs[remote_rank]);
#endif
            nixl_ctx->cpu_barrier_reqs[remote_rank] = nullptr;
            nixl_ctx->gpu_barrier_reqs[remote_rank] = nullptr;
        }
    }
}

void Buffer::_nixl_ep_batches_cleanup(const std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        if (remote_rank == rank) continue;
        
        // Clean up cpu_batch_reqs and gpu_batch_reqs
        if (remote_rank < nixl_ctx->cpu_batch_reqs.size() && 
            nixl_ctx->cpu_batch_reqs[remote_rank] != nullptr) {
            
            // Release GPU transfer request first
            if (nixl_ctx->gpu_batch_reqs[remote_rank] != nullptr) {
#ifndef EP_REMOVE_ONCE
                nixl_agent_info->agent->releaseGpuXferReq(nixl_ctx->gpu_batch_reqs[remote_rank]);
#endif
                nixl_ctx->gpu_batch_reqs[remote_rank] = nullptr;
            }
            
            // Release CPU transfer request
#ifndef EP_REMOVE_ONCE
            nixl_status_t status = nixl_agent_info->agent->releaseXferReq(nixl_ctx->cpu_batch_reqs[remote_rank]);
            if (status != NIXL_SUCCESS) {
                printf("[WARNING] %s: Failed to release CPU batch xfer req for rank %d, status: %d\n", 
                        __func__, remote_rank, status);
            }
#endif
            nixl_ctx->cpu_batch_reqs[remote_rank] = nullptr;
        }
    }
}

void Buffer::_nixl_ep_p2p_ptrs_cleanup(const std::vector<int>& ranks) {
    for (int remote_rank : ranks) {
        EP_HOST_ASSERT(remote_rank < num_ranks);
        // Close P2P memory mappings if they exist
        if (nixl_ctx->rdma_p2p_ptrs[remote_rank] != nullptr && 
            nixl_ctx->rdma_p2p_ptrs[remote_rank] != rdma_buffer_ptr) {
            CUDA_CHECK(cudaIpcCloseMemHandle(nixl_ctx->rdma_p2p_ptrs[remote_rank]));
            nixl_ctx->rdma_p2p_ptrs[remote_rank] = nullptr;
        }
        
        if (nixl_ctx->counters_p2p_ptrs[remote_rank] != nullptr &&
            nixl_ctx->counters_p2p_ptrs[remote_rank] != counters_buffer_ptr) {
            CUDA_CHECK(cudaIpcCloseMemHandle(nixl_ctx->counters_p2p_ptrs[remote_rank]));
            nixl_ctx->counters_p2p_ptrs[remote_rank] = nullptr;
        }
    }
}

int Buffer::get_num_rdma_ranks() const {
    return num_rdma_ranks;
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, torch::Tensor, std::optional<EventHandle>>
Buffer::get_dispatch_layout(const torch::Tensor& topk_idx, int num_experts,
                            const std::optional<EventHandle>& previous_event,
                            bool async_finish, bool allocate_on_comm_stream) {
    // This function calculates the dispatch layout for high-throughput mode
    // Currently returns placeholder tensors as high-throughput mode is not yet fully implemented
    
    EP_HOST_ASSERT(topk_idx.dim() == 2 && topk_idx.is_contiguous());
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    
    auto num_tokens = static_cast<int>(topk_idx.size(0));
    auto num_topk = static_cast<int>(topk_idx.size(1));
    
    // Wait for previous event if specified
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    if (previous_event.has_value()) {
        previous_event->current_stream_wait();
    }
    
    // Allocate output tensors
    auto num_tokens_per_rank = torch::zeros({num_ranks}, torch::dtype(torch::kInt32).device(torch::kCUDA));
    auto num_tokens_per_expert = torch::zeros({num_experts}, torch::dtype(torch::kInt32).device(torch::kCUDA));
    auto is_token_in_rank = torch::zeros({num_tokens, num_ranks}, torch::dtype(torch::kBool).device(torch::kCUDA));
    
    // For internode mode, also return num_tokens_per_rdma_rank
    std::optional<torch::Tensor> num_tokens_per_rdma_rank = std::nullopt;
    int num_rdma_ranks = get_num_rdma_ranks();
    if (num_rdma_ranks > 1) {
        num_tokens_per_rdma_rank = torch::zeros({num_rdma_ranks}, torch::dtype(torch::kInt32).device(torch::kCUDA));
    }
    
    // Call the layout kernel
    layout::get_dispatch_layout(
        topk_idx.data_ptr<int64_t>(),
        num_tokens_per_rank.data_ptr<int>(),
        num_tokens_per_rdma_rank.has_value() ? num_tokens_per_rdma_rank->data_ptr<int>() : nullptr,
        num_tokens_per_expert.data_ptr<int>(),
        is_token_in_rank.data_ptr<bool>(),
        num_tokens, num_topk, num_ranks, num_experts,
        compute_stream
    );
    
    // Create event if async_finish is requested
    std::optional<EventHandle> event = std::nullopt;
    if (async_finish) {
        event = EventHandle(compute_stream);
    }
    
    return {num_tokens_per_rank, num_tokens_per_rdma_rank, num_tokens_per_expert, is_token_in_rank, event};
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::vector<int>,
           torch::Tensor, torch::Tensor, std::optional<torch::Tensor>, torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
           std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::optional<torch::Tensor>, std::optional<EventHandle>>
Buffer::internode_dispatch(const torch::Tensor& x, const std::optional<torch::Tensor>& x_scales,
                           const std::optional<torch::Tensor>& topk_idx, const std::optional<torch::Tensor>& topk_weights,
                           const std::optional<torch::Tensor>& num_tokens_per_rank_opt, const std::optional<torch::Tensor>& num_tokens_per_rdma_rank_opt,
                           const torch::Tensor& is_token_in_rank, const std::optional<torch::Tensor>& num_tokens_per_expert_opt,
                           int cached_num_recv_tokens, int cached_num_rdma_recv_tokens,
                           const std::optional<torch::Tensor>& cached_rdma_channel_prefix_matrix, const std::optional<torch::Tensor>& cached_recv_rdma_rank_prefix_sum,
                           const std::optional<torch::Tensor>& cached_gbl_channel_prefix_matrix, const std::optional<torch::Tensor>& cached_recv_gbl_rank_prefix_sum,
                           int expert_alignment, const Config& config, std::optional<EventHandle>& previous_event, bool async, bool allocate_on_comm_stream) {
    // Release GIL for CPU busy-wait
    pybind11::gil_scoped_release release;
    HOST_LOG_DEBUG("internode_dispatch");

    const int num_channels = config.num_sms / 2;
    EP_HOST_ASSERT(config.num_sms % 2 == 0);
    EP_HOST_ASSERT(0 < get_num_rdma_ranks() and get_num_rdma_ranks() <= NUM_MAX_RDMA_PEERS);
    EP_HOST_ASSERT(internode_ctx != nullptr && "Internode context not initialized");

    bool cached_mode = cached_rdma_channel_prefix_matrix.has_value();
    if (cached_mode) {
        EP_HOST_ASSERT(cached_rdma_channel_prefix_matrix.has_value());
        EP_HOST_ASSERT(cached_recv_rdma_rank_prefix_sum.has_value());
        EP_HOST_ASSERT(cached_gbl_channel_prefix_matrix.has_value());
        EP_HOST_ASSERT(cached_recv_gbl_rank_prefix_sum.has_value());
    } else {
        EP_HOST_ASSERT(num_tokens_per_rank_opt.has_value());
        EP_HOST_ASSERT(num_tokens_per_rdma_rank_opt.has_value());
        EP_HOST_ASSERT(num_tokens_per_expert_opt.has_value());
    }

    // Type checks
    if (cached_mode) {
        EP_HOST_ASSERT(cached_rdma_channel_prefix_matrix->scalar_type() == torch::kInt32);
        EP_HOST_ASSERT(cached_recv_rdma_rank_prefix_sum->scalar_type() == torch::kInt32);
        EP_HOST_ASSERT(cached_gbl_channel_prefix_matrix->scalar_type() == torch::kInt32);
        EP_HOST_ASSERT(cached_recv_gbl_rank_prefix_sum->scalar_type() == torch::kInt32);
    } else {
        EP_HOST_ASSERT(num_tokens_per_rank_opt->scalar_type() == torch::kInt32);
        EP_HOST_ASSERT(num_tokens_per_rdma_rank_opt->scalar_type() == torch::kInt32);
        EP_HOST_ASSERT(num_tokens_per_expert_opt->scalar_type() == torch::kInt32);
    }

    // Shape and contiguous checks
    EP_HOST_ASSERT(x.dim() == 2 and x.is_contiguous());
    EP_HOST_ASSERT((x.size(1) * x.element_size()) % sizeof(int4) == 0);
    if (cached_mode) {
        EP_HOST_ASSERT(cached_rdma_channel_prefix_matrix->dim() == 2 and cached_rdma_channel_prefix_matrix->is_contiguous());
        EP_HOST_ASSERT(cached_rdma_channel_prefix_matrix->size(0) == num_rdma_ranks and cached_rdma_channel_prefix_matrix->size(1) == num_channels);
        EP_HOST_ASSERT(cached_recv_rdma_rank_prefix_sum->dim() == 1 and cached_recv_rdma_rank_prefix_sum->is_contiguous());
        EP_HOST_ASSERT(cached_recv_rdma_rank_prefix_sum->size(0) == num_rdma_ranks);
        EP_HOST_ASSERT(cached_gbl_channel_prefix_matrix->dim() == 2 and cached_gbl_channel_prefix_matrix->is_contiguous());
        EP_HOST_ASSERT(cached_gbl_channel_prefix_matrix->size(0) == num_ranks and cached_gbl_channel_prefix_matrix->size(1) == num_channels);
        EP_HOST_ASSERT(cached_recv_gbl_rank_prefix_sum->dim() == 1 and cached_recv_gbl_rank_prefix_sum->is_contiguous());
        EP_HOST_ASSERT(cached_recv_gbl_rank_prefix_sum->size(0) == num_ranks);
    } else {
        EP_HOST_ASSERT(num_tokens_per_rank_opt->dim() == 1 and num_tokens_per_rank_opt->is_contiguous());
        EP_HOST_ASSERT(num_tokens_per_rdma_rank_opt->dim() == 1 and num_tokens_per_rdma_rank_opt->is_contiguous());
        EP_HOST_ASSERT(num_tokens_per_expert_opt->dim() == 1 and num_tokens_per_expert_opt->is_contiguous());
        EP_HOST_ASSERT(num_tokens_per_rank_opt->size(0) == num_ranks);
        EP_HOST_ASSERT(num_tokens_per_rdma_rank_opt->size(0) == num_rdma_ranks);
        EP_HOST_ASSERT(num_tokens_per_expert_opt->size(0) % num_ranks == 0);
        EP_HOST_ASSERT(num_tokens_per_expert_opt->size(0) / num_ranks <= NUM_MAX_LOCAL_EXPERTS);
    }

    auto num_tokens = static_cast<int>(x.size(0)), hidden = static_cast<int>(x.size(1)), hidden_int4 = static_cast<int>(x.size(1) * x.element_size() / sizeof(int4));
    auto num_experts = cached_mode ? 0 : static_cast<int>(num_tokens_per_expert_opt->size(0)), num_local_experts = num_experts / num_ranks;

    // Top-k checks
    int num_topk = 0;
    int64_t* topk_idx_ptr = nullptr;
    float* topk_weights_ptr = nullptr;
    EP_HOST_ASSERT(topk_idx.has_value() == topk_weights.has_value());
    if (topk_idx.has_value()) {
        num_topk = static_cast<int>(topk_idx->size(1));
        EP_HOST_ASSERT(num_experts > 0);
        EP_HOST_ASSERT(topk_idx->dim() == 2 and topk_idx->is_contiguous());
        EP_HOST_ASSERT(topk_weights->dim() == 2 and topk_weights->is_contiguous());
        EP_HOST_ASSERT(num_tokens == topk_idx->size(0) and num_tokens == topk_weights->size(0));
        EP_HOST_ASSERT(num_topk == topk_weights->size(1));
        EP_HOST_ASSERT(topk_weights->scalar_type() == torch::kFloat32);
        topk_idx_ptr = topk_idx->data_ptr<int64_t>();
        topk_weights_ptr = topk_weights->data_ptr<float>();
    }

    // FP8 scales checks
    float* x_scales_ptr = nullptr;
    int num_scales = 0, scale_token_stride = 0, scale_hidden_stride = 0;
    if (x_scales.has_value()) {
        EP_HOST_ASSERT(x.element_size() == 1);
        EP_HOST_ASSERT(x_scales->scalar_type() == torch::kFloat32 or x_scales->scalar_type() == torch::kInt);
        EP_HOST_ASSERT(x_scales->dim() == 2);
        EP_HOST_ASSERT(x_scales->size(0) == num_tokens);
        num_scales = x_scales->dim() == 1 ? 1 : static_cast<int>(x_scales->size(1));
        x_scales_ptr = static_cast<float*>(x_scales->data_ptr());
        scale_token_stride = static_cast<int>(x_scales->stride(0));
        scale_hidden_stride = static_cast<int>(x_scales->stride(1));
    }

    // Allocate all tensors on comm stream if set
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    if (allocate_on_comm_stream) {
        EP_HOST_ASSERT(previous_event.has_value() and async);
        at::cuda::setCurrentCUDAStream(comm_stream);
    }

    // Wait previous tasks to be finished
    if (previous_event.has_value()) {
        stream_wait(comm_stream, previous_event.value());
    } else {
        stream_wait(comm_stream, compute_stream);
    }

    // Create handles (only return for non-cached mode)
    int num_recv_tokens = -1, num_rdma_recv_tokens_local = -1;
    auto rdma_channel_prefix_matrix = torch::Tensor();
    auto recv_rdma_rank_prefix_sum = torch::Tensor();
    auto gbl_channel_prefix_matrix = torch::Tensor();
    auto recv_gbl_rank_prefix_sum = torch::Tensor();
    std::vector<int> num_recv_tokens_per_expert_list;

    // Barrier or send sizes
    if (cached_mode) {
        num_recv_tokens = cached_num_recv_tokens;
        num_rdma_recv_tokens_local = cached_num_rdma_recv_tokens;
        rdma_channel_prefix_matrix = cached_rdma_channel_prefix_matrix.value();
        recv_rdma_rank_prefix_sum = cached_recv_rdma_rank_prefix_sum.value();
        gbl_channel_prefix_matrix = cached_gbl_channel_prefix_matrix.value();
        recv_gbl_rank_prefix_sum = cached_recv_gbl_rank_prefix_sum.value();

        // Just a barrier and clean flags
        internode::cached_notify(hidden_int4, num_scales, num_topk, num_topk,
                                 num_ranks, num_channels, 0, nullptr,
                                 nullptr, nullptr, nullptr,
                                 rdma_buffer_ptr, config.num_max_rdma_chunked_recv_tokens,
                                 buffer_ptrs_gpu, config.num_max_nvl_chunked_recv_tokens,
                                 barrier_signal_ptrs_gpu, rank, comm_stream,
                                 config.get_internode_rdma_buffer_size_hint(hidden_int4 * sizeof(int4), num_ranks),
                                 num_nvl_bytes, true, false, internode_ctx->gpu_ctx);
    } else {
        rdma_channel_prefix_matrix = torch::empty({num_rdma_ranks, num_channels}, torch::dtype(torch::kInt32).device(torch::kCUDA));
        recv_rdma_rank_prefix_sum = torch::empty({num_rdma_ranks}, torch::dtype(torch::kInt32).device(torch::kCUDA));
        gbl_channel_prefix_matrix = torch::empty({num_ranks, num_channels}, torch::dtype(torch::kInt32).device(torch::kCUDA));
        recv_gbl_rank_prefix_sum = torch::empty({num_ranks}, torch::dtype(torch::kInt32).device(torch::kCUDA));

        // Send sizes
        *const_cast<int*>(moe_recv_counter) = -1;
        *const_cast<int*>(moe_recv_rdma_counter) = -1;
        for (int i = 0; i < num_local_experts; ++i)
            const_cast<int*>(moe_recv_expert_counter)[i] = -1;
        
        internode::notify_dispatch(num_tokens_per_rank_opt->data_ptr<int>(), moe_recv_counter_mapped, num_ranks,
                                   num_tokens_per_rdma_rank_opt->data_ptr<int>(), moe_recv_rdma_counter_mapped,
                                   num_tokens_per_expert_opt->data_ptr<int>(), moe_recv_expert_counter_mapped, num_experts,
                                   is_token_in_rank.data_ptr<bool>(), num_tokens, num_channels,
                                   hidden_int4, num_scales, num_topk, expert_alignment,
                                   rdma_channel_prefix_matrix.data_ptr<int>(), recv_rdma_rank_prefix_sum.data_ptr<int>(),
                                   gbl_channel_prefix_matrix.data_ptr<int>(), recv_gbl_rank_prefix_sum.data_ptr<int>(),
                                   rdma_buffer_ptr, config.num_max_rdma_chunked_recv_tokens,
                                   buffer_ptrs_gpu, config.num_max_nvl_chunked_recv_tokens,
                                   barrier_signal_ptrs_gpu, rank, comm_stream,
                                   config.get_internode_rdma_buffer_size_hint(hidden_int4 * sizeof(int4), num_ranks),
                                   num_nvl_bytes, false, internode_ctx->gpu_ctx);

        // Synchronize total received tokens and tokens per expert
        auto start_time = std::chrono::high_resolution_clock::now();
        while (true) {
            // Read total count
            num_recv_tokens = static_cast<int>(*moe_recv_counter);
            num_rdma_recv_tokens_local = static_cast<int>(*moe_recv_rdma_counter);

            // Read per-expert count
            bool ready = (num_recv_tokens >= 0) and (num_rdma_recv_tokens_local >= 0);
            for (int i = 0; i < num_local_experts and ready; ++i)
                ready &= moe_recv_expert_counter[i] >= 0;

            if (ready)
                break;

            // Timeout check
            if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - start_time).count() > NUM_CPU_TIMEOUT_SECS) {
                for (int i = 0; i < num_local_experts; ++i)
                    printf("moe_recv_expert_counter[%d]: %d\n", i, moe_recv_expert_counter[i]);
                throw std::runtime_error("NIXL_EP error: timeout (dispatch CPU)");
            }
        }
        num_recv_tokens_per_expert_list = std::vector<int>(moe_recv_expert_counter, moe_recv_expert_counter + num_local_experts);
    }

    // Allocate new tensors
    auto recv_x = torch::empty({num_recv_tokens, hidden}, x.options());
    auto recv_topk_idx = std::optional<torch::Tensor>(), recv_topk_weights = std::optional<torch::Tensor>(), recv_x_scales = std::optional<torch::Tensor>();
    auto recv_src_meta = std::optional<torch::Tensor>();
    auto recv_rdma_channel_prefix_matrix = std::optional<torch::Tensor>();
    auto recv_gbl_channel_prefix_matrix = std::optional<torch::Tensor>();
    auto send_rdma_head = std::optional<torch::Tensor>();
    auto send_nvl_head = std::optional<torch::Tensor>();
    if (not cached_mode) {
        recv_src_meta = torch::empty({num_recv_tokens, internode::get_source_meta_bytes()}, torch::dtype(torch::kByte).device(torch::kCUDA));
        recv_rdma_channel_prefix_matrix = torch::empty({num_rdma_ranks, num_channels}, torch::dtype(torch::kInt32).device(torch::kCUDA));
        recv_gbl_channel_prefix_matrix = torch::empty({num_ranks, num_channels}, torch::dtype(torch::kInt32).device(torch::kCUDA));
        send_rdma_head = torch::empty({num_tokens, num_rdma_ranks}, torch::dtype(torch::kInt32).device(torch::kCUDA));
        send_nvl_head = torch::empty({num_rdma_recv_tokens_local, NUM_MAX_NVL_PEERS}, torch::dtype(torch::kInt32).device(torch::kCUDA));
    }

    // Assign pointers
    int64_t* recv_topk_idx_ptr = nullptr;
    float* recv_topk_weights_ptr = nullptr;
    float* recv_x_scales_ptr = nullptr;
    if (topk_idx.has_value()) {
        recv_topk_idx = torch::empty({num_recv_tokens, num_topk}, topk_idx->options());
        recv_topk_weights = torch::empty({num_recv_tokens, num_topk}, topk_weights->options());
        recv_topk_idx_ptr = recv_topk_idx->data_ptr<int64_t>();
        recv_topk_weights_ptr = recv_topk_weights->data_ptr<float>();
    }
    if (x_scales.has_value()) {
        recv_x_scales = x_scales->dim() == 1 ?
                        torch::empty({num_recv_tokens}, x_scales->options()) :
                        torch::empty({num_recv_tokens, num_scales}, x_scales->options());
        recv_x_scales_ptr = static_cast<float*>(recv_x_scales->data_ptr());
    }

    // Launch data dispatch
    internode::dispatch(recv_x.data_ptr(), recv_x_scales_ptr, recv_topk_idx_ptr, recv_topk_weights_ptr,
                        cached_mode ? nullptr : recv_src_meta->data_ptr(),
                        x.data_ptr(), x_scales_ptr, topk_idx_ptr, topk_weights_ptr,
                        cached_mode ? nullptr : send_rdma_head->data_ptr<int>(), cached_mode ? nullptr : send_nvl_head->data_ptr<int>(),
                        cached_mode ? nullptr : recv_rdma_channel_prefix_matrix->data_ptr<int>(),
                        cached_mode ? nullptr : recv_gbl_channel_prefix_matrix->data_ptr<int>(),
                        rdma_channel_prefix_matrix.data_ptr<int>(), recv_rdma_rank_prefix_sum.data_ptr<int>(),
                        gbl_channel_prefix_matrix.data_ptr<int>(), recv_gbl_rank_prefix_sum.data_ptr<int>(),
                        is_token_in_rank.data_ptr<bool>(),
                        num_tokens, hidden_int4, num_scales, num_topk, num_experts,
                        scale_token_stride, scale_hidden_stride,
                        rdma_buffer_ptr, config.num_max_rdma_chunked_send_tokens, config.num_max_rdma_chunked_recv_tokens,
                        buffer_ptrs_gpu, config.num_max_nvl_chunked_send_tokens, config.num_max_nvl_chunked_recv_tokens,
                        rank, num_ranks, cached_mode,
                        comm_stream, num_channels, false, internode_ctx->gpu_ctx);

    HOST_LOG_DEBUG("internode_dispatch finished");

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        event = EventHandle(comm_stream);
        for (auto& t: {x, is_token_in_rank, recv_x,
                       rdma_channel_prefix_matrix, recv_rdma_rank_prefix_sum, gbl_channel_prefix_matrix, recv_gbl_rank_prefix_sum}) {
            t.record_stream(comm_stream);
            if (allocate_on_comm_stream)
                t.record_stream(compute_stream);
        }
        for (auto& to: {x_scales, topk_idx, topk_weights,
                        num_tokens_per_rank_opt, num_tokens_per_rdma_rank_opt, num_tokens_per_expert_opt,
                        cached_rdma_channel_prefix_matrix, cached_recv_rdma_rank_prefix_sum,
                        cached_gbl_channel_prefix_matrix, cached_recv_gbl_rank_prefix_sum,
                        recv_topk_idx, recv_topk_weights, recv_x_scales,
                        recv_rdma_channel_prefix_matrix, recv_gbl_channel_prefix_matrix, send_rdma_head, send_nvl_head,
                        recv_src_meta}) {
            to.has_value() ? to->record_stream(comm_stream) : void();
            if (allocate_on_comm_stream)
                to.has_value() ? to->record_stream(compute_stream) : void();
        }
    } else {
        stream_wait(compute_stream, comm_stream);
    }

    // Switch back compute stream
    if (allocate_on_comm_stream)
        at::cuda::setCurrentCUDAStream(compute_stream);

    // Return values
    return {recv_x, recv_x_scales, recv_topk_idx, recv_topk_weights, num_recv_tokens_per_expert_list,
            rdma_channel_prefix_matrix, gbl_channel_prefix_matrix,
            recv_rdma_channel_prefix_matrix, recv_rdma_rank_prefix_sum,
            recv_gbl_channel_prefix_matrix, recv_gbl_rank_prefix_sum,
            recv_src_meta, send_rdma_head, send_nvl_head, event};
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, std::optional<EventHandle>>
Buffer::internode_combine(const torch::Tensor& x, const std::optional<torch::Tensor>& topk_weights,
                          const std::optional<torch::Tensor>& bias_0, const std::optional<torch::Tensor>& bias_1,
                          const torch::Tensor& src_meta, const torch::Tensor& is_combined_token_in_rank,
                          const torch::Tensor& rdma_channel_prefix_matrix, const torch::Tensor& rdma_rank_prefix_sum, const torch::Tensor& gbl_channel_prefix_matrix,
                          const torch::Tensor& combined_rdma_head, const torch::Tensor& combined_nvl_head,
                          const Config& config, std::optional<EventHandle>& previous_event, bool async, bool allocate_on_comm_stream) {
    const int num_channels = config.num_sms / 2;
    HOST_LOG_DEBUG("internode_combine | num_channels: %d", num_channels);
    EP_HOST_ASSERT(config.num_sms % 2 == 0);
    EP_HOST_ASSERT(internode_ctx != nullptr && "Internode context not initialized");

    // Shape and contiguous checks
    EP_HOST_ASSERT(x.dim() == 2 and x.is_contiguous());
    EP_HOST_ASSERT(src_meta.dim() == 2 and src_meta.is_contiguous() and src_meta.scalar_type() == torch::kByte);
    EP_HOST_ASSERT(is_combined_token_in_rank.dim() == 2 and is_combined_token_in_rank.is_contiguous() and is_combined_token_in_rank.scalar_type() == torch::kBool);
    EP_HOST_ASSERT(rdma_channel_prefix_matrix.dim() == 2 and rdma_channel_prefix_matrix.is_contiguous() and rdma_channel_prefix_matrix.scalar_type() == torch::kInt32);
    EP_HOST_ASSERT(rdma_rank_prefix_sum.dim() == 1 and rdma_rank_prefix_sum.is_contiguous() and rdma_rank_prefix_sum.scalar_type() == torch::kInt32);
    EP_HOST_ASSERT(gbl_channel_prefix_matrix.dim() == 2 and gbl_channel_prefix_matrix.is_contiguous() and gbl_channel_prefix_matrix.scalar_type() == torch::kInt32);
    EP_HOST_ASSERT(combined_rdma_head.dim() == 2 and combined_rdma_head.is_contiguous() and combined_rdma_head.scalar_type() == torch::kInt32);
    EP_HOST_ASSERT(combined_nvl_head.dim() == 2 and combined_nvl_head.is_contiguous() and combined_nvl_head.scalar_type() == torch::kInt32);

    auto num_tokens = static_cast<int>(x.size(0)), hidden = static_cast<int>(x.size(1)), hidden_int4 = static_cast<int>(x.size(1) * x.element_size() / sizeof(int4));
    auto num_combined_tokens = static_cast<int>(is_combined_token_in_rank.size(0));
    EP_HOST_ASSERT((hidden * x.element_size()) % sizeof(int4) == 0);
    EP_HOST_ASSERT(src_meta.size(1) == internode::get_source_meta_bytes());
    EP_HOST_ASSERT(is_combined_token_in_rank.size(1) == num_ranks);
    EP_HOST_ASSERT(rdma_channel_prefix_matrix.size(0) == num_rdma_ranks and rdma_channel_prefix_matrix.size(1) == num_channels);
    EP_HOST_ASSERT(rdma_rank_prefix_sum.size(0) == num_rdma_ranks);
    EP_HOST_ASSERT(gbl_channel_prefix_matrix.size(0) == num_ranks and gbl_channel_prefix_matrix.size(1) == num_channels);
    EP_HOST_ASSERT(combined_rdma_head.dim() == 2 and combined_rdma_head.size(0) == num_combined_tokens and combined_rdma_head.size(1) == num_rdma_ranks);
    EP_HOST_ASSERT(combined_nvl_head.dim() == 2 and combined_nvl_head.size(1) == NUM_MAX_NVL_PEERS);

    // Allocate all tensors on comm stream if set
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    if (allocate_on_comm_stream) {
        EP_HOST_ASSERT(previous_event.has_value() and async);
        at::cuda::setCurrentCUDAStream(comm_stream);
    }

    // Wait previous tasks to be finished
    if (previous_event.has_value()) {
        stream_wait(comm_stream, previous_event.value());
    } else {
        stream_wait(comm_stream, compute_stream);
    }

    // Top-k checks
    int num_topk = 0;
    auto combined_topk_weights = std::optional<torch::Tensor>();
    float* topk_weights_ptr = nullptr;
    float* combined_topk_weights_ptr = nullptr;
    if (topk_weights.has_value()) {
        EP_HOST_ASSERT(topk_weights->dim() == 2 and topk_weights->is_contiguous());
        EP_HOST_ASSERT(topk_weights->size(0) == num_tokens);
        EP_HOST_ASSERT(topk_weights->scalar_type() == torch::kFloat32);
        num_topk = static_cast<int>(topk_weights->size(1));
        topk_weights_ptr = topk_weights->data_ptr<float>();
        combined_topk_weights = torch::empty({num_combined_tokens, num_topk}, topk_weights->options());
        combined_topk_weights_ptr = combined_topk_weights->data_ptr<float>();
    }

    // Extra check for avoid-dead-lock design
    EP_HOST_ASSERT(config.num_max_nvl_chunked_recv_tokens % num_rdma_ranks == 0);
    EP_HOST_ASSERT(config.num_max_nvl_chunked_send_tokens <= config.num_max_nvl_chunked_recv_tokens / num_rdma_ranks);

    // Launch barrier and reset queue head and tail
    internode::cached_notify(hidden_int4, 0, 0, num_topk,
                             num_ranks, num_channels,
                             num_combined_tokens, combined_rdma_head.data_ptr<int>(),
                             rdma_channel_prefix_matrix.data_ptr<int>(), rdma_rank_prefix_sum.data_ptr<int>(), combined_nvl_head.data_ptr<int>(),
                             rdma_buffer_ptr, config.num_max_rdma_chunked_recv_tokens,
                             buffer_ptrs_gpu, config.num_max_nvl_chunked_recv_tokens,
                             barrier_signal_ptrs_gpu, rank, comm_stream,
                             config.get_internode_rdma_buffer_size_hint(hidden_int4 * sizeof(int4), num_ranks),
                             num_nvl_bytes, false, false, internode_ctx->gpu_ctx);

    // Assign bias pointers
    auto bias_opts = std::vector<std::optional<torch::Tensor>>({bias_0, bias_1});
    void* bias_ptrs[2] = {nullptr, nullptr};
    for (int i = 0; i < 2; ++i) if (bias_opts[i].has_value()) {
        auto bias = bias_opts[i].value();
        EP_HOST_ASSERT(bias.dim() == 2 and bias.is_contiguous());
        EP_HOST_ASSERT(bias.scalar_type() == x.scalar_type());
        EP_HOST_ASSERT(bias.size(0) == num_combined_tokens and bias.size(1) == hidden);
        bias_ptrs[i] = bias.data_ptr();
    }

    // Launch data combine
    auto combined_x = torch::empty({num_combined_tokens, hidden}, x.options());
    internode::combine(at::cuda::ScalarTypeToCudaDataType(x.scalar_type()),
                       combined_x.data_ptr(), combined_topk_weights_ptr,
                       is_combined_token_in_rank.data_ptr<bool>(),
                       x.data_ptr(), topk_weights_ptr, bias_ptrs[0], bias_ptrs[1],
                       combined_rdma_head.data_ptr<int>(), combined_nvl_head.data_ptr<int>(),
                       src_meta.data_ptr(), rdma_channel_prefix_matrix.data_ptr<int>(), rdma_rank_prefix_sum.data_ptr<int>(), gbl_channel_prefix_matrix.data_ptr<int>(),
                       num_tokens, num_combined_tokens, hidden, num_topk,
                       rdma_buffer_ptr, config.num_max_rdma_chunked_send_tokens, config.num_max_rdma_chunked_recv_tokens,
                       buffer_ptrs_gpu, config.num_max_nvl_chunked_send_tokens, config.num_max_nvl_chunked_recv_tokens,
                       rank, num_ranks, comm_stream, num_channels, false, internode_ctx->gpu_ctx);

    HOST_LOG_DEBUG("internode_combine finished");

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        event = EventHandle(comm_stream);
        for (auto& t: {x, src_meta,
                       is_combined_token_in_rank, rdma_channel_prefix_matrix, rdma_rank_prefix_sum, gbl_channel_prefix_matrix,
                       combined_x, combined_rdma_head, combined_nvl_head}) {
            t.record_stream(comm_stream);
            if (allocate_on_comm_stream)
                t.record_stream(compute_stream);
        }
        for (auto& to: {topk_weights, combined_topk_weights, bias_0, bias_1}) {
            to.has_value() ? to->record_stream(comm_stream) : void();
            if (allocate_on_comm_stream)
                to.has_value() ? to->record_stream(compute_stream) : void();
        }
    } else {
        stream_wait(compute_stream, comm_stream);
    }

    // Switch back compute stream
    if (allocate_on_comm_stream)
        at::cuda::setCurrentCUDAStream(compute_stream);

    // Return values
    return {combined_x, combined_topk_weights, event};
}

} // namespace nixl_ep

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.doc() = "NIXL_EP: an efficient expert-parallel communication library";
    m.def("get_low_latency_rdma_size_hint", &nixl_ep::get_low_latency_rdma_size_hint);

    pybind11::class_<nixl_ep::EventHandle>(m, "EventHandle")
        .def(pybind11::init<>())
        .def("current_stream_wait", &nixl_ep::EventHandle::current_stream_wait);

    pybind11::class_<nixl_ep::Buffer>(m, "Buffer")
        .def(pybind11::init<int, bool, bool>())
        .def("update_memory_buffers", &nixl_ep::Buffer::update_memory_buffers)
        .def("barrier", &nixl_ep::Buffer::barrier)
        .def("connect_ranks", &nixl_ep::Buffer::connect_ranks, py::arg("remote_ranks"))
        .def("disconnect_ranks", &nixl_ep::Buffer::disconnect_ranks)
        .def("is_available", &nixl_ep::Buffer::is_available)
        .def("get_local_device_id", &nixl_ep::Buffer::get_local_device_id)
        .def("get_local_buffer_tensor", &nixl_ep::Buffer::get_local_buffer_tensor)
        .def("get_comm_stream", &nixl_ep::Buffer::get_comm_stream)
        .def("destroy", &nixl_ep::Buffer::destroy)
        .def("low_latency_dispatch", &nixl_ep::Buffer::low_latency_dispatch)
        .def("low_latency_combine", &nixl_ep::Buffer::low_latency_combine)
        .def("update_mask_buffer", &nixl_ep::Buffer::update_mask_buffer)
        .def("query_mask_buffer", &nixl_ep::Buffer::query_mask_buffer)
        .def("clean_mask_buffer", &nixl_ep::Buffer::clean_mask_buffer)
        .def("clean_low_latency_buffer", &nixl_ep::Buffer::clean_low_latency_buffer)
        .def("get_next_low_latency_combine_buffer", &nixl_ep::Buffer::get_next_low_latency_combine_buffer)
        .def("get_num_rdma_ranks", &nixl_ep::Buffer::get_num_rdma_ranks)
        .def("get_dispatch_layout", &nixl_ep::Buffer::get_dispatch_layout)
        .def("internode_dispatch", &nixl_ep::Buffer::internode_dispatch)
        .def("internode_combine", &nixl_ep::Buffer::internode_combine);

    // Config class for high-throughput kernels
    pybind11::class_<nixl_ep::Config>(m, "Config")
        .def(pybind11::init<int, int, int, int, int>(),
             py::arg("num_sms") = 20,
             py::arg("num_max_nvl_chunked_send_tokens") = 6, py::arg("num_max_nvl_chunked_recv_tokens") = 256,
             py::arg("num_max_rdma_chunked_send_tokens") = 6, py::arg("num_max_rdma_chunked_recv_tokens") = 256)
        .def("get_nvl_buffer_size_hint", &nixl_ep::Config::get_nvl_buffer_size_hint)
        .def("get_internode_rdma_buffer_size_hint", &nixl_ep::Config::get_internode_rdma_buffer_size_hint);

    m.attr("topk_idx_t") = pybind11::cast(c10::CppTypeToScalarType<nixl_ep::topk_idx_t>::value);
    m.def("is_sm90_compiled", nixl_ep::is_sm90_compiled);
}
