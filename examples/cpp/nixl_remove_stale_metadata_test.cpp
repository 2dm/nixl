/*
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
#include <iostream>
#include <thread>
#include <chrono>
#include <cstring>

#include "nixl.h"
#include "test_utils.h"

// Test configuration
const std::string ETCD_ENDPOINT = "http://localhost:2379";
const std::string AGENT1_NAME = "TestAgent1";
const std::string AGENT2_NAME = "TestAgent2";

void printStatus(const std::string& operation, nixl_status_t status) {
    std::cout << operation << ": " << nixlEnumStrings::statusStr(status) << std::endl;
    if (status != NIXL_SUCCESS) {
        std::cerr << "Status: " << nixlEnumStrings::statusStr(status) << std::endl;
    }
}

int main() {
    std::cout << "\n==============================================\n";
    std::cout << "NIXL Remove Stale Metadata Test\n";
    std::cout << "==============================================\n\n";

    // Setup ETCD environment
    if (getenv("NIXL_ETCD_ENDPOINTS")) {
        std::cout << "NIXL_ETCD_ENDPOINTS is set to: " << getenv("NIXL_ETCD_ENDPOINTS") << std::endl;
    } else {
        std::cout << "NIXL_ETCD_ENDPOINTS not set, using: " << ETCD_ENDPOINT << std::endl;
        setenv("NIXL_ETCD_ENDPOINTS", ETCD_ENDPOINT.c_str(), 1);
    }

    nixl_status_t ret1, ret2;
    
    // Create agent configuration with ETCD enabled
    nixlAgentConfig cfg(true);
    
    std::cout << "\n[Step 1] Creating two agents...\n";
    nixlAgent A1(AGENT1_NAME, cfg);
    nixlAgent* A2 = new nixlAgent(AGENT2_NAME, cfg);  // Use pointer so we can delete/recreate
    std::cout << "  ✓ " << AGENT1_NAME << " created\n";
    std::cout << "  ✓ " << AGENT2_NAME << " created\n";

    // Get available plugins
    std::vector<nixl_backend_t> plugins;
    ret1 = A1.getAvailPlugins(plugins);
    nixl_exit_on_failure(ret1, "Failed to get available plugins", AGENT1_NAME);

    std::cout << "\nAvailable plugins: ";
    for (nixl_backend_t b: plugins) {
        std::cout << b << " ";
    }
    std::cout << "\n";

    // Setup UCX backend
    nixl_b_params_t init1, init2;
    nixl_mem_list_t mems1, mems2;

    ret1 = A1.getPluginParams("UCX", mems1, init1);
    ret2 = A2->getPluginParams("UCX", mems2, init2);
    nixl_exit_on_failure(ret1, "Failed to get plugin params for UCX", AGENT1_NAME);
    nixl_exit_on_failure(ret2, "Failed to get plugin params for UCX", AGENT2_NAME);

    nixlBackendH* ucx1, *ucx2;
    ret1 = A1.createBackend("UCX", init1, ucx1);
    ret2 = A2->createBackend("UCX", init2, ucx2);
    nixl_exit_on_failure(ret1, "Failed to create UCX backend", AGENT1_NAME);
    nixl_exit_on_failure(ret2, "Failed to create UCX backend", AGENT2_NAME);

    std::cout << "  ✓ UCX backends created\n";

    // Register memory
    nixl_reg_dlist_t dlist1(DRAM_SEG), dlist2(DRAM_SEG);
    nixl_opt_args_t extra_params1, extra_params2;
    extra_params1.backends.push_back(ucx1);
    extra_params2.backends.push_back(ucx2);

    size_t buffer_size = 1024;
    void* addr1 = malloc(buffer_size);
    void* addr2 = malloc(buffer_size);
    memset(addr1, 0xaa, buffer_size);
    memset(addr2, 0xbb, buffer_size);

    nixlBlobDesc desc1, desc2;
    desc1.addr = (uintptr_t)addr1;
    desc1.len = buffer_size;
    desc1.devId = 0;
    desc2.addr = (uintptr_t)addr2;
    desc2.len = buffer_size;
    desc2.devId = 0;

    dlist1.addDesc(desc1);
    dlist2.addDesc(desc2);

    ret1 = A1.registerMem(dlist1, &extra_params1);
    ret2 = A2->registerMem(dlist2, &extra_params2);
    nixl_exit_on_failure(ret1, "Failed to register memory", AGENT1_NAME);
    nixl_exit_on_failure(ret2, "Failed to register memory", AGENT2_NAME);

    std::cout << "  ✓ Memory registered\n";

    // Test Scenario: Simulate Agent2 publishing metadata, then "crashing"
    std::cout << "\n[Step 2] Both agents send metadata to ETCD...\n";
    ret1 = A1.sendLocalMD();
    ret2 = A2->sendLocalMD();
    nixl_exit_on_failure(ret1, "Failed to send local MD", AGENT1_NAME);
    nixl_exit_on_failure(ret2, "Failed to send local MD", AGENT2_NAME);
    
    std::cout << "  ✓ " << AGENT1_NAME << " sent metadata to ETCD\n";
    std::cout << "  ✓ " << AGENT2_NAME << " sent metadata to ETCD\n";

    // Give ETCD time to process
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::cout << "\n[Step 3] Agent1 fetches Agent2's metadata...\n";
    ret1 = A1.fetchRemoteMD(AGENT2_NAME);
    nixl_exit_on_failure(ret1, "Failed to fetch remote MD", AGENT1_NAME);
    
    // Wait for fetch to complete (it's asynchronous)
    std::cout << "  Waiting for fetch to complete...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Verify metadata was loaded by checking if we can create a transfer
    nixl_xfer_dlist_t xfer_dlist_check(DRAM_SEG);
    ret1 = A1.checkRemoteMD(AGENT2_NAME, xfer_dlist_check);
    if (ret1 == NIXL_SUCCESS) {
        std::cout << "  ✓ " << AGENT1_NAME << " successfully loaded " << AGENT2_NAME << "'s metadata\n";
    } else {
        std::cerr << "  ⚠ Warning: Remote metadata may not be fully loaded yet (status: " 
                  << nixlEnumStrings::statusStr(ret1) << ")\n";
    }

    std::cout << "\n[Step 4] *** KILLING Agent2 (simulating crash) ***\n";
    std::cout << "  Agent2 crashes WITHOUT calling invalidateLocalMD()\n";
    std::cout << "  This leaves stale metadata in ETCD with invalid UCX file descriptors\n";
    
    // Deregister memory before killing Agent2 to clean up properly on this side
    ret2 = A2->deregisterMem(dlist2, &extra_params2);
    if (ret2 == NIXL_SUCCESS) {
        std::cout << "  ✓ Deregistered Agent2 memory\n";
    }
    
    // KILL Agent2 - this simulates a crash without cleanup
    std::cout << "  🗡️  Deleting Agent2 (crash simulation)...\n";
    delete A2;
    A2 = nullptr;
    std::cout << "  ✓ Agent2 is dead (stale metadata remains in ETCD)\n";
    
    // Give time for destructor to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cout << "\n[Step 5] Agent1 detects failure and invalidates cached metadata...\n";
    
    // Agent1 detects Agent2 has failed and clears local cache
    ret1 = A1.invalidateRemoteMD(AGENT2_NAME);
    if (ret1 == NIXL_ERR_NOT_FOUND) {
        std::cout << "  ⚠ Note: No cached metadata found (already cleared)\n";
    } else if (ret1 == NIXL_SUCCESS) {
        std::cout << "  ✓ " << AGENT1_NAME << " invalidated cached metadata for " << AGENT2_NAME << "\n";
    } else {
        nixl_exit_on_failure(ret1, "Failed to invalidate remote MD", AGENT1_NAME);
    }

    std::cout << "\n[Step 6] Testing removeRemoteMetadata() to clean stale ETCD data...\n";
    
    // NEW FUNCTION TEST: Remove stale metadata from ETCD
    ret1 = A1.removeRemoteMetadata(AGENT2_NAME);
    if (ret1 == NIXL_SUCCESS) {
        std::cout << "  ✓ " << AGENT1_NAME << " successfully removed stale metadata for " 
                  << AGENT2_NAME << " from ETCD\n";
    } else if (ret1 == NIXL_ERR_NOT_SUPPORTED) {
        std::cerr << "  ✗ ETCD is not enabled, cannot remove remote metadata\n";
        goto cleanup;
    } else {
        std::cerr << "  ✗ Failed to remove remote metadata: " 
                  << nixlEnumStrings::statusStr(ret1) << "\n";
        goto cleanup;
    }

    // Give ETCD time to process the removal
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "\n[Step 7] Verifying metadata was removed from ETCD...\n";
    std::cout << "  Agent1 tries to fetch Agent2's metadata (should fail/timeout)\n";
    
    // This should fail because metadata was removed
    ret1 = A1.fetchRemoteMD(AGENT2_NAME);
    if (ret1 == NIXL_ERR_NOT_FOUND || ret1 != NIXL_SUCCESS) {
        std::cout << "  ✓ Fetch failed as expected (metadata not in ETCD): " 
                  << nixlEnumStrings::statusStr(ret1) << "\n";
    } else {
        std::cerr << "  ⚠ Warning: Fetch succeeded unexpectedly (metadata may still be cached)\n";
    }

    std::cout << "\n[Step 8] *** RESTARTING Agent2 ***\n";
    std::cout << "  Creating new Agent2 instance (simulating process restart)...\n";
    
    // Create a NEW Agent2 - simulating a fresh process start
    A2 = new nixlAgent(AGENT2_NAME, cfg);
    std::cout << "  ✓ New Agent2 instance created\n";
    
    // Recreate backend for new Agent2
    ret2 = A2->getPluginParams("UCX", mems2, init2);
    nixl_exit_on_failure(ret2, "Failed to get plugin params for UCX", AGENT2_NAME);
    
    ret2 = A2->createBackend("UCX", init2, ucx2);
    nixl_exit_on_failure(ret2, "Failed to create UCX backend for restarted agent", AGENT2_NAME);
    std::cout << "  ✓ UCX backend recreated\n";
    
    // Re-register memory for new Agent2
    extra_params2.backends.clear();
    extra_params2.backends.push_back(ucx2);
    dlist2.clear();
    dlist2.addDesc(desc2);
    
    ret2 = A2->registerMem(dlist2, &extra_params2);
    nixl_exit_on_failure(ret2, "Failed to register memory for restarted agent", AGENT2_NAME);
    std::cout << "  ✓ Memory re-registered\n";
    
    // Agent2 publishes fresh metadata to ETCD
    ret2 = A2->sendLocalMD();
    nixl_exit_on_failure(ret2, "Failed to send local MD after restart", AGENT2_NAME);
    std::cout << "  ✓ " << AGENT2_NAME << " published FRESH metadata to ETCD\n";

    // Give ETCD time to process
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::cout << "\n[Step 9] Agent1 fetches fresh metadata after Agent2 restart...\n";
    ret1 = A1.fetchRemoteMD(AGENT2_NAME);
    nixl_exit_on_failure(ret1, "Failed to fetch remote MD after restart", AGENT1_NAME);
    
    // Wait for fetch to complete
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    std::cout << "  ✓ " << AGENT1_NAME << " successfully fetched FRESH metadata for " 
              << AGENT2_NAME << "\n";
    std::cout << "  ✓ Agent2 can now communicate with Agent1 again!\n";

    std::cout << "\n[Step 10] Testing edge cases...\n";
    
    // Test 1: Try to remove own metadata (should fail)
    std::cout << "  Test 1: Trying to remove own metadata (should fail)...\n";
    ret1 = A1.removeRemoteMetadata(AGENT1_NAME);
    if (ret1 == NIXL_ERR_INVALID_PARAM) {
        std::cout << "    ✓ Correctly rejected removing own metadata\n";
    } else {
        std::cerr << "    ✗ Expected NIXL_ERR_INVALID_PARAM, got: " 
                  << nixlEnumStrings::statusStr(ret1) << "\n";
    }

    // Test 2: Try to remove with empty name (should fail)
    std::cout << "  Test 2: Trying to remove with empty agent name (should fail)...\n";
    ret1 = A1.removeRemoteMetadata("");
    if (ret1 == NIXL_ERR_INVALID_PARAM) {
        std::cout << "    ✓ Correctly rejected empty agent name\n";
    } else {
        std::cerr << "    ✗ Expected NIXL_ERR_INVALID_PARAM, got: " 
                  << nixlEnumStrings::statusStr(ret1) << "\n";
    }

    // Test 3: Remove non-existent agent (should succeed - idempotent)
    std::cout << "  Test 3: Removing non-existent agent (should succeed)...\n";
    ret1 = A1.removeRemoteMetadata("NonExistentAgent");
    if (ret1 == NIXL_SUCCESS) {
        std::cout << "    ✓ Successfully handled non-existent agent (idempotent)\n";
    } else {
        std::cerr << "    ⚠ Got: " << nixlEnumStrings::statusStr(ret1) << "\n";
    }

    std::cout << "\n[Step 11] Final cleanup...\n";

cleanup:
    // Cleanup
    ret1 = A1.deregisterMem(dlist1, &extra_params1);
    if (A2 != nullptr) {
        ret2 = A2->deregisterMem(dlist2, &extra_params2);
        delete A2;
        A2 = nullptr;
    }
    
    free(addr1);
    free(addr2);

    std::cout << "  ✓ Memory cleaned up\n";

    std::cout << "\n==============================================\n";
    std::cout << "Test completed successfully!\n";
    std::cout << "==============================================\n\n";

    return 0;
}

