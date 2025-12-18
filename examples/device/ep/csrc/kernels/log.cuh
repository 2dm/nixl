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

#ifdef ENABLE_DEBUG_LOGS
#define DEVICE_LOG_DEBUG(fmt, ...) printf("[DEBUG][%s] " fmt "\n", __func__, ##__VA_ARGS__)

#define _DEVICE_LOG_DEBUG_LANE_IMPL(lane, fmt, ...) do { \
    if (lane_id == (lane)) { \
        printf("[DEBUG][%s] " fmt "\n", __func__, ##__VA_ARGS__); \
    } \
} while(0)

#define DEVICE_LOG_DEBUG_LANE(lane, fmt, ...) _DEVICE_LOG_DEBUG_LANE_IMPL(lane, fmt, ##__VA_ARGS__)

#define DEVICE_LOG_DEBUG_LANE_SYNC(lane, fmt, ...) do { \
    _DEVICE_LOG_DEBUG_LANE_IMPL(lane, fmt, ##__VA_ARGS__); \
    __syncwarp(); \
} while(0)
#else
#define DEVICE_LOG_DEBUG(...)
#define DEVICE_LOG_DEBUG_LANE(...)
#define DEVICE_LOG_DEBUG_LANE_SYNC(...)
#endif

