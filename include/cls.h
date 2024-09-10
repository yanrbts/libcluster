/*
 * Copyright (c) 2024-2024, yanruibinghxu@gmail.com All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef __CLS_H__
#define __CLS_H__

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__clang__) || defined(__GNUC__)
#define CLS_API __attribute__((visibility("default")))
#elif defined(__SUNPRO_C) && (__SUNPRO_C >= 0x550) /* Sun Studio >= 8 */
#define CLS_API __global
#else
#define CLS_API
#endif

/**
 * cluster module init
 * 
 * @param port Service listening port(0~65535), 
 *             Cluster node cport = port + 10000
 * @param cf Cluster service configuration file, 
 *           This file configuration refers to 
 *           the redis cluster configuration file
 */
CLS_API void clsInit(int port, const char *cf);

/** 
 * Reset a node performing a soft or hard reset:
 *
 * 1) All other nodes are forget.
 * 2) All the assigned / open slots are released.
 * 3) If the node is a slave, it turns into a master.
 * 5) Only for hard reset: a new Node ID is generated.
 * 6) Only for hard reset: currentEpoch and configEpoch are set to 0.
 * 7) The new configuration is saved and the cluster state updated.
 * 8) If the node was a slave, the whole data set is flushed away. 
 * 
 * @param hard Only for hard reset: a new Node ID is generated. 
 *             Only for hard reset: currentEpoch and configEpoch are set to 0.
 */
CLS_API void clsReset(int hard);

/**
 * Start cluster service, This method always succeeds and returns 0.
 * This function is called after clsInit is called. This is the core function
 */
CLS_API int clsStart(void);

/**
 * Set the log printing standard
 * @param level Minimum printing standards,The minimum printing standard. 
 *              If the standard is lower than the level setting, the image 
 *              will not be printed.
 * LOG_EMERG	0	system is unusable 
 * LOG_ALERT	1	action must be taken immediately 
 * LOG_CRIT	    2	critical conditions
 * LOG_ERR		3	error conditions 
 * LOG_WARNING	4	warning conditions 
 * LOG_NOTICE	5	normal but significant condition 
 * LOG_INFO	    6	informational 
 * LOG_DEBUG	7	debug-level messages
 */
CLS_API void clsSetLogLevel(int level);

/**
 * Dynamically add cluster nodes. The added nodes will be written 
 * into the cluster configuration file. You can also directly write 
 * cluster nodes into the cluster configuration file. 
 * Check the clsInit function. If successful, it returns 0. Otherwise, it returns -1.
 * @param ip Node IP address
 * @param port Node listening port
 * @return If successful, it returns 0. Otherwise, it returns -1
 */
CLS_API int clsSetMeet(char *ip, int port);

/**
 * Return the node id
 * @return Return the node id
 * @warning The function return value does not need to be released, 
 *          the local node will always exist
 */
CLS_API char *clsGetSelfNodeId(void);

/**
 * return cluster configuration seen by node.
 * 
 * @return cluster configuration seen by node. Output format:",
 * <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ... <slot>"
 * @warning return char* Need to call clsFree() to release
 */
CLS_API char *clsGetNodesDescription(void);

/**
 * Release data, for example, the clsGetNodesDescription() 
 * function needs to call the clsFree() function to release it.
 * @param ptr ptr to be released
 */
CLS_API void clsFree(void *ptr);

/**
 * Used to obtain the slave node (slave) information of 
 * the specified master node (master) in the cluster
 * Returns a list containing information about all slave nodes under the specified master node. 
 * This information includes the slave node's Node ID, IP address, port number, status, etc.
 * @param masterid Master node id
 * @return Returns a list containing information about all slave nodes under the specified master node. 
 *         Otherwise, it returns NULL
 * @warning return char* Need to call clsFree() to release
 */
CLS_API char *clsGetSlaves(const char *masterid);

/**
 * obtain cluster infomation
 * @return Returns the cluster information string, or NULL if failed.
 */
CLS_API char *clsGetInfo(void);

#ifdef __cplusplus
}
#endif

#endif