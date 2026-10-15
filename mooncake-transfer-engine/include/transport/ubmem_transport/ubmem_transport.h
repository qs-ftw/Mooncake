// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// UbmemTransport — UB 共享内存设备（obmm shmdev）传输层。
//
// 结构上仿 cxl_transport：整设备窗 mmap + offset 语义 + 同步内联拷贝；
// 介质差异带来的两点扩展：
//   1. 跨节点：对端段发布 ubmem_name（设备路径），提交方经本端镜像设备
//      （MC_UBMEM_PEER_DEV_MAP 解析）按同一 offset 访问——镜像对两侧
//      offset 空间一致，物理内存同一块。
//   2. 拷贝引擎：设备窗读采用 8 深独立 load 流水（nc 内存上 glibc memcpy
//      不流水，单线程差 30 倍；组合与 kvc-sdk/core/ub/kvc_ub.c 生产实证
//      路径一致：O_RDWR 优先、O_SYNC fallback + 流水拷贝）。
//
// env 契约：
//   MC_UBMEM_DEV_PATH      本进程池设备（如 /dev/obmm_shmdev1，必设）
//   MC_UBMEM_DEV_SIZE      窗口字节数（缺省 lseek 探测）
//   MC_UBMEM_PEER_DEV_MAP  跨节点镜像解析
//                          "peerhost:/dev/obmm_shmdev1=/dev/obmm_shmdev5,..."
//                          （对端主机:对端设备=本端镜像设备）
//   MC_UBMEM_FORCE_O_SYNC  诊断开关：两侧强制 O_SYNC 映射（对齐
//                          kvc_ub.c 的 KVC_FORCE_O_SYNC 定位实验）
//   MC_UBMEM_INLINE_MEMCPY 内联拷贝引擎：默认 1=glibc memcpy（cacheable/
//                          nc 均优）；置 0 恢复 nc→8 深流水（仅单线程
//                          nc 场景 +2%）
//
// 实现注记：解析缓存与 INLINE_MEMCPY 旋钮以 static 成员实现（不参与
// sizeof），nc 标志经私有方法参数传递——刻意保持实例布局零变化：本
// 传输层对象大小一旦改变，会在当前部署上引爆一处与本传输层无关的
// 潜伏堆越界（CopyPool 首个线程创建时 malloc 报 invalid size；改回
// 原布局即消失，valgrind 因 glibc 无 debuginfo 暂不可用，根因待查）。
// 类级 static 缓存按裸段 id 索引，故进程内至多一个存活实例：install()
// 以进程级守卫拒绝第二实例并存（响亮失败），析构清缓存并释放旗标以
// 允许顺序"销毁→重建"。
//
// v1 范围：单协议 segment（protocol="ubmem"），同步内联完成；不参与
// 多协议组合路由，不含分配器（buffer 为手工注册窗口，cxl 同款）。

#ifndef UBMEM_TRANSPORT_H_
#define UBMEM_TRANSPORT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferMetadata;

class UbmemTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

   public:
    UbmemTransport();

    ~UbmemTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    void *getUbmemBaseAddr() { return own_base_; }

    size_t getUbmemDevSize() { return own_size_; }

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo);

    int allocateLocalSegmentID();

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void *addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    const char *getName() const override { return "ubmem"; }

    // 设备窗映射（本端池设备 + 惰性建立的跨节点镜像）。
    struct Mapping {
        void *base = nullptr;
        uint64_t size = 0;
    };

    int ubmemDevInit();

    static uint64_t probeDevSize(const char *path);

    // 8 深独立 load 流水拷贝（nc 内存必需；cacheable 上不劣于 memcpy）。
    static void ubmemCopy(void *dest, const void *source, size_t size);

    static void *mapDeviceWindow(const char *path, uint64_t bytes);

    // 单 request 同步执行：边界校验 + 按 opcode 拷贝（大块走并行池）+ 屏障。
    // target_base = 段属主的窗基址（VA）；request.target_offset 是属主侧
    // 虚拟地址，须先减 target_base 得窗内偏移（本端映射基址可能不同）。
    // nc = 目标窗是否走 O_SYNC 强序语义（镜像窗；按映射类型选拷贝引擎）。
    int ubmemExecuteOne(Mapping *mapping, const TransferRequest &request,
                        std::string &error, uint64_t target_base = 0,
                        bool nc = false);

    // 大块拷贝的常驻并行分片池。nc 读单线程上限 ~2GB/s（non-posted load
    // 串行），多线程近线性；posted 写亦受益。完成语义不变：copy 返回即
    // 拷贝完成（同步内联）。单任务槽 + 派发互斥：并发大块拷贝串行化
    // （当前提交方为单线程轮询模型，无争用场景）。
    // MC_UBMEM_COPY_THREADS 调 worker 数（默认 8，0 关闭）。
    class CopyPool {
       public:
        ~CopyPool();
        void start(int workers);
        void stop();
        bool ready() const { return !stop_flag_ && workers_ > 0; }
        void copy(void *dest, const void *src, size_t bytes);

       private:
        void workerMain(int index);

        std::vector<std::thread> threads_;
        std::mutex dispatch_mu_;  // 同一时刻最多一个在途大块拷贝
        std::mutex mu_;           // 任务槽 + 世代
        std::condition_variable wake_cv_;
        std::condition_variable done_cv_;
        const uint8_t *task_src_ = nullptr;
        uint8_t *task_dst_ = nullptr;
        size_t task_bytes_ = 0;
        uint64_t gen_ = 0;
        size_t done_count_ = 0;
        bool stop_flag_ = false;
        int workers_ = 0;
    };

    static const size_t kParallelThreshold = 1ull << 20;

    // 段解析缓存（static 成员：不参与 sizeof，实例布局零变化——见类
    // 顶部"实现注记"）。每段首个 request 走慢路径解析一次，之后按段
    // id 无锁命中。SegmentID 本进程自 1 单调稠密；超容 id 回落慢路径。
    // 只缓存成功解析；失败每次重试。mappings_ 为节点式容器，元素指针
    // 插入后稳定，条目进程生命周期有效。
    struct Resolved {
        Mapping *mapping;
        uint64_t target_base;
        bool nc;  // 目标窗是否 O_SYNC 强序（镜像 true；本地池 false）
    };
    static constexpr size_t kResolveCacheSize = 4096;
    static inline std::atomic<Resolved *> resolve_cache_[kResolveCacheSize]{};

    // MC_UBMEM_INLINE_MEMCPY（ubmemDevInit 读一次；同上 static 存储；
    // 默认 1=memcpy，置 0 恢复 nc→流水）
    static inline bool inline_memcpy_ = true;

    Mapping *resolveTargetMapping(const TransferRequest &request,
                                  std::string &error,
                                  uint64_t *target_base = nullptr,
                                  bool *nc = nullptr);

    Mapping *getOrCreateMapping(const std::string &path, uint64_t bytes);

    void loadPeerDevMap();

   private:
    std::string own_path_;
    void *own_base_ = nullptr;
    uint64_t own_size_ = 0;

    CopyPool copy_pool_;

    std::mutex mappings_mu_;
    // 设备路径 → 窗口映射（含本端池设备与跨节点镜像，读多写少，惰性建）
    std::unordered_map<std::string, Mapping> mappings_;

    // "peer_server_name\x1Fpeer_device_path" → 本端镜像设备路径
    std::unordered_map<std::string, std::string> peer_dev_map_;
};
}  // namespace mooncake

#endif
