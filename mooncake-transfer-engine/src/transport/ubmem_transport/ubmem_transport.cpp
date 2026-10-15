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

#include "transport/ubmem_transport/ubmem_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace mooncake {

namespace {

// 进程级"至多一个存活实例"守卫：resolve_cache_/inline_memcpy_ 为类级
// static（零布局考量，见 .h 实现注记），两个实例并存时各自引擎的段
// id 空间（都从 1 编号）会在共享缓存互踩。当前部署恒单实例；此守卫把
// 多实例并存变成安装期响亮失败。顺序"销毁→重建"合法：析构清缓存并
// 释放旗标。
std::atomic<bool> g_ubmem_installed{false};

// "peerhost:/dev/obmm_shmdev1=/dev/obmm_shmdev5,..." →
// {"peerhost\x1F/dev/obmm_shmdev1": "/dev/obmm_shmdev5", ...}
std::unordered_map<std::string, std::string> parsePeerDevMap(
    const char *spec) {
    std::unordered_map<std::string, std::string> out;
    if (!spec || !*spec)
        return out;
    std::string s(spec);
    size_t beg = 0;
    for (;;) {
        size_t end = s.find(',', beg);
        std::string entry =
            s.substr(beg, end == std::string::npos ? std::string::npos
                                                   : end - beg);
        size_t sep = entry.find('=');
        size_t colon = entry.find(':');
        if (sep != std::string::npos && colon != std::string::npos &&
            colon < sep) {
            std::string peer = entry.substr(0, colon);
            std::string remote = entry.substr(colon + 1, sep - colon - 1);
            std::string local = entry.substr(sep + 1);
            auto trim = [](std::string &v) {
                size_t b = v.find_first_not_of(' ');
                size_t e = v.find_last_not_of(' ');
                v = b == std::string::npos ? "" : v.substr(b, e - b + 1);
            };
            trim(peer);
            trim(remote);
            trim(local);
            if (!peer.empty() && !remote.empty() && !local.empty())
                out[peer + "\x1f" + remote] = local;
        }
        if (end == std::string::npos)
            break;
        beg = end + 1;
    }
    return out;
}

}  // namespace

UbmemTransport::UbmemTransport() {
    const char *env_path = std::getenv("MC_UBMEM_DEV_PATH");
    if (!env_path || !*env_path) {
        // install() 阶段才硬性报错；构造保持轻量（对齐 cxl 构造语义）
        return;
    }
    own_path_ = env_path;
}

UbmemTransport::~UbmemTransport() {
    copy_pool_.stop();
    {
        std::lock_guard<std::mutex> lock(mappings_mu_);
        for (auto &kv : mappings_) {
            if (kv.second.base && kv.second.size)
                munmap(kv.second.base, kv.second.size);
        }
        mappings_.clear();
    }
    // 清空解析缓存并释放单实例旗标：段 id 空间随引擎销毁作废，残留在
    // static 缓存里会污染后续新实例（新引擎 id 同样从 1 编号）；顺序
    // "销毁→重建"因此合法。
    for (auto &slot : resolve_cache_) {
        Resolved *r = slot.exchange(nullptr, std::memory_order_acq_rel);
        delete r;
    }
    g_ubmem_installed.store(false, std::memory_order_release);
    own_base_ = nullptr;
    metadata_->removeSegmentDesc(local_server_name_);
}

UbmemTransport::CopyPool::~CopyPool() { stop(); }

void UbmemTransport::CopyPool::start(int workers) {
    if (workers <= 0 || !threads_.empty())
        return;
    workers_ = workers;
    for (int i = 0; i < workers; i++)
        threads_.emplace_back([this, i]() { workerMain(i); });
}

void UbmemTransport::CopyPool::stop() {
    if (threads_.empty())
        return;
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_flag_ = true;
    }
    wake_cv_.notify_all();
    for (auto &t : threads_)
        if (t.joinable())
            t.join();
    threads_.clear();
    workers_ = 0;
    stop_flag_ = false;
}

void UbmemTransport::CopyPool::workerMain(int index) {
    uint64_t my_gen = 0;
    for (;;) {
        std::unique_lock<std::mutex> lock(mu_);
        wake_cv_.wait(lock, [&] { return gen_ != my_gen || stop_flag_; });
        if (stop_flag_)
            return;
        const uint8_t *src = task_src_;
        uint8_t *dst = task_dst_;
        size_t bytes = task_bytes_;
        my_gen = gen_;
        lock.unlock();

        size_t parts = static_cast<size_t>(workers_) + 1;
        size_t chunk = (bytes + parts - 1) / parts;
        chunk = (chunk + 63) & ~static_cast<size_t>(63);
        size_t beg = static_cast<size_t>(index) * chunk;
        size_t end = std::min(beg + chunk, bytes);
        if (beg < end)
            UbmemTransport::ubmemCopy(dst + beg, src + beg, end - beg);

        lock.lock();
        if (++done_count_ == static_cast<size_t>(workers_))
            done_cv_.notify_all();
    }
}

void UbmemTransport::CopyPool::copy(void *dest, const void *src,
                                    size_t bytes) {
    std::lock_guard<std::mutex> dispatch_lock(dispatch_mu_);
    {
        std::lock_guard<std::mutex> lock(mu_);
        task_src_ = static_cast<const uint8_t *>(src);
        task_dst_ = static_cast<uint8_t *>(dest);
        task_bytes_ = bytes;
        done_count_ = 0;
        ++gen_;
    }
    wake_cv_.notify_all();

    // 提交线程承担最后一片，与 worker 并行
    size_t parts = static_cast<size_t>(workers_) + 1;
    size_t chunk = (bytes + parts - 1) / parts;
    chunk = (chunk + 63) & ~static_cast<size_t>(63);
    size_t beg = static_cast<size_t>(workers_) * chunk;
    if (beg < bytes)
        ubmemCopy(static_cast<uint8_t *>(dest) + beg,
                  static_cast<const uint8_t *>(src) + beg, bytes - beg);

    std::unique_lock<std::mutex> lock(mu_);
    done_cv_.wait(lock, [&] {
        return done_count_ == static_cast<size_t>(workers_);
    });
}

uint64_t UbmemTransport::probeDevSize(const char *path) {
    const char *env_size = std::getenv("MC_UBMEM_DEV_SIZE");
    if (env_size && *env_size) {
        char *end = nullptr;
        unsigned long long val = strtoull(env_size, &end, 10);
        if (end != env_size && *end == '\0')
            return static_cast<uint64_t>(val);
    }
    int fd = open(path, O_RDWR);
    if (fd < 0)
        return 0;
    off_t end = lseek(fd, 0, SEEK_END);
    close(fd);
    return end < 0 ? 0 : static_cast<uint64_t>(end);
}

// 映射语义与 kvc-sdk/core/ub/kvc_ub.c 生产实证路径一致：
// O_RDWR 优先，O_RDWR|O_SYNC fallback；MC_UBMEM_FORCE_O_SYNC 诊断反转。
void *UbmemTransport::mapDeviceWindow(const char *path, uint64_t bytes) {
    if (!path || !*path || bytes == 0)
        return nullptr;
    int open_flags[2] = {O_RDWR, O_RDWR | O_SYNC};
    int nflags = 2;
    if (std::getenv("MC_UBMEM_FORCE_O_SYNC") != nullptr) {
        open_flags[0] = O_RDWR | O_SYNC;
        open_flags[1] = O_RDWR;
    }
    int fd = -1;
    for (int i = 0; i < nflags; i++) {
        fd = open(path, open_flags[i]);
        if (fd >= 0)
            break;
    }
    if (fd < 0) {
        LOG(ERROR) << "UbmemTransport: cannot open device " << path
                   << " (" << strerror(errno) << ")";
        return nullptr;
    }
    void *ptr = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "UbmemTransport: mmap failed on " << path << " bytes="
                   << bytes << " (" << strerror(errno) << ")";
        return nullptr;
    }
    return ptr;
}

void UbmemTransport::ubmemCopy(void *dest, const void *source, size_t size) {
    const uint64_t *s = static_cast<const uint64_t *>(source);
    uint64_t *d = static_cast<uint64_t *>(dest);
    while (size >= 64) {
        // 8 个独立 load（无地址/数据依赖）→ 乱序引擎可 8 深度流水发出；
        // nc 设备内存上 glibc memcpy 单线程差 30 倍。
        uint64_t a0 = s[0], a1 = s[1], a2 = s[2], a3 = s[3];
        uint64_t a4 = s[4], a5 = s[5], a6 = s[6], a7 = s[7];
        d[0] = a0; d[1] = a1; d[2] = a2; d[3] = a3;
        d[4] = a4; d[5] = a5; d[6] = a6; d[7] = a7;
        s += 8; d += 8; size -= 64;
    }
    while (size >= 8) {
        *d++ = *s++;
        size -= 8;
    }
    if (size > 0) {
        const unsigned char *b = reinterpret_cast<const unsigned char *>(s);
        unsigned char *e = reinterpret_cast<unsigned char *>(d);
        while (size--)
            *e++ = *b++;
    }
}

int UbmemTransport::ubmemDevInit() {
    if (own_path_.empty()) {
        LOG(ERROR) << "UbmemTransport: MC_UBMEM_DEV_PATH is not set";
        return -1;
    }
    own_size_ = probeDevSize(own_path_.c_str());
    if (own_size_ == 0) {
        LOG(ERROR) << "UbmemTransport: cannot determine device size for "
                   << own_path_ << " (set MC_UBMEM_DEV_SIZE)";
        return -1;
    }
    own_base_ = mapDeviceWindow(own_path_.c_str(), own_size_);
    if (!own_base_)
        return -1;
    {
        std::lock_guard<std::mutex> lock(mappings_mu_);
        Mapping &m = mappings_[own_path_];
        m.base = own_base_;
        m.size = own_size_;
    }
    // 默认内联拷贝走 glibc memcpy：高并发（≥8 线程）cacheable/nc 均
    // 胜 8 深流水（nc 实测 8266 vs 7900 @128；单线程流水仅 +2% 不值）。
    // MC_UBMEM_INLINE_MEMCPY=0 恢复 nc→流水。
    {
        const char *env = std::getenv("MC_UBMEM_INLINE_MEMCPY");
        inline_memcpy_ = !(env && *env == '0');
    }
    LOG(INFO) << "UbmemTransport: pool device " << own_path_
              << " mapped base=" << own_base_ << " size=" << own_size_
              << " inline_memcpy=" << inline_memcpy_;
    return 0;
}

void UbmemTransport::loadPeerDevMap() {
    peer_dev_map_ = parsePeerDevMap(std::getenv("MC_UBMEM_PEER_DEV_MAP"));
    if (!peer_dev_map_.empty())
        LOG(INFO) << "UbmemTransport: " << peer_dev_map_.size()
                  << " peer mirror mapping(s) configured";
}

int UbmemTransport::install(std::string &local_server_name,
                            std::shared_ptr<TransferMetadata> meta,
                            std::shared_ptr<Topology> topo) {
    bool expected = false;
    if (!g_ubmem_installed.compare_exchange_strong(expected, true)) {
        LOG(ERROR) << "UbmemTransport: a live instance already exists in this "
                      "process; second instance is unsupported (class-level "
                      "resolve cache is not instance-keyed)";
        return -1;
    }
    auto fail = [&](int rc) {
        g_ubmem_installed.store(false, std::memory_order_release);
        return rc;
    };

    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = ubmemDevInit();
    if (ret)
        return fail(ret);

    loadPeerDevMap();

    {
        int copy_threads = 8;
        const char *env_threads = std::getenv("MC_UBMEM_COPY_THREADS");
        if (env_threads && *env_threads) {
            char *end = nullptr;
            long val = strtol(env_threads, &end, 10);
            if (end != env_threads && *end == '\0' && val >= 0 && val <= 64)
                copy_threads = static_cast<int>(val);
        }
        copy_pool_.start(copy_threads);
        if (copy_pool_.ready())
            LOG(INFO) << "UbmemTransport: parallel copy pool started with "
                      << copy_threads << " worker(s)";
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "UbmemTransport: cannot allocate local segment";
        return fail(ret);
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "UbmemTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return fail(ret);
    }

    return 0;
}

int UbmemTransport::allocateLocalSegmentID() {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc)
        desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
    desc->protocol = "ubmem";
    desc->ubmem_name = own_path_;
    // 本端映射地址：仅供本端 unregisterLocalMemory 匹配 offset+base；
    // 对端忽略此值（各自解析镜像 + offset）。
    desc->ubmem_base_addr = (uint64_t)own_base_;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int UbmemTransport::registerLocalMemory(void *addr, size_t length,
                                        const std::string &location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    (void)remote_accessible;
    BufferDesc ubmem_buffer_desc;
    ubmem_buffer_desc.name = local_server_name_;

    uintptr_t base = reinterpret_cast<uintptr_t>(own_base_);
    uintptr_t end = base + own_size_;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);
    uintptr_t ptr_end = ptr + length;
    // check addr legal
    if (ptr < base || ptr >= end) {
        errno = EFAULT;
        return -1;
    }
    // check overflow
    if (ptr_end > end || ptr_end < ptr) {
        errno = EOVERFLOW;
        return -1;
    }

    ubmem_buffer_desc.offset = (uint64_t)addr - (uint64_t)own_base_;
    ubmem_buffer_desc.length = length;
    return metadata_->addLocalMemoryBuffer(ubmem_buffer_desc, update_metadata);
}

int UbmemTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int UbmemTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret)
            return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int UbmemTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int first_error = 0;
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error)
            first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

UbmemTransport::Mapping *UbmemTransport::getOrCreateMapping(
    const std::string &path, uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mappings_mu_);
    auto it = mappings_.find(path);
    if (it != mappings_.end())
        return &it->second;
    // 镜像窗口尺寸按本端池窗对称（镜像对两侧窗口一致的部署约定）
    uint64_t size = bytes ? bytes : own_size_;
    void *base = mapDeviceWindow(path.c_str(), size);
    if (!base)
        return nullptr;
    Mapping &m = mappings_[path];
    m.base = base;
    m.size = size;
    LOG(INFO) << "UbmemTransport: mirror device " << path
              << " mapped base=" << base << " size=" << size;
    return &m;
}

UbmemTransport::Mapping *UbmemTransport::resolveTargetMapping(
    const TransferRequest &request, std::string &error,
    uint64_t *target_base, bool *nc) {
    // 快路径：命中段解析缓存——零锁、零字符串解析（对齐 cxl 的
    // "基址+偏移"直达；曾以每 request 全局互斥查表+host 解析，128 线程
    // 下比 cxl 慢 ~15% 的主因）。
    const size_t sid = static_cast<size_t>(request.target_id);
    if (sid < kResolveCacheSize) {
        Resolved *r = resolve_cache_[sid].load(std::memory_order_acquire);
        if (r) {
            if (target_base)
                *target_base = r->target_base;
            if (nc)
                *nc = r->nc;
            return r->mapping;
        }
    }

    auto segment_desc = metadata_->getSegmentDescByID(request.target_id);
    if (!segment_desc) {
        error = "invalid target segment id " +
                std::to_string(request.target_id);
        return nullptr;
    }
    if (segment_desc->protocol != "ubmem") {
        error = "target segment protocol is not ubmem (" +
                segment_desc->protocol + ")";
        return nullptr;
    }
    if (segment_desc->ubmem_name.empty()) {
        error = "target segment has no ubmem_name";
        return nullptr;
    }
    if (target_base)
        *target_base = segment_desc->ubmem_base_addr;

    Mapping *mapping = nullptr;
    // 目标窗语义：镜像窗（跨节点）恒为 O_SYNC 强序；本地池窗 cacheable
    // （MC_UBMEM_FORCE_O_SYNC 强制时除外——与 mapDeviceWindow 的开窗
    // 旗标逻辑一致）。
    bool is_nc = std::getenv("MC_UBMEM_FORCE_O_SYNC") != nullptr;
    /* 同机判定按 host 部分比较：段名与本端名各带自己的端口（如
     * 127.0.0.1:13536 vs 127.0.0.1:23011），全串比对会把同机对端误判为
     * 跨节点而去找镜像设备。 */
    auto host_of = [](const std::string &ep) {
        auto scheme = ep.find("://");
        size_t beg = scheme == std::string::npos ? 0 : scheme + 3;
        auto colon = ep.find(':', beg);
        return ep.substr(beg, colon == std::string::npos
                                   ? std::string::npos
                                   : colon - beg);
    };
    if (segment_desc->name == local_server_name_ ||
        host_of(segment_desc->name) == host_of(local_server_name_)) {
        // 本机目标：直接映射目标设备（cacheable 语义，O_RDWR 优先）
        mapping = getOrCreateMapping(segment_desc->ubmem_name, 0);
        if (!mapping)
            error = "cannot map local device " + segment_desc->ubmem_name;
    } else {
        // 跨节点目标：经本端镜像设备访问对端内存（offset 空间一致）。
        // 查表键取段名的 host 部分——MC_UBMEM_PEER_DEV_MAP 的键是
        // "peerhost:remotedev=localdev"（无端口），而 store 侧段名带端口。
        auto it = peer_dev_map_.find(host_of(segment_desc->name) + "\x1f" +
                                     segment_desc->ubmem_name);
        if (it == peer_dev_map_.end()) {
            error = "no mirror configured for peer " + segment_desc->name +
                    " device " + segment_desc->ubmem_name +
                    " (set MC_UBMEM_PEER_DEV_MAP)";
            return nullptr;
        }
        mapping = getOrCreateMapping(it->second, 0);
        if (!mapping)
            error = "cannot map mirror device " + it->second;
        else
            is_nc = true;
    }
    if (mapping) {
        if (nc)
            *nc = is_nc;
        if (sid < kResolveCacheSize) {
            // 并发首解析可能重复建条目，CAS 败者丢弃（值等价）。
            Resolved *r = new Resolved{mapping, segment_desc->ubmem_base_addr,
                                       is_nc};
            Resolved *expected = nullptr;
            if (!resolve_cache_[sid].compare_exchange_strong(expected, r))
                delete r;
        }
    }
    return mapping;
}

Status UbmemTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UbmemTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

// 单个 request 的同步执行：解析目标映射 → 边界校验 → 按窗语义选引擎拷贝。
// 返回 0 成功；失败写 error（slice 标 FAILED，任务不中断）。
int UbmemTransport::ubmemExecuteOne(Mapping *mapping,
                                    const TransferRequest &request,
                                    std::string &error, uint64_t target_base,
                                    bool nc) {
    if (!mapping) {
        error = "no mapping";
        return -1;
    }
    if (request.target_offset < target_base) {
        error = "target offset below owner window base: offset=" +
                std::to_string(request.target_offset) + " base=" +
                std::to_string(target_base);
        return -1;
    }
    uint64_t offset = request.target_offset - target_base;
    uint64_t length = request.length;
    if (offset + length > mapping->size || offset + length < offset) {
        error = "target offset/length out of window bounds: offset=" +
                std::to_string(offset) + " length=" + std::to_string(length) +
                " window=" + std::to_string(mapping->size);
        return -1;
    }
    char *device_side = static_cast<char *>(mapping->base) + offset;
    // WRITE: 本地内存 → 设备窗；READ: 设备窗 → 本地内存
    const void *copy_src =
        request.opcode == TransferRequest::WRITE
            ? static_cast<const void *>(request.source)
            : static_cast<const void *>(device_side);
    void *copy_dst = request.opcode == TransferRequest::WRITE
                         ? static_cast<void *>(device_side)
                         : static_cast<void *>(request.source);
    if (length >= kParallelThreshold && copy_pool_.ready()) {
        copy_pool_.copy(copy_dst, copy_src, length);
    } else if (!nc || inline_memcpy_) {
        // glibc NEON memcpy：高并发下 cacheable/nc 均胜软件流水
        // （nc 实测 8266 vs 7900 @128；对照 cxl 同窗 memcpy 7698）。
        memcpy(copy_dst, copy_src, length);
    } else {
        // nc 窗 + 显式关 memcpy：8 深独立 load 流水（仅单线程 nc +2%）。
        ubmemCopy(copy_dst, copy_src, length);
    }
    // 设备内存序屏障（对齐 cxl：确保设备侧写入对后续访问可见）
    __sync_synchronize();
    return 0;
}

Status UbmemTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "UbmemTransport: Exceed the limitation of current "
                      "batch's capacity";
        return Status::InvalidArgument(
            "UbmemTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);

        std::string error;
        uint64_t target_base = 0;
        bool nc = false;
        Mapping *mapping = resolveTargetMapping(request, error,
                                                &target_base, &nc);
        if (mapping && request.target_offset >= target_base)
            slice->ubmem.dest_addr =
                (uint64_t)((char *)mapping->base +
                           (request.target_offset - target_base));
        if (!mapping ||
            ubmemExecuteOne(mapping, request, error, target_base, nc) != 0) {
            LOG(ERROR) << "UbmemTransport: " << error;
            slice->markFailed();
        } else {
            slice->markSuccess();
        }
    }

    return Status::OK();
}

Status UbmemTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        task.total_bytes = request.length;

        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        std::string error;
        uint64_t target_base = 0;
        bool nc = false;
        Mapping *mapping = resolveTargetMapping(request, error,
                                                &target_base, &nc);
        if (mapping && request.target_offset >= target_base)
            slice->ubmem.dest_addr =
                (uint64_t)((char *)mapping->base +
                           (request.target_offset - target_base));
        if (!mapping ||
            ubmemExecuteOne(mapping, request, error, target_base, nc) != 0) {
            LOG(ERROR) << "UbmemTransport: " << error;
            slice->markFailed();
        } else {
            slice->markSuccess();
        }
    }
    return Status::OK();
}

}  // namespace mooncake
