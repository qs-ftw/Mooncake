#include "client_service.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"

#include <csignal>
#include <atomic>
#include <fstream>
#include <iterator>
#include "crc_checksum.h"
#include <algorithm>
#include <array>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iomanip>
#include <limits>
#ifdef USE_NOF
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>

#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "types.h"
#include "client_buffer.h"

// ============================================================================
// KVC slot-state flip adapter (hpc-redis kvc-sdk, P1)
//
// 把 Mooncake 写路径语义落到 KVC region 的 slot 状态机:
//   PutStart 成功   → mark_writing（分配到的 offset 对应 slot）
//   TransferWrite 成功 → mark_ready（release 语义发布, peer 可见）
//
// 环境变量门控（未设置则完全旁路, 零开销）:
//   KVC_FLIP_DEVICES   逗号分隔多个 shmdev 路径——同一对端 host 的多个
//                      region（多 owner daemon / 多 size class）; 读路径按
//                      buffer_address_ 落在哪段地址区间自动选 region
//   KVC_FLIP_DEVICE    单 region 形态（向后兼容; KVC_FLIP_DEVICES 优先）
//   KVC_FLIP_BLOCK    block size（可选; 仅作断言, 权威值来自 region header;
//                     仅单 region 形态生效）
//   KVC_FLIP_BYTES    region 总字节（可选; 同上）
//   T0-1: 几何/owner 身份自 shm header 自描述读取，与 master 元数据
//         （transport_endpoint host）交叉核验；不符 → T0-2 永久降级全走 TE
//
// slot 数学: replica.buffer_descriptor.buffer_address_ 即段内 offset（TE
// target_offset 同源）, slot_idx = offset / block。仅对单 slice 且
// offset 对齐 block 的对象生效（P1 约束）。
// ============================================================================
#include <kvc/kvc_server.h>
namespace kvcflip {
namespace {
/* 一个对端 host 的一个 region 视图: 各 region 是独立物理段, owner 发布的
 * 数据区地址区间互不重叠——地址 → region 的归属是唯一确定的。 */
struct RegionView {
    kvc_region_t *region;
    uint32_t block;
    uint64_t owner_base;    // owner 发布的数据区绝对基址（header 元数据）
    uint64_t data_bytes;
    uint32_t epoch;         // lazy_init/refresh 时的身代快照
    const char *data_base;  // 本进程映射的数据区基址
    bool borrowed = false;  // UBME: 复用本进程 owner region（不参与重建关闭）
};
std::vector<RegionView> g_regions;   // 同一对端 host 的全部 region
bool g_init_failed = false;   // 一次性放弃标志: 失败后不再重试, 全走 TE
char g_disable_reason[512];   // 永久降级原因（观测/门禁判定用）
uint64_t g_flip_count = 0, g_flip_fail = 0, g_direct_count = 0, g_direct_fallback = 0;
uint64_t g_guard_rejects = 0;   // 读后校验拦截数（观测用）
uint64_t g_epoch_refresh = 0;   // owner 重启身代刷新计数（观测用）
uint64_t g_checksum_rejects = 0;   // R5: 直读 checksum 不符回退 TE 的次数
uint64_t g_crc_verified_hits = 0;   // R5 策略4: 首验标记命中（免 CRC）次数

/* R8: 周期性计数器出口（60s 一条 LOG(INFO)，回答"直读为什么没生效/
 * 为什么回退"）。热路径只付一次 time(nullptr) 比较。 */
inline void metrics_tick() {
    static time_t last = 0;
    time_t now = ::time(nullptr);
    if (now - last < 60)
        return;
    last = now;
    if (g_init_failed)
        LOG(INFO) << "KVC-FLIP metrics: disabled=1 reason=\"" << g_disable_reason
                  << "\" flip_ok=" << g_flip_count
                  << " flip_fail=" << g_flip_fail
                  << " direct_ok=" << g_direct_count
                  << " direct_fallback=" << g_direct_fallback
                  << " guard_rejects=" << g_guard_rejects
                  << " checksum_rejects=" << g_checksum_rejects
                  << " crc_verified_hits=" << g_crc_verified_hits
                  << " epoch_refresh=" << g_epoch_refresh;
    else
        LOG(INFO) << "KVC-FLIP metrics: disabled=0"
                  << " flip_ok=" << g_flip_count
                  << " flip_fail=" << g_flip_fail
                  << " direct_ok=" << g_direct_count
                  << " direct_fallback=" << g_direct_fallback
                  << " guard_rejects=" << g_guard_rejects
                  << " checksum_rejects=" << g_checksum_rejects
                  << " crc_verified_hits=" << g_crc_verified_hits
                  << " epoch_refresh=" << g_epoch_refresh;
}


/* 配置的全部设备路径: KVC_FLIP_DEVICES（逗号分隔, 多 region）优先,
 * 回退 KVC_FLIP_DEVICE 单值（C2 daemon 场景再回退 KVC_SEGMENT_DEVICE）。 */
std::vector<std::string> device_list() {
    std::vector<std::string> out;
    const char *multi = std::getenv("KVC_FLIP_DEVICES");
    if (multi && *multi) {
        std::string s(multi);
        size_t beg = 0;
        for (;;) {
            size_t end = s.find(',', beg);
            std::string tok =
                s.substr(beg, end == std::string::npos ? std::string::npos
                                                       : end - beg);
            size_t b = tok.find_first_not_of(' ');
            size_t e = tok.find_last_not_of(' ');
            if (e != std::string::npos)
                out.push_back(tok.substr(b, e - b + 1));
            if (end == std::string::npos)
                break;
            beg = end + 1;
        }
        return out;
    }
    const char *dev = std::getenv("KVC_FLIP_DEVICE");
    if (!dev)   // C2: daemon 进程（驱逐推送执行端）只有 KVC_SEGMENT_DEVICE
        dev = std::getenv("KVC_SEGMENT_DEVICE");
    if (dev && *dev)
        out.push_back(dev);
    return out;
}

/* ---- UBME 动态段来源（kvc-sdk ABI v3）----
 * KVC_UBME_OWNER_HOSTS="host[,host...]": 未配置任何路径 env 时，按 owner
 * host 推导段名（kvc_seg_name）attach（同机 journal 复用优先，跨节点经
 * mesh PULL import），得到本地设备路径——下游 probe/open 流程不变。
 * ctx 参数: KVC_UBME_NODE_ID（默认 1）/ KVC_UBME_TCP_PORT（默认 0=内核
 * 自动分配，纯出向 reader）/ KVC_UBME_PEERS（"id:ip:port:cna,..."，跨
 * 节点必配; cna 裸值按十进制=/proc/mami/udm/udevice 口径，0x 前缀按十六
 * 进制——喂错进制会把 decoder dstCNA 指到不存在的节点）。 */
std::vector<std::string> g_ubme_seg_names;   // attach 在案段名（owner 重启重建用）
std::mutex g_ubme_mutex;                     // 解析缓存 + 重建串行化

bool ubme_ctx_ready() {
    static bool ok = []() {
        const char *n = std::getenv("KVC_UBME_NODE_ID");
        const char *p = std::getenv("KVC_UBME_TCP_PORT");
        return kvc_ubme_init(n ? (uint32_t)strtoul(n, nullptr, 0) : 1u,
                             p ? (uint16_t)strtoul(p, nullptr, 0) : 0u,
                             std::getenv("KVC_UBME_PEERS")) == KVC_OK;
    }();
    return ok;
}

std::vector<std::string> ubme_attach_hosts() {
    std::vector<std::string> out;
    const char *multi = std::getenv("KVC_UBME_OWNER_HOSTS");
    if (!multi || !*multi)
        return out;
    if (!ubme_ctx_ready()) {
        LOG(ERROR) << "KVC-FLIP: ubme ctx init failed (runtime present? "
                      "KVC_UBME_PEERS well-formed?)";
        return out;
    }
    std::string s(multi);
    size_t beg = 0;
    for (;;) {
        size_t end = s.find(',', beg);
        std::string host = s.substr(
            beg, end == std::string::npos ? std::string::npos : end - beg);
        size_t b = host.find_first_not_of(' ');
        size_t e = host.find_last_not_of(' ');
        if (e != std::string::npos) {
            host = host.substr(b, e - b + 1);
            char seg[64];
            kvc_seg_name(seg, host.c_str());
            char path[96];
            if (kvc_region_attach_ubme(seg, 30000, path, sizeof(path)) !=
                KVC_OK) {
                LOG(ERROR) << "KVC-FLIP: ubme attach failed (seg=" << seg
                           << " — owner up? peers configured?)";
                return {};
            }
            LOG(INFO) << "KVC-FLIP: ubme segment attached (seg=" << seg
                      << " -> " << path << ")";
            g_ubme_seg_names.push_back(seg);
            out.push_back(path);
        }
        if (end == std::string::npos)
            break;
        beg = end + 1;
    }
    return out;
}

/* 路径 env 优先；缺省走 UBME 按名解析。结果缓存（attach 含 mesh/IO 开销，
 * 且 enabled() 每请求查询）; owner 重启重建时经 invalidate 刷新。
 * 热路径只读 atomic（解析完成后 vector 不再被写，重建走 slow path）。 */
std::vector<std::string> g_devices_cache;
std::atomic<bool> g_devices_resolved{false};

const std::vector<std::string> &effective_devices() {
    if (!g_devices_resolved.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(g_ubme_mutex);
        if (!g_devices_resolved.load(std::memory_order_relaxed)) {
            g_devices_cache = device_list();
            if (g_devices_cache.empty())
                g_devices_cache = ubme_attach_hosts();
            g_devices_resolved.store(true, std::memory_order_release);
        }
    }
    return g_devices_cache;
}

bool enabled() { return !effective_devices().empty(); }

/* owner 绝对地址 → 所属 region 视图（含 [addr, addr+len) 完整落在数据区） */
const RegionView *find_region(uint64_t addr, uint64_t len) {
    for (const auto &rv : g_regions) {
        if (addr < rv.owner_base)
            continue;
        uint64_t off = addr - rv.owner_base;
        if (off < rv.data_bytes && off + len <= rv.data_bytes)
            return &rv;
    }
    return nullptr;
}

bool direct_enabled() {
    return enabled() && std::getenv("KVC_DIRECT_READ") != nullptr;
}

// R5: 直读后做内容级 checksum 校验（对齐 TE 路径的证据链）。
// 默认关。两级降代价:
//   策略3: slice-by-8 CRC-64/ECMA（与 mooncake::CrcChecksum 逐位等价,
//          启动时自校验, 不等即回退逐字节实现）—— 8 字节/步 ~8× 提速
//   策略4: 首验标记（key → 已验 checksum 值）, 后续读仅比对期望值,
//          CRC 摊薄到每对象一次; epoch 切换/覆写(checksum 变化)自动重验
bool direct_checksum_enabled() {
    return enabled() && std::getenv("KVC_DIRECT_CHECKSUM") != nullptr;
}

/* ---- slice-by-8 CRC-64/ECMA（MSB-first, 与 CrcChecksum 逐位等价） ---- */
namespace fastcrc64 {
constexpr uint64_t kPoly = 0x42F0E1EBA9EA3693ULL;
constexpr auto MakeTable() {
    std::array<uint64_t, 256> t{};
    for (size_t i = 0; i < 256; i++) {
        uint64_t c = (uint64_t)i << 56;
        for (int b = 0; b < 8; b++)
            c = (c & (1ULL << 63)) ? (c << 1) ^ kPoly : (c << 1);
        t[i] = c;
    }
    return t;
}
constexpr auto MakeTables() {
    std::array<std::array<uint64_t, 256>, 8> ts{};
    ts[0] = MakeTable();
    for (size_t k = 1; k < 8; k++)
        for (size_t i = 0; i < 256; i++)
            ts[k][i] = ts[0][ts[k-1][i] >> 56] ^ (ts[k-1][i] << 8);
    return ts;
}
constexpr auto kTables = MakeTables();
}  // namespace fastcrc64

struct FastCrc64 {

    uint64_t crc_ = 0;
    void Update(const void* data, size_t size) {
        const uint8_t* p = (const uint8_t*)data;
        while (size >= 8) {
            uint64_t n;
            memcpy(&n, p, 8);
            n = __builtin_bswap64(n) ^ crc_;
            crc_ = fastcrc64::kTables[7][n & 0xff] ^ fastcrc64::kTables[6][(n >> 8) & 0xff] ^
                   fastcrc64::kTables[5][(n >> 16) & 0xff] ^ fastcrc64::kTables[4][(n >> 24) & 0xff] ^
                   fastcrc64::kTables[3][(n >> 32) & 0xff] ^ fastcrc64::kTables[2][(n >> 40) & 0xff] ^
                   fastcrc64::kTables[1][(n >> 48) & 0xff] ^ fastcrc64::kTables[0][n >> 56];
            p += 8; size -= 8;
        }
        while (size--) {
            crc_ = (crc_ << 8) ^ fastcrc64::kTables[0][(crc_ >> 56) ^ *p++];
        }
    }
    uint64_t Finalize() const { return crc_; }
};

/* 首验标记表: key → 已验证的 checksum 值（策略4） */
std::unordered_map<std::string, uint64_t> g_crc_verified;
std::mutex g_crc_mutex;

/* 自校验: 快速实现与上游 CrcChecksum 逐位等价才启用（一次性） */
bool fast_crc_validated() {
    static bool ok = [] {
        char buf[4096];
        for (size_t i = 0; i < sizeof(buf); i++) buf[i] = (char)(i * 131 + 7);
        FastCrc64 fast;
        fast.Update(buf, sizeof(buf));
        return fast.Finalize() == mooncake::ComputeCrcChecksum(buf, sizeof(buf));
    }();
    return ok;
}

/* 直读校验入口: 期望值命中首验标记 → 免 CRC; 否则全量快算。
 * 返回 true = 校验通过（或无期望值）; false = 不符（调用方回退 TE）。 */
inline bool direct_checksum_ok(const std::string& key,
                               const std::vector<mooncake::Slice>& slices,
                               const std::optional<uint64_t>& expected) {
    if (!expected.has_value())
        return true;   // 无期望值（put 时未开 checksum）: 与 TE 路径同语义
    {
        std::lock_guard<std::mutex> lock(g_crc_mutex);
        auto it = g_crc_verified.find(key);
        if (it != g_crc_verified.end() && it->second == *expected) {
            g_crc_verified_hits++;
            return true;   // 同一 checksum 已全量验过
        }
    }
    if (fast_crc_validated()) {
        FastCrc64 crc;
        for (const auto& sl : slices)
            crc.Update(sl.ptr, sl.size);
        if (crc.Finalize() != *expected) {
            if (std::getenv("KVC_DEBUG_DIRECT")) {
                /* ④取证: 校验失败现场 —— 区分"读错 slot"vs"同 slot 旧内容" */
                const auto& sl0 = slices.front();
                uint64_t addr = (uint64_t)(uintptr_t)sl0.ptr;
                LOG(ERROR) << "KVC-DIRECT-DEBUG key=" << key
                           << " expected_crc=" << *expected
                           << " actual_crc=" << crc.Finalize()
                           << " dst_ptr=0x" << std::hex << addr << std::dec
                           << " first16=" << std::hex
                           << *(const uint64_t *)sl0.ptr
                           << *(const uint64_t *)((const char *)sl0.ptr + 8)
                           << std::dec;
            }
            return false;
        }
    } else {
        mooncake::CrcChecksum slow;
        for (const auto& sl : slices)
            slow.Update(sl.ptr, sl.size);
        if (slow.Finalize() != *expected)
            return false;
    }
    {
        std::lock_guard<std::mutex> lock(g_crc_mutex);
        if (g_crc_verified.size() > (1u << 20))
            g_crc_verified.clear();   // 上限防膨胀（与 poscache 同策略）
        g_crc_verified[key] = *expected;
    }
    return true;
}



// 永久禁用（T0-2）: 一次性告警（含原因与 env 快照），此后本进程全走 TE
void disable(const std::string &reason) {
    if (!g_init_failed)
        LOG(ERROR) << "KVC-FLIP: PERMANENTLY DISABLED — " << reason
                   << " [env: KVC_FLIP_DEVICE="
                   << (std::getenv("KVC_FLIP_DEVICE") ? std::getenv("KVC_FLIP_DEVICE") : "<unset>")
                   << " KVC_FLIP_BYTES="
                   << (std::getenv("KVC_FLIP_BYTES") ? std::getenv("KVC_FLIP_BYTES") : "<unset>")
                   << " KVC_FLIP_BLOCK="
                   << (std::getenv("KVC_FLIP_BLOCK") ? std::getenv("KVC_FLIP_BLOCK") : "<unset>")
                   << "] — all reads/writes fall back to TransferEngine";
    snprintf(g_disable_reason, sizeof(g_disable_reason), "%s",
             reason.c_str());
    g_init_failed = true;
}

// transport_endpoint（"tcp://host:port" / "rdma://..."）→ host 串。
// 与 owner 端 owner_identity（host-only，不含设备名——远端读者设备号不同）同源。
inline std::string endpoint_host(const std::string &ep) {
    auto scheme = ep.find("://");
    size_t beg = scheme == std::string::npos ? 0 : scheme + 3;
    auto colon = ep.find(':', beg);
    return ep.substr(beg, colon == std::string::npos ? std::string::npos
                                                     : colon - beg);
}

bool lazy_init(const std::string &expected_host = std::string()) {
    if (!g_regions.empty())
        return true;
    if (g_init_failed)
        return false;   // 已判定不可用（配置错/设备缺失），避免每请求重试
    /* ABI 校验: 编译期已直接链接 libkvc_server.so，这里防的是部署侧
     * 运行时 .so 与构建时头文件版本错配（几何/身份协议跨版本不兼容）。 */
    if (kvc_api_version() != KVC_ABI_VERSION) {
        disable("SDK ABI version mismatch — linked libkvc_server.so does not "
                "match build-time headers, redeploy matching SDK");
        return false;
    }

    const std::vector<std::string> devs = effective_devices();

    /* 多线程首次直读会并发进入 init（如 native driver 的 reader 线程），
     * 并发 build 后各自 move-assign g_regions 是数据竞争——用 g_ubme_mutex
     * 串行化，锁内双重检查避免重复建。锁必须在 effective_devices() 之后：
     * 它内部也拿 g_ubme_mutex。 */
    std::lock_guard<std::mutex> init_lock(g_ubme_mutex);
    if (!g_regions.empty())
        return true;
    if (g_init_failed)
        return false;

    /* UBME owner 进程（daemon, KVC_SEGMENT_UBME=1）无路径 env: C2 驱逐推送
     * 直接复用本进程 owner region（real_client map_segment 所建），免二次
     * 映射。borrowed 视图不参与 owner 重启重建（owner 不会死在自己之下）。 */
    if (devs.empty() && std::getenv("KVC_SEGMENT_UBME")) {
        kvc_region_t *own = kvc_owner_region();
        if (own) {
            kvc_region_meta_t m = {};
            kvc_region_meta(own, &m);
            RegionView rv;
            rv.region = own;
            rv.block = m.block_size;
            rv.owner_base = kvc_region_owner_base(own);
            rv.data_bytes = kvc_region_data_bytes(own);
            rv.epoch = kvc_region_owner_epoch(own);
            rv.data_base = (const char *)kvc_region_data_base(own);
            rv.borrowed = true;
            g_regions.push_back(rv);
            LOG(INFO) << "KVC-FLIP: using in-process owner region (C2 flips, "
                         "region_id="
                      << m.region_id << " data_bytes=" << rv.data_bytes << ")";
            return true;
        }
        return false;   // owner region 未建（segment 未启用/已失败）→ flips 关
    }
    if (devs.empty()) {
        return false;
    }
    const bool single = devs.size() == 1;   // env 断言仅单 region 形态有意义
    std::vector<RegionView> built;
    built.reserve(devs.size());
    for (const auto &dev : devs) {
        /* T0-1 几何自读: header 是权威，env 只作断言（配错即拒绝） */
        kvc_region_meta_t geo = {};
        uint64_t host_id = 0;
        uint32_t epoch = 0;
        int prc = kvc_region_probe_identity(dev.c_str(), 0,
                                            KVC_REGION_O_SYNC_FALLBACK, &geo,
                                            &host_id, &epoch);
        if (prc == KVC_ENOENT) {
            char buf[320];
            snprintf(buf, sizeof(buf),
                     "device has no KVC region (wrong device or owner never "
                     "initialized): %s",
                     dev.c_str());
            disable(buf);
            return false;
        }
        if (prc != KVC_OK) {
            char buf[320];
            snprintf(buf, sizeof(buf),
                     "region header corrupt/geometry inconsistent — wrong "
                     "device? %s",
                     dev.c_str());
            disable(buf);
            return false;
        }
        if (single && std::getenv("KVC_FLIP_BYTES")) {
            const char *e = std::getenv("KVC_FLIP_BYTES");
            uint64_t want = strtoull(e, nullptr, 0);
            if (want != geo.data_bytes + geo.data_off) {
                char buf[256];
                snprintf(buf, sizeof(buf),
                         "KVC_FLIP_BYTES=%llu mismatches region header "
                         "(header total=%llu) — fix env or unset it",
                         (unsigned long long)want,
                         (unsigned long long)(geo.data_bytes + geo.data_off));
                disable(buf);
                return false;
            }
        }
        if (single && std::getenv("KVC_FLIP_BLOCK")) {
            const char *e = std::getenv("KVC_FLIP_BLOCK");
            uint32_t want = (uint32_t)strtoul(e, nullptr, 0);
            if (want != geo.block_size) {
                char buf[224];
                snprintf(buf, sizeof(buf),
                         "KVC_FLIP_BLOCK=%u mismatches region header (block=%u)",
                         want, geo.block_size);
                disable(buf);
                return false;
            }
        }

        /* T0-1 身份核验: shm 自描述 vs master 元数据（transport_endpoint
         * host）; 多 region 时每个 region 独立核验（同一对端 host） */
        if (host_id != 0 && !expected_host.empty()) {
            if (kvc_identity_hash(expected_host.c_str()) != host_id) {
                char buf[384];
                snprintf(buf, sizeof(buf),
                         "owner identity mismatch: region %s claims host-hash "
                         "0x%llx, master metadata says '%s' — wrong device "
                         "mapping?",
                         dev.c_str(), (unsigned long long)host_id,
                         expected_host.c_str());
                disable(buf);
                return false;
            }
        }

        kvc_region_config_t cfg = {};
        cfg.region_id = geo.region_id;
        cfg.provider = KVC_PROVIDER_DEVICE;
        snprintf(cfg.device_path, sizeof(cfg.device_path), "%s", dev.c_str());
        cfg.block_size = geo.block_size;
        cfg.bytes = geo.data_bytes + geo.data_off;   // 权威几何，来自 header
        cfg.flags = KVC_REGION_O_SYNC_FALLBACK;      // validate 模式, 不 CREATE

        kvc_region_t *r = kvc_region_open_local(&cfg);
        if (!r) {
            char buf[320];
            snprintf(buf, sizeof(buf),
                     "open region failed after probe (concurrent re-init?): %s",
                     dev.c_str());
            disable(buf);
            return false;
        }
        RegionView rv;
        rv.region = r;
        rv.block = geo.block_size;
        rv.owner_base = kvc_region_owner_base(r);
        rv.data_bytes = kvc_region_data_bytes(r);
        rv.epoch = kvc_region_owner_epoch(r);
        if (rv.owner_base == 0) {
            char buf[320];
            snprintf(buf, sizeof(buf),
                     "owner base not published (segment owner must be up "
                     "first): %s",
                     dev.c_str());
            disable(buf);
            return false;
        }
        rv.data_base = (const char *)kvc_region_data_base(r);
        built.push_back(rv);
        LOG(INFO) << "KVC-FLIP: region mapped for state flips (dev=" << dev
                  << " region_id=" << geo.region_id
                  << " block=" << cfg.block_size << " bytes=" << cfg.bytes
                  << " owner_base=0x" << std::hex << rv.owner_base << std::dec
                  << " owner_epoch=" << rv.epoch << " identity=0x" << std::hex
                  << host_id << std::dec << ")";
    }
    g_regions = std::move(built);
    return true;
}

/* T0-1/R2 epoch 复查（命中路径调用，每 region 一次 acquire load）:
 * owner 重启（任一 region 的 epoch 变化）→ 刷新该 region 的 owner_base/
 * epoch；刷新失败才降级。返回 false = 继续正常路径；true = 本次操作放弃
 * （调用方走 TE）。 */
/* UBME reader 侧 owner 重启恢复: 关旧映射 → detach 旧 import（对端新段
 * 已是新 mem_id，journal 复用会命中死节点，必须先解除）→ 重 attach →
 * 重开 → 换视图。失败则永久降级 TE（与既有 disable 语义一致）。 */
void ubme_rebuild_views() {
    std::lock_guard<std::mutex> lock(g_ubme_mutex);
    g_devices_resolved.store(false, std::memory_order_release);
    for (auto &rv : g_regions)
        if (!rv.borrowed)
            kvc_region_close(rv.region);
    g_regions.clear();
    for (const auto &seg : g_ubme_seg_names)
        kvc_region_detach_ubme(seg.c_str());
    g_ubme_seg_names.clear();
    if (!effective_devices().empty()) {
        g_init_failed = false;
        if (lazy_init()) {
            g_epoch_refresh++;
            return;
        }
    }
    disable("ubme re-attach after owner restart failed");
}

inline bool owner_restarted() {
    if (g_regions.empty())
        return false;
    /* UBME 动态段: owner 重启后是"新段新 epoch"，被 watch 的旧段 epoch
     * 永远不变——必须叠加 mesh 陈旧探测（对端离线/重上线闩锁）触发重建;
     * 无 mesh（纯同机复用）时探测恒 0，陈旧窗口由 poscache TTL + master
     * 元数据（新 buffer_address_ 不落旧视图 → TE 回退）兜底。重建窗口内
     * 并发直读可能踩到刚 munmap 的旧映射（与既有 epoch 无锁刷新同级风险
     * 面，owner 重启为罕见事件; 本拍作 miss 走 TE）。 */
    if (!g_ubme_seg_names.empty()) {
        for (const auto &rv : g_regions) {
            if ((!rv.borrowed &&
                 kvc_region_owner_epoch(rv.region) != rv.epoch) ||
                kvc_ubme_imports_stale() == 1) {
                ubme_rebuild_views();
                return true;
            }
        }
        return false;
    }
    bool any = false;
    for (auto &rv : g_regions) {
        uint32_t e = kvc_region_owner_epoch(rv.region);
        if (e == rv.epoch)
            continue;
        uint64_t base = kvc_region_owner_base(rv.region);
        if (base == 0) {
            disable("owner restarted and has not republished base");
            return true;
        }
        rv.owner_base = base;   /* 身代切换: 新 owner_base 立即生效 */
        rv.epoch = e;
        any = true;
    }
    if (any) {
        g_epoch_refresh++;
        {
            /* R5: owner 重启后数据可能被重写, 首验标记全部作废 */
            std::lock_guard<std::mutex> lock(g_crc_mutex);
            g_crc_verified.clear();
        }
    }
    return false;
}

inline uint64_t fnv1a(const std::string &s) {
    uint64_t h = 1469598103934665603ULL;
    for (char c : s) {
        h ^= (unsigned char)c;
        h *= 1099511628211ULL;
    }
    return h;
}

// flip/ready_slot 的跨调用 slot 引用: bare slot 序号在多 region 下有歧义,
// 必须连同所属 region 一起传递（batch 异步路径经 PendingTransferRecord 持有）。
struct SlotRef {
    uint32_t region_idx = 0xffffffffu;
    uint32_t slot = 0xffffffffu;
    bool valid() const { return slot != 0xffffffffu; }
};

// 对象覆盖的 slot 区间（R3: 支持任意 k*block 尺寸, 不再定长-only）。
// 返回 false = 几何不符（不在任何 region 内/未对齐/越界/非整 block）。
inline bool slot_range(const RegionView *&rv,
                       const mooncake::AllocatedBuffer::Descriptor &handle,
                       uint32_t *first, uint32_t *last) {
    const RegionView *r = find_region((uint64_t)handle.buffer_address_,
                                      handle.size_);
    if (!r)
        return false;
    uint64_t off = (uint64_t)handle.buffer_address_ - r->owner_base;
    uint64_t size = handle.size_;
    if (size == 0 || off % r->block != 0 || size % r->block != 0)
        return false;
    rv = r;
    *first = (uint32_t)(off / r->block);
    *last = (uint32_t)((off + size - 1) / r->block);
    return true;
}

// flip(replica, writing): 取 memory replica 的绝对地址定位所属 region 并
// 逐 slot 翻转状态（R3: k*block 对象翻转 k 个 slot）。
// 返回首 slot 的 {region, slot} 引用（invalid = 未翻转）。batch 路径把它
// 存进 PendingTransferRecord, 传输完成后用 ready_slot() 发布。
inline SlotRef flip(const mooncake::Replica::Descriptor &replica,
                    const std::string &key, bool writing) {
    if (!enabled() || !replica.is_memory_replica())
        return {};
    const auto &handle0 = replica.get_memory_descriptor().buffer_descriptor;
    if (!lazy_init(endpoint_host(handle0.transport_endpoint_)))
        return {};
    if (owner_restarted())
        return {};
    metrics_tick();
    const auto &handle = replica.get_memory_descriptor().buffer_descriptor;
    /* buffer_address_ 是段 owner 进程的绝对 VA（TCP transport 在目标侧直接
     * 解引用）；用 owner 发布的 data_base 换算成段内 offset。 */
    const RegionView *rv = nullptr;
    uint32_t first, last;
    if (!slot_range(rv, handle, &first, &last)) {
        g_flip_fail++;
        return {};
    }
    uint64_t key_hash = fnv1a(key);
    for (uint32_t s = first; s <= last; s++) {
        int rc = writing ? kvc_slot_mark_writing(rv->region, s, key_hash)
                         : kvc_slot_mark_ready(rv->region, s);
        if (rc != 0) {
            g_flip_fail++;
            return {};
        }
    }
    g_flip_count++;
    return {(uint32_t)(rv - g_regions.data()), first};
}

// 传输完成后发布 slot（batch 异步路径的第二段; 仍是单 slot——batch 路径
// 只对 1-block 对象生效, k-block 对象走同步 flip(false)）
inline void ready_slot(SlotRef ref) {
    if (!ref.valid() || ref.region_idx >= g_regions.size())
        return;
    if (kvc_slot_mark_ready(g_regions[ref.region_idx].region, ref.slot) == 0)
        g_flip_count++;
    else
        g_flip_fail++;
}

// R4: PutRevoke 联动——撤销路径把对象覆盖的 slot 置 INVALID（可被后续
// mark_writing 复用），WRITING 残留不再等 daemon 重启回收。
inline void revoke(const mooncake::Replica::Descriptor &replica) {
    if (!enabled() || !replica.is_memory_replica())
        return;
    const auto &handle = replica.get_memory_descriptor().buffer_descriptor;
    if (!lazy_init(endpoint_host(handle.transport_endpoint_)))
        return;
    if (owner_restarted())
        return;
    const RegionView *rv = nullptr;
    uint32_t first, last;
    if (!slot_range(rv, handle, &first, &last))
        return;
    for (uint32_t s = first; s <= last; s++)
        kvc_slot_invalidate(rv->region, s);
}

// P2a 直读: 绕过 TE, 从本进程的 region 映射直接 memcpy 到调用方缓冲。
// 安全规则: 读区间覆盖的所有 slot 必须 READY（flip 已发布）,
//           否则回退 TE 路径（保守 — 未翻转的对象无法验证完整性）。
// 返回 true = 已完成读取; false = 未处理, 调用方走 TE。
inline bool direct_read(const mooncake::Replica::Descriptor &replica,
                        const std::vector<mooncake::Slice> &slices,
                        uint64_t src_offset) {
    if (!direct_enabled() || !replica.is_memory_replica())
        return false;
    const auto &handle0 = replica.get_memory_descriptor().buffer_descriptor;
    if (!lazy_init(endpoint_host(handle0.transport_endpoint_)))
        return false;
    if (owner_restarted())
        return false;   // 身代切换当拍放弃，下一拍用新 owner_base
    metrics_tick();

    const auto &handle = replica.get_memory_descriptor().buffer_descriptor;
    uint64_t addr = (uint64_t)handle.buffer_address_ + src_offset;
    uint64_t total = 0;
    for (const auto &s : slices)
        total += s.size;
    /* 对齐前置: off 必须落 block 边界（slot 语义的权威前提）。
     * 非对齐（512 对齐的旧分配或 range read 半 slot 起点）→ 回退 TE。 */
    const RegionView *rv = find_region(addr, total);
    if (rv == nullptr || total == 0) {
        g_direct_fallback++;
        return false;
    }
    uint64_t off = addr - rv->owner_base;
    if (off % rv->block != 0) {
        g_direct_fallback++;
        return false;
    }

    /* 覆盖 slot 全部 READY 才直读（跨 block 的对象同样安全） */
    uint32_t first = (uint32_t)(off / rv->block);
    uint32_t last = (uint32_t)((off + total - 1) / rv->block);
    kvc_slot_info_t info;
    for (uint32_t s = first; s <= last; s++) {
        if (kvc_slot_query(rv->region, s, &info) != 0 ||
            info.state != KVC_SLOT_READY) {
            g_direct_fallback++;
            return false;
        }
    }
    const uint64_t seq_before = info.write_seq;   // 末 slot 的序号快照

    const char *src = rv->data_base + off;
    uint64_t done = 0;
    for (const auto &s : slices) {
        /* nc 远端内存: 8 深独立 load 流水拷贝（glibc memcpy 单线程仅 63MB/s） */
        const uint64_t *sp = (const uint64_t *)(src + done);
        uint64_t *dp = (uint64_t *)s.ptr;
        size_t n = s.size;
        while (n >= 64) {
            uint64_t a0 = sp[0], a1 = sp[1], a2 = sp[2], a3 = sp[3];
            uint64_t a4 = sp[4], a5 = sp[5], a6 = sp[6], a7 = sp[7];
            dp[0] = a0; dp[1] = a1; dp[2] = a2; dp[3] = a3;
            dp[4] = a4; dp[5] = a5; dp[6] = a6; dp[7] = a7;
            sp += 8; dp += 8; n -= 64;
        }
        while (n >= 8) { *dp++ = *sp++; n -= 8; }
        if (n) memcpy(dp, sp, n);
        done += s.size;
    }

    /* 读后校验（correctness T1-2 防线）:
     * 1) 重读首/末 16B 哨兵 —— 捕获拷贝期间数据被改写（竞态窗口）
     * 2) 复查末 slot write_seq —— 捕获读前发布的 slot 被复用
     * 任一不符 → 判脏, 回退 TE（TE 走 owner 本地视图, 永远权威）。
     * 成本: 2 次 16B nc 读 + 1 次状态查询（纳秒级）。 */
    {
        /* 哨兵防御: 首/尾 slice 至少 16B 才可比（零长/超短 slice 跳过
         * 对应哨兵，避免 size-16 下溢越界读） */
        const auto &fs = slices.front();
        const auto &bs = slices.back();
        bool head_ok = fs.size >= 16;
        bool tail_ok = bs.size >= 16;
        const uint64_t *head = (const uint64_t *)(src);
        const uint64_t *tail =
            (const uint64_t *)(src + total - 16);
        const uint64_t *dh = (const uint64_t *)fs.ptr;
        const uint64_t *dt =
            (const uint64_t *)((const char *)bs.ptr + bs.size - 16);
        bool stable = (!head_ok || (dh[0] == head[0] && dh[1] == head[1])) &&
                      (!tail_ok || (dt[0] == tail[0] && dt[1] == tail[1]));
        if (stable &&
            kvc_slot_query(rv->region, last, &info) == 0 &&
            info.state == KVC_SLOT_READY && info.write_seq == seq_before) {
            g_direct_count++;
            return true;
        }
        g_guard_rejects++;
        return false;   // 脏读嫌疑: 回退 TE
    }
}

// Remove/驱逐联动 (T1-3): 对象删除后 slot 翻 INVALID, 保持介质状态与
// master 元数据一致（防止位置缓存/直读命中已删对象）
inline void invalidate_replica(const mooncake::Replica::Descriptor &replica) {
    if (!enabled() || g_regions.empty() || !replica.is_memory_replica())
        return;
    if (owner_restarted())
        return;
    const auto &handle = replica.get_memory_descriptor().buffer_descriptor;
    const RegionView *rv = nullptr;
    uint32_t first, last;
    if (!slot_range(rv, handle, &first, &last))
        return;
    for (uint32_t s = first; s <= last; s++)
        kvc_slot_invalidate(rv->region, s);
}
// C2: 驱逐推送执行端——按 master 任务里的裸地址失效 slot 区间
inline void invalidate_addr(uint64_t buffer_address, uint64_t size,
                            uint64_t key_hash) {
    /* C2: daemon 进程不设 KVC_FLIP_DEVICE（那是读侧开关）——驱逐推送
     * 到达时按写侧 KVC_SEGMENT_DEVICE 自行初始化 region 访问 */
    if (!enabled() && !std::getenv("KVC_SEGMENT_DEVICE"))
        return;
    if (!lazy_init() || g_regions.empty())
        return;
    if (owner_restarted())
        return;
    const RegionView *rv = find_region(buffer_address, size);
    if (!rv || size == 0 || size % rv->block != 0)
        return;
    uint64_t off = buffer_address - rv->owner_base;
    if (off % rv->block != 0)
        return;
    uint32_t first = (uint32_t)(off / rv->block);
    uint32_t last = (uint32_t)((off + size - 1) / rv->block);
    /* key_hash 守卫: 迟到的推送若遇 slot 已被新对象复用（hash 不符）,
     * 跳过——绝不误杀活对象; WRITING/FREE 状态同样跳过 */
    kvc_slot_info_t info;
    for (uint32_t s2 = first; s2 <= last; s2++) {
        if (kvc_slot_query(rv->region, s2, &info) != 0)
            continue;
        if (info.state != KVC_SLOT_READY || info.key_hash != key_hash)
            continue;
        kvc_slot_invalidate(rv->region, s2);
    }
}
}  // namespace
}  // namespace kvcflip
#include "utils.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "environ.h"

namespace mooncake {

namespace {

constexpr size_t kObjectChecksumD2HChunkSize = 8 * 1024 * 1024;

class ScopedObjectChecksumBuffer {
   public:
    ScopedObjectChecksumBuffer(PinnedBufferPool& pool, size_t size)
        : pool_(pool), buffer_(pool_.Acquire(size)) {}

    ~ScopedObjectChecksumBuffer() { pool_.Release(std::move(buffer_)); }

    char* data() const { return buffer_.data; }

   private:
    PinnedBufferPool& pool_;
    PinnedBufferPool::Buffer buffer_;
};

#ifdef USE_NOF
std::optional<int> GetConfiguredNumaSocketId() {
    const char* raw_value = std::getenv("MC_STORE_NUMA_SOCKET_ID");
    if (!raw_value || raw_value[0] == '\0') {
        return std::nullopt;
    }

    char* end_ptr = nullptr;
    errno = 0;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed < 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid MC_STORE_NUMA_SOCKET_ID=" << raw_value
                     << ", falling back to auto-detect";
        return std::nullopt;
    }

    return static_cast<int>(parsed);
}

int GetCurrentNumaSocketId() {
    if (numa_available() < 0) {
        return 0;
    }
    int cpu = sched_getcpu();
    if (cpu < 0) {
        return 0;
    }
    int node = numa_node_of_cpu(cpu);
    return node < 0 ? 0 : node;
}
#endif

struct ContiguousSliceRange {
    void* ptr = nullptr;
    size_t size = 0;
};

std::optional<ContiguousSliceRange> GetContiguousSliceRange(
    std::span<const Slice> slices) {
    if (slices.empty()) {
        return std::nullopt;
    }

    uintptr_t expected_ptr = 0;
    uintptr_t start_ptr = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) {
            return std::nullopt;
        }

        const auto current_ptr = reinterpret_cast<uintptr_t>(slice.ptr);
        if (i == 0) {
            start_ptr = current_ptr;
            expected_ptr = current_ptr;
        } else if (current_ptr != expected_ptr) {
            return std::nullopt;
        }

        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return std::nullopt;
        }
        if (slice.size > std::numeric_limits<uintptr_t>::max() - current_ptr) {
            return std::nullopt;
        }

        total_size += slice.size;
        expected_ptr = current_ptr + slice.size;
    }

    if (total_size == 0) {
        return std::nullopt;
    }

    return ContiguousSliceRange{.ptr = reinterpret_cast<void*>(start_ptr),
                                .size = total_size};
}

ErrorCode ScatterFragmentError(const Status& status) {
    return status.IsInvalidArgument() ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::TRANSFER_FAIL;
}

// Collects the fragments of many ranged entries into a single scatter submit.
//
// One submit per entry would hand the transport one tiny transfer per key per
// layer; batching them lets the transport coalesce every fragment bound for the
// same segment into one transfer, which is what keeps layer-wise sessions at
// full bandwidth.
//
// ScatterTransferRange holds non-owning spans, so the offset/length storage
// lives here and must outlive the submit. Both vectors are reserved to the
// exact fragment count up front so pushes never reallocate under a live span.
class ScatterRangeBuilder {
   public:
    explicit ScatterRangeBuilder(size_t fragment_count)
        : zero_offsets_(fragment_count, 0) {
        remote_offsets_.reserve(fragment_count);
        lengths_.reserve(fragment_count);
        ranges_.reserve(fragment_count);
    }

    // `error_slot` is shared by every fragment of one entry and keeps the first
    // failure. Callbacks run on the waiting thread, so no locking is needed.
    void Add(TransferRequest::OpCode opcode,
             const AllocatedBuffer::Descriptor& handle, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        const size_t index = remote_offsets_.size();
        remote_offsets_.push_back(static_cast<size_t>(remote_offset));
        lengths_.push_back(slice.size);
        ranges_.push_back(TransferEngine::ScatterTransferRange{
            .opcode = opcode,
            .remote_segment = handle.transport_endpoint_,
            .remote_base_offset = handle.buffer_address_,
            .remote_size = static_cast<size_t>(handle.size_),
            .local_buffer = slice.ptr,
            .local_capacity = slice.size,
            .local_offsets = std::span<const size_t>(&zero_offsets_[index], 1),
            .remote_offsets =
                std::span<const size_t>(&remote_offsets_[index], 1),
            .lengths = std::span<const size_t>(&lengths_[index], 1),
            .on_fragment_complete =
                [error_slot](size_t, const Status& status) {
                    if (!status.ok() && !error_slot->has_value()) {
                        *error_slot = ScatterFragmentError(status);
                    }
                },
        });
    }

    bool empty() const { return ranges_.empty(); }

    const std::vector<TransferEngine::ScatterTransferRange>& ranges() const {
        return ranges_;
    }

   private:
    std::vector<size_t> zero_offsets_;
    std::vector<size_t> remote_offsets_;
    std::vector<size_t> lengths_;
    std::vector<TransferEngine::ScatterTransferRange> ranges_;
};

struct ReplicaTransferSummary {
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    size_t successful_memory_transfers = 0;
    size_t successful_nof_transfers = 0;
    size_t failed_memory_transfers = 0;
    size_t failed_nof_transfers = 0;
    ErrorCode first_error = ErrorCode::OK;

    void RecordAllocatedReplica(const Replica::Descriptor& replica) {
        if (replica.is_memory_replica()) {
            ++allocated_memory_replicas;
        } else if (replica.is_nof_replica()) {
            ++allocated_nof_replicas;
        }
    }

    void RecordSuccess(ReplicaType replica_type) {
        if (replica_type == ReplicaType::MEMORY) {
            ++successful_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++successful_nof_transfers;
        }
    }

    void RecordFailure(ReplicaType replica_type, ErrorCode error) {
        if (replica_type == ReplicaType::MEMORY) {
            ++failed_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++failed_nof_transfers;
        }
        if (first_error == ErrorCode::OK) {
            first_error = error;
        }
    }
};

bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  const ReplicaTransferSummary& summary) {
    if (config.nof_replica_num == 0) {
        return summary.allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return summary.allocated_memory_replicas +
                   summary.allocated_nof_replicas >
               0;
    }
    return summary.allocated_memory_replicas == config.replica_num &&
           summary.allocated_nof_replicas == config.nof_replica_num;
}

// success describes whether the overall put should succeed. Reliable modes
// require all allocated replicas to complete. Flexible dual-replica mode only
// requires one replica type to succeed, so success may be true while
// revoke_type is also set for the failed side.
struct FinalizeDecision {
    std::optional<ReplicaType> end_type;
    std::optional<ReplicaType> revoke_type;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

FinalizeDecision DetermineFinalizeDecision(
    const ReplicateConfig& config, const ReplicaTransferSummary& summary) {
    const auto write_mode = DetermineReplicaWriteMode(config);
    const bool allocation_satisfied =
        HasExpectedReplicaAllocation(config, summary);

    if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        const bool all_transfers_succeeded =
            summary.successful_memory_transfers ==
                summary.allocated_memory_replicas &&
            summary.successful_nof_transfers ==
                summary.allocated_nof_replicas &&
            summary.failed_memory_transfers == 0 &&
            summary.failed_nof_transfers == 0;
        if (allocation_satisfied && all_transfers_succeeded) {
            return {.end_type = ReplicaType::ALL,
                    .revoke_type = std::nullopt,
                    .success = true,
                    .error = ErrorCode::OK};
        }
        return {.end_type = std::nullopt,
                .revoke_type = ReplicaType::ALL,
                .success = false,
                .error = allocation_satisfied
                             ? (summary.first_error == ErrorCode::OK
                                    ? ErrorCode::TRANSFER_FAIL
                                    : summary.first_error)
                             : ErrorCode::NO_AVAILABLE_HANDLE};
    }

    const bool memory_succeeded = summary.successful_memory_transfers > 0;
    const bool nof_succeeded = summary.successful_nof_transfers > 0;

    if (memory_succeeded && nof_succeeded) {
        return {.end_type = ReplicaType::ALL,
                .revoke_type = std::nullopt,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (memory_succeeded) {
        return {.end_type = ReplicaType::MEMORY,
                .revoke_type = ReplicaType::NOF_SSD,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (nof_succeeded) {
        return {.end_type = ReplicaType::NOF_SSD,
                .revoke_type = ReplicaType::MEMORY,
                .success = true,
                .error = ErrorCode::OK};
    }

    return {.end_type = std::nullopt,
            .revoke_type = ReplicaType::ALL,
            .success = false,
            .error = summary.first_error == ErrorCode::OK
                         ? ErrorCode::NO_AVAILABLE_HANDLE
                         : summary.first_error};
}

}  // namespace

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

[[nodiscard]] size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               const std::map<std::string, std::string>& labels,
               const std::string& tenant_id)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr,
                     tenant_id),
      local_hostname_(local_hostname),
      host_id_(ResolveMooncakeHostId(local_hostname)),
      metadata_connstring_(metadata_connstring),
      protocol_(protocol),
      object_checksum_enabled_(Environ::Get().GetStoreChecksumEnabled()),
      pinned_buffer_pool_(std::make_unique<PinnedBufferPool>()),
      write_thread_pool_(2),
      task_thread_pool_(4) {
    LOG(INFO) << "client_id=" << client_id_;
    if (!host_id_.empty()) {
        LOG(INFO) << "client_id=" << client_id_ << ", host_id=" << host_id_;
    }
    if (object_checksum_enabled_) {
        LOG(INFO) << "Object checksum validation is enabled";
    }

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }
}

Client::~Client() {
    task_poll_running_ = false;
    if (task_poll_thread_.joinable()) {
        task_poll_thread_.join();
    }

    storage_heartbeat_running_ = false;
    if (storage_heartbeat_thread_.joinable()) {
        storage_heartbeat_thread_.join();
    }

    leader_monitor_running_ = false;
    if (leader_monitor_thread_.joinable()) {
        leader_monitor_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(graceful_unmount_timer_mutex_);
        graceful_unmount_timer_stopping_ = true;
    }
    graceful_unmount_timer_cv_.notify_all();

    // Stop queued timer/task callbacks before tearing down segment state.
    task_running_ = false;
    task_thread_pool_.stop();

    // Make copies to avoid modifying while iterating
    std::vector<Segment> mounted_segments_copy;
    std::vector<Segment> gracefully_unmounting_segments_copy;
    std::vector<std::pair<UUID, std::function<void(const UUID&)>>>
        graceful_cleanup_callbacks;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_copy.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            mounted_segments_copy.emplace_back(entry.second);
        }
        for (auto& entry : gracefully_unmounting_segments_) {
            gracefully_unmounting_segments_copy.emplace_back(entry.second);
        }
        graceful_cleanup_callbacks.reserve(
            graceful_unmount_cleanup_callbacks_.size());
        for (auto& entry : graceful_unmount_cleanup_callbacks_) {
            graceful_cleanup_callbacks.emplace_back(entry.first,
                                                    std::move(entry.second));
        }
        graceful_unmount_cleanup_callbacks_.clear();
    }

    // Unmount mounted segments: notify master + local cleanup
    for (auto& segment : mounted_segments_copy) {
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment in destructor: "
                       << toString(result.error());
        }
    }

    // Unregister gracefully unmounting segments: master already has timer
    for (auto& segment : gracefully_unmounting_segments_copy) {
        int rc = transfer_engine_->unregisterLocalMemory(
            reinterpret_cast<void*>(segment.base));
        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
            LOG(ERROR) << "Failed to unregister transfer buffer in destructor: "
                       << rc;
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
        gracefully_unmounting_segments_.clear();
    }

    for (auto& entry : graceful_cleanup_callbacks) {
        if (entry.second) {
            entry.second(entry.first);
        }
    }

    // Stop hot cache handler before unregistering/freeing its backing memory.
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
}

ReplicateConfig Client::AttachHostId(const ReplicateConfig& config) const {
    ReplicateConfig client_cfg = config;
    if (!host_id_.empty()) {
        client_cfg.host_id = host_id_;
    }
    return client_cfg;
}

static std::optional<bool> get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        try {
            int iv = std::stoi(ev_ad);
            if (iv == 1) {
                LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
                return true;
            } else if (iv == 0) {
                LOG(INFO) << "auto discovery not set by env MC_MS_AUTO_DISC";
                return false;
            }
        } catch (const std::exception&) {
            // A non-numeric or out-of-range value makes std::stoi throw; fall
            // through to the warning below and use the default instead of
            // letting the exception abort client initialization.
        }
        LOG(WARNING)
            << "invalid MC_MS_AUTO_DISC value: " << ev_ad
            << ", should be 0 or 1, using default: auto discovery not set";
    }
    return std::nullopt;
}

static std::vector<std::string> get_auto_discover_filters() {
    const char* raw_filters = std::getenv("MC_MS_FILTERS");
    if (raw_filters == nullptr) {
        return {};
    }

    LOG(INFO) << "whitelist filters: " << raw_filters;
    std::vector<std::string> filters;
    boost::split(filters, std::string(raw_filters), boost::is_any_of(","),
                 boost::token_compress_off);
    for (auto& filter : filters) {
        filter = std::string(TrimAsciiWhitespace(filter));
    }
    return filters;
}

static std::vector<std::string> ParseDeviceNames(std::string_view value) {
    std::vector<std::string> devices;
    boost::split(devices, std::string(value), boost::is_any_of(","),
                 boost::token_compress_on);
    for (auto& device : devices) {
        device = std::string(TrimAsciiWhitespace(device));
    }
    return devices;
}

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry) {
    const auto delimiter_pos = master_server_entry.find("://");
    if (delimiter_pos == std::string::npos) {
        return std::optional<ha::HABackendSpec>{std::nullopt};
    }

    auto backend_type =
        ha::ParseHABackendType(master_server_entry.substr(0, delimiter_pos));
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ha::ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    return std::optional<ha::HABackendSpec>{ha::HABackendSpec{
        .type = backend_type.value(),
        .connstring = master_server_entry.substr(delimiter_pos + 3),
        .cluster_namespace = "",
    }};
}

tl::expected<void, ErrorCode> CheckRegisterMemoryParams(const void* addr,
                                                        size_t length) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Tcp is not limited by max_mr_size, but we ignore it for now.
    auto max_mr_size = globalConfig().max_mr_size;  // Max segment size
    if (length > max_mr_size) {
        LOG(ERROR) << "length " << length
                   << " is larger than max_mr_size: " << max_mr_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    auto ha_backend_spec = ParseHABackendSpec(master_server_entry);
    if (!ha_backend_spec) {
        LOG(ERROR) << "Invalid HA backend entry: " << master_server_entry;
        return ha_backend_spec.error();
    }

    if (ha_backend_spec.value().has_value()) {
        auto coordinator =
            ha::CreateLeaderCoordinator(ha_backend_spec.value().value());
        if (!coordinator) {
            LOG(ERROR) << "Failed to create HA backend coordinator: "
                       << toString(coordinator.error());
            return coordinator.error();
        }

        auto current_view = coordinator.value()->ReadCurrentView();
        if (!current_view) {
            LOG(ERROR) << "Failed to read current master view: "
                       << toString(current_view.error());
            return current_view.error();
        }
        if (!current_view.value().has_value()) {
            LOG(ERROR) << "No master is available in HA backend";
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }

        const auto& master_view = current_view.value().value();
        auto err = SwitchLeader(master_view);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        leader_coordinator_ = std::move(coordinator.value());
        direct_master_address_.clear();

        leader_monitor_running_ = true;
        leader_monitor_thread_ =
            std::thread([this]() { this->LeaderMonitorThreadMain(); });

        return ErrorCode::OK;
    } else {
        auto err = master_client_.Connect(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        direct_master_address_ = master_server_entry;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            current_master_view_.reset();
        }
        last_ping_success_.store(true);
        return ErrorCode::OK;
    }
}

ErrorCode Client::SwitchLeader(const ha::MasterView& target_view) {
    std::lock_guard<std::mutex> lock(leader_switch_mutex_);

    if (current_master_view_.has_value()) {
        const auto& current_view = current_master_view_.value();
        if (target_view.view_version < current_view.view_version) {
            return ErrorCode::OK;
        }

        if (target_view.view_version == current_view.view_version &&
            target_view.leader_address == current_view.leader_address &&
            last_ping_success_.load()) {
            return ErrorCode::OK;
        }
    }

    auto err = master_client_.Connect(target_view.leader_address);
    if (err != ErrorCode::OK) {
        last_ping_success_.store(false);
        return err;
    }

    current_master_view_ = target_view;
    last_ping_success_.store(true);
    return ErrorCode::OK;
}

void Client::LeaderMonitorThreadMain() {
    // WaitForViewChange now blocks on a backend watch and returns as soon as
    // leadership actually changes, so this timeout no longer drives polling
    // cadence -- it is only a periodic re-sync safety net that re-reads the
    // current view in case a watch event was missed (e.g. a connection blip).
    // Keeping it long (30s instead of 1s) collapses steady-state backend load
    // from ~1 read/sec/client to ~1 read/30s/client without affecting failover
    // responsiveness, which is driven by the watch.
    constexpr auto kViewChangeTimeout = std::chrono::milliseconds(30000);
    constexpr auto kErrorRetryInterval = std::chrono::milliseconds(1000);

    while (leader_monitor_running_.load()) {
        std::optional<ViewVersionId> known_version;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            if (current_master_view_.has_value()) {
                known_version = current_master_view_->view_version;
            }
        }

        auto view_change = leader_coordinator_->WaitForViewChange(
            known_version, kViewChangeTimeout);
        if (!view_change) {
            LOG(WARNING) << "Failed to wait for leader view change: "
                         << toString(view_change.error());
            std::this_thread::sleep_for(kErrorRetryInterval);
            continue;
        }

        if (!view_change->changed || !view_change->current_view.has_value()) {
            continue;
        }

        auto err = SwitchLeader(view_change->current_view.value());
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to switch to leader "
                         << view_change->current_view->leader_address << ": "
                         << toString(err);
            std::this_thread::sleep_for(kErrorRetryInterval);
        }
    }
}

void Client::EnsureStorageControlPlaneStarted() {
    if (!storage_heartbeat_running_.exchange(true)) {
        storage_heartbeat_thread_ =
            std::thread([this]() { this->StorageHeartbeatThreadMain(); });
    }

    if (!task_poll_running_.exchange(true)) {
        task_poll_thread_ =
            std::thread([this]() { this->TaskPollThreadMain(); });
    }
}

ErrorCode Client::InitTransferEngine(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    // TEs created through the Store entry (Client::Create ->
    // InitTransferEngine) are tagged so ascend_direct can resolve a per-role
    // link config (e.g. Store=RoCE/D2H, P2P=HCCS/D2D). The flag only needs to
    // be live while the ascend transport is installed below; reset it on every
    // exit path. Assumes TE inits are serialized within the process.
    struct StoreTeInitGuard {
        ~StoreTeInitGuard() { globalConfig().ascend_store_te_init = false; }
    } store_te_init_guard;
    if (protocol == "ascend") {
        globalConfig().ascend_store_te_init = true;
    }

    // Check if using TENT mode - TENT handles transport configuration
    // internally. Use the engine's own check rather than the raw env var:
    // if Mooncake was built without USE_TENT, the env var has no effect on
    // the TransferEngine, so the store must still install transports.
    bool use_tent = transfer_engine_->isUsingTent();

    bool auto_discover = false;
    if (!use_tent) {
        // Get auto_discover and filters from env (non-TENT only)
        std::optional<bool> env_auto_discover = get_auto_discover();
        if (env_auto_discover.has_value()) {
            // Use user-specified auto-discover setting
            auto_discover = env_auto_discover.value();
        } else {
            // Enable auto-discover for RDMA/EFA if no devices are specified
            if ((protocol == "rdma" || protocol == "efa") &&
                !device_names.has_value()) {
                LOG(INFO) << "Set auto discovery ON by default for " << protocol
                          << " protocol, since no device names provided";
                auto_discover = true;
            }
        }
        transfer_engine_->setAutoDiscover(
            {.enabled = auto_discover, .protocol = protocol});

        // Honor filters when auto-discovery is enabled; otherwise warn once
        if (auto_discover) {
            LOG(INFO)
                << "Transfer engine auto discovery is enabled for protocol: "
                << protocol;
            auto filters = get_auto_discover_filters();
            transfer_engine_->setWhitelistFilters(std::move(filters));
        } else {
            const char* env_filters = std::getenv("MC_MS_FILTERS");
            if (env_filters && *env_filters != '\0') {
                LOG(WARNING)
                    << "MC_MS_FILTERS is set but auto discovery is disabled; "
                    << "ignoring whitelist: " << env_filters;
            }
        }
    }

    if (protocol == "ascend" || protocol == "ubshmem") {
        const char* ascend_use_fabric_mem =
            std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
        if (ascend_use_fabric_mem) {
            globalConfig().ascend_use_fabric_mem = true;
        }
    }
    if (protocol == "sunrise_link") {
        const char* sunrise_use_device_mem_env =
            std::getenv("SUNRISE_USE_DEVICE_MEM");
        if (sunrise_use_device_mem_env) {
            globalConfig().sunrise_use_device_mem = true;
            LOG(INFO) << "SUNRISE_USE_DEVICE_MEM enabled: segments will be "
                      << "allocated in device memory via tangMalloc";
        }
    }
    auto [hostname, port] = parseHostNameWithPort(local_hostname);
    int rc = transfer_engine_->init(metadata_connstring, local_hostname,
                                    hostname, port);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
        return ErrorCode::INTERNAL_ERROR;
    }

    // TENT mode: Skip manual transport installation - TENT handles this
    // internally
    if (use_tent) {
        LOG(INFO)
            << "Using TENT mode - transport configuration handled internally";
        if (device_names.has_value()) {
            LOG(INFO)
                << "Note: device_names parameter is ignored in TENT mode. "
                << "Configure devices via TENT config file or environment "
                   "variables.";
        }
        return ErrorCode::OK;
    }

    if (!auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is disabled for protocol: "
                  << protocol;

        Transport* transport = nullptr;

        if (protocol == "rdma" || protocol == "efa") {
            if (!device_names.has_value() || device_names->empty()) {
                LOG(ERROR) << "RDMA protocol requires device names when auto "
                              "discovery is disabled";
                return ErrorCode::INVALID_PARAMS;
            }

            LOG(INFO) << "Using specified RDMA devices: "
                      << device_names.value();

            std::vector<std::string> devices =
                ParseDeviceNames(device_names.value());

            // Manually discover topology with specified devices only
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }

            transport = transfer_engine_->installTransport(protocol, nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install RDMA transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "tcp") {
            if (device_names.has_value()) {
                LOG(WARNING)
                    << "TCP protocol does not use device names, ignoring";
            }

            try {
                transport = transfer_engine_->installTransport("tcp", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "ub") {
            /* URMA 设备名不在 RDMA 注册表里，Topology::discover 认不出
             * （HCA 列表空 → UbTransport 回退 mock_urma_device 后崩）；
             * 用 MC_CUSTOM_TOPO_JSON 矩阵直接注入（格式同 transfer-engine
             * ub bench: {"cpu:0": [["udma1"], []], ...}）。 */
            auto topology = transfer_engine_->getLocalTopology();
            const char* topo_path = std::getenv("MC_CUSTOM_TOPO_JSON");
            bool topo_ok = false;
            if (topology && topo_path && *topo_path) {
                std::ifstream in(topo_path);
                std::string json((std::istreambuf_iterator<char>(in)),
                                 std::istreambuf_iterator<char>());
                if (!json.empty() && topology->parse(json) == 0 &&
                    !topology->getHcaList().empty()) {
                    topo_ok = true;
                    LOG(INFO) << "UB topology injected from " << topo_path
                              << " (" << topology->getHcaList().size()
                              << " HCAs)";
                }
            }
            if (!topo_ok) {
                LOG(ERROR) << "ub protocol requires MC_CUSTOM_TOPO_JSON "
                              "pointing to a valid topology matrix (URMA "
                              "devices are invisible to auto discovery)";
                return ErrorCode::INVALID_PARAMS;
            }
            transport = transfer_engine_->installTransport("ub", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install UB transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "ascend" || protocol == "ubshmem" ||
                   protocol == "sunrise_link") {
            if (device_names.has_value()) {
                LOG(WARNING) << protocol
                             << " protocol does not use device names, ignoring";
            }
            try {
                transport =
                    transfer_engine_->installTransport(protocol, nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << protocol
                           << "_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install " << protocol << " transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "cxl") {
            if (device_names.has_value()) {
                LOG(WARNING) << "CXL protocol does not use device "
                                "names, ignoring";
            }
            try {
                transport = transfer_engine_->installTransport("cxl", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxl_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install CXL transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "cxi") {
            try {
                transport = transfer_engine_->installTransport("cxi", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxi_transport_install_failed error_message=\""
                           << e.what() << "\"";
            }
        } else if (protocol == "ub") {
            auto deviceName = device_names.value_or("bonding_dev_0");
            LOG(ERROR) << "ub protocol entable device names is " << deviceName;
            auto devices = ParseDeviceNames(deviceName);
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }
            transport = transfer_engine_->installTransport("ub", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install ub transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
            LOG(INFO) << "Using intra-NVLink protocol.";
            transport =
                transfer_engine_->installTransport("nvlink_intra", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install nvlink_intra transport.";
                return ErrorCode::INTERNAL_ERROR;
            }
#else
            LOG(ERROR)
                << "--protocol=nvlink_intra requires USE_INTRA_NVLINK=ON, "
                   "please rebuild mooncake from source.";
            return ErrorCode::INVALID_PARAMS;
#endif
        } else {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    return ErrorCode::OK;
}

void Client::InitTransferSubmitter() {
    // Initialize TransferSubmitter after transfer engine is ready
    // Keep using logical local_hostname for name-based behaviors; endpoint is
    // used separately where needed.
#ifdef USE_NOF
    int numa_socket_id =
        GetConfiguredNumaSocketId().value_or(GetCurrentNumaSocketId());
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr, numa_socket_id);
#else
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr);
#endif
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, const std::optional<std::string>& device_names,
    const std::string& master_server_entry,
    const std::shared_ptr<TransferEngine>& transfer_engine,
    std::map<std::string, std::string> labels, const std::string& tenant_id) {
    auto client = std::shared_ptr<Client>(new Client(
        local_hostname, metadata_connstring, protocol, labels, tenant_id));

    ErrorCode err = client->ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Initialize storage backend if storage_root_dir is valid
    auto config_response = client->master_client_.GetStorageConfig();
    if (!config_response) {
        LOG(ERROR) << "Failed to get storage config from master";
        // Fallback to GetFsdir for backward compatibility
        auto response = client->master_client_.GetFsdir();
        if (!response) {
            LOG(ERROR) << "Failed to get fsdir from master";
        } else if (response.value().empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            auto dir_string = response.value();
            size_t pos = dir_string.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = dir_string.substr(0, pos);
                std::string fs_subdir = dir_string.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                // Initialize storage backend with default eviction settings
                auto prep_err = client->PrepareStorageBackend(
                    storage_root_dir, fs_subdir, true, 0);
                if (prep_err != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to initialize storage backend: " << prep_err
                        << ". Persistence was requested via fsdir but is "
                           "unavailable.";
                    return std::nullopt;
                }
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << dir_string;
                return std::nullopt;
            }
        }
    } else {
        auto config = config_response.value();
        if (config.fsdir.empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            size_t pos = config.fsdir.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = config.fsdir.substr(0, pos);
                std::string fs_subdir = config.fsdir.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                LOG(INFO) << "Disk eviction enabled: "
                          << config.enable_disk_eviction;
                LOG(INFO) << "Quota bytes: " << config.quota_bytes;
                // Initialize storage backend with config from master
                auto prep_err = client->PrepareStorageBackend(
                    storage_root_dir, fs_subdir, config.enable_disk_eviction,
                    config.quota_bytes);
                if (prep_err != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to initialize storage backend: " << prep_err
                        << ". Persistence was requested via storage config "
                           "but is unavailable.";
                    return std::nullopt;
                }
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << config.fsdir;
                return std::nullopt;
            }
        }
    }

    // this only performs RPC calls
    if (protocol == "rpc_only") {
        LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
        return client;
    }

    // Initialize transfer engine
    if (transfer_engine == nullptr) {
        client->transfer_engine_ = std::make_shared<TransferEngine>();
        err = client->InitTransferEngine(local_hostname, metadata_connstring,
                                         protocol, device_names);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return std::nullopt;
        }
    } else {
        client->transfer_engine_ = transfer_engine;
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }

    client->InitTransferSubmitter();
    // Initialize local hot cache
    err = client->InitLocalHotCache();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize local hot cache";
    }

    return client;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          std::vector<Slice>& slices) {
    auto query_result = Query(object_key);
    if (!query_result) {
        return tl::unexpected(query_result.error());
    }
    return Get(object_key, query_result.value(), slices);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    auto batched_query_results = BatchQuery(object_keys);

    // If any queries failed, return error results immediately for failed
    // queries
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_keys.size());

    std::vector<QueryResult> valid_query_results;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_query_results.emplace_back(batched_query_results[i].value());
            valid_indices.emplace_back(i);
            valid_keys.emplace_back(object_keys[i]);
            results.emplace_back();  // placeholder for successful results
        } else {
            results.emplace_back(
                tl::unexpected(batched_query_results[i].error()));
        }
    }

    // If we have any valid queries, process them
    if (!valid_keys.empty()) {
        std::unordered_map<std::string, std::vector<Slice>> valid_slices;
        for (const auto& key : valid_keys) {
            auto it = slices.find(key);
            if (it != slices.end()) {
                valid_slices[key] = it->second;
            }
        }

        auto valid_results =
            BatchGet(valid_keys, valid_query_results, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
Client::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto result = master_client_.BatchQueryIp(client_ids);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
Client::QueryByRegex(const std::string& str) {
    auto result = master_client_.GetReplicaListByRegex(str);
    return result;
}

tl::expected<QueryResult, ErrorCode> Client::Query(
    const std::string& object_key) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_.GetReplicaList(object_key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return QueryResult(
        std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(result.value().lease_ttl_ms),
        result.value().object_checksum);
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys) {
    return BatchQuery(object_keys, master_client_.tenant_id());
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys, const std::string& tenant_id) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response = master_client_.BatchGetReplicaList(object_keys, tenant_id);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<QueryResult, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            results.emplace_back(QueryResult(
                std::move(response[i].value().replicas),
                start_time +
                    std::chrono::milliseconds(response[i].value().lease_ttl_ms),
                response[i].value().object_checksum));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

tl::expected<uint64_t, ErrorCode> Client::ComputeObjectChecksumForSlices(
    const std::string& object_key, const std::vector<Slice>& slices,
    size_t object_size) {
    CrcChecksum checksum;
    size_t remaining = object_size;
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    std::unique_ptr<ScopedObjectChecksumBuffer> staging;

    for (const auto& slice : slices) {
        const size_t bytes = std::min(slice.size, remaining);
        if (bytes == 0) {
            continue;
        }
        if (slice.ptr == nullptr) {
            LOG(ERROR) << "object_checksum_null_slice key=" << object_key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (!runtime_accelerator.FindDeviceForPointer(slice.ptr)) {
            checksum.Update(slice.ptr, bytes);
            remaining -= bytes;
            continue;
        }

        if (!staging) {
            staging = std::make_unique<ScopedObjectChecksumBuffer>(
                *pinned_buffer_pool_,
                std::min(kObjectChecksumD2HChunkSize, object_size));
        }
        size_t offset = 0;
        while (offset < bytes) {
            const size_t chunk =
                std::min(kObjectChecksumD2HChunkSize, bytes - offset);
            const auto* source = static_cast<const char*>(slice.ptr) + offset;
            if (!runtime_accelerator.CopyToHost(staging->data(), source,
                                                chunk)) {
                LOG(ERROR) << "object_checksum_d2h_failed key=" << object_key
                           << " size=" << chunk;
                return tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
            checksum.Update(staging->data(), chunk);
            offset += chunk;
        }
        remaining -= bytes;
    }

    if (remaining != 0) {
        LOG(ERROR) << "object_checksum_slices_too_small key=" << object_key
                   << " missing_bytes=" << remaining;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return checksum.Finalize();
}

tl::expected<void, ErrorCode> Client::VerifyObjectChecksum(
    const std::string& object_key, const std::vector<Slice>& slices,
    size_t object_size, std::optional<uint64_t> expected_checksum) {
    if (!object_checksum_enabled_) {
        return {};
    }
    if (!expected_checksum.has_value()) {
        VLOG(1) << "object_checksum_absent key=" << object_key;
        return {};
    }

    auto actual_checksum =
        ComputeObjectChecksumForSlices(object_key, slices, object_size);
    if (!actual_checksum) {
        return tl::unexpected(actual_checksum.error());
    }
    if (*actual_checksum != *expected_checksum) {
        LOG(ERROR) << "object_checksum_mismatch key=" << object_key
                   << " expected=" << *expected_checksum
                   << " actual=" << *actual_checksum;
        return tl::unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode> Client::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    auto result =
        master_client_.BatchReplicaClear(object_keys, client_id, segment_name);
    return result;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices) {
    // Find the first complete replica
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    // Check local hot cache and update replica descriptor if cache hit
    bool cache_used = false;
    if (hot_cache_ && replica.is_memory_replica()) {
        cache_used = RedirectToHotCache(object_key, replica);
    }

    auto t0_get = std::chrono::steady_clock::now();
    // KVC P2a 直读（R9: 补挂单 get 主路径; hot_cache 命中时 replica 已改指向
    // 本地缓存 buffer, 几何校验天然不过, 自动走 TransferRead）
    bool kvc_direct = kvcflip::direct_read(replica, slices, 0);
    if (kvc_direct && kvcflip::direct_checksum_enabled() &&
        !kvcflip::direct_checksum_ok(object_key, slices,
                                     query_result.object_checksum)) {
        kvcflip::g_checksum_rejects++;
        kvc_direct = false;
    }
    err = kvc_direct ? ErrorCode::OK : TransferRead(replica, slices);

    // Release the cache block after transfer completes (memcpy is done)
    if (hot_cache_ && cache_used) {
        hot_cache_->ReleaseHotKey(object_key);
    }

    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }

    auto checksum_result =
        VerifyObjectChecksum(object_key, slices, calculate_total_size(replica),
                             query_result.object_checksum);
    if (!checksum_result) {
        return tl::unexpected(checksum_result.error());
    }

    // Frequency admission: only promote frequently accessed keys to hot cache.
    // Skip when cache_used — data was already served from local cache, no need
    // to re-promote or increment the CMS counter.
    if (ShouldAdmitToHotCache(object_key, cache_used)) {
        ProcessSlicesAsync(object_key, slices, replica);
    }

    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    // Log cache hit statistics
    if (hot_cache_ && replica.is_memory_replica()) {
        VLOG(1) << "Get completed: key=" << object_key
                << " cache_hit=" << (cache_used ? 1 : 0);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices,
                                          uint64_t src_offset) {
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }
    if (!replica.is_memory_replica()) {
        LOG(ERROR) << "Range read only supported for memory replicas, key="
                   << object_key;
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    auto t0_get = std::chrono::steady_clock::now();
    bool kvc_direct = kvcflip::direct_read(replica, slices, src_offset);  // KVC P2a
    if (kvc_direct && kvcflip::direct_checksum_enabled()) {
        // R5: 直读命中后内容级校验（快算+首验标记）, 不符回退 TE
        if (!kvcflip::direct_checksum_ok(object_key, slices,
                                         query_result.object_checksum)) {
            kvcflip::g_checksum_rejects++;
            kvc_direct = false;   // 校验失败: 数据存疑, 交给权威 TE 路径
        }
    }
    if (!kvc_direct)
        err = TransferReadRange(replica, slices, src_offset);
    else
        err = ErrorCode::OK;
    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_range_failed key=" << object_key;
        return tl::unexpected(err);
    }
    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    return {};
}

std::optional<TransferEngine::ScatterTransferOperation> Client::SubmitScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitScatter(transfers);
}

struct BatchGetOperation {
    std::vector<Replica::Descriptor> replicas;
    std::vector<std::vector<Slice>> batched_slices;
    std::vector<size_t> key_indexes;
    std::vector<bool> cache_used;  // Track which keys used cache
    std::vector<TransferFuture> futures;
};

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetWhenPreferSameNode(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.resize(object_keys.size());

    std::unordered_map<std::string, BatchGetOperation> seg_to_op_map{};
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = query_results[i].replicas;
        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        Replica::Descriptor replica;
        ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }
        if (!replica.is_memory_replica()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Check local hot cache and update replica descriptor if cache hit
        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
        }

        // KVC P2a 直读（R9: 补挂 prefer_alloc_in_same_node 分支; 命中则
        // 不进 TE 分组。hot_cache 重定向后的 replica 几何校验天然不过）
        bool kvc_direct = kvcflip::direct_read(replica, slices_it->second, 0);
        if (kvc_direct && kvcflip::direct_checksum_enabled() &&
            !kvcflip::direct_checksum_ok(
                key, slices_it->second, query_results[i].object_checksum)) {
            kvcflip::g_checksum_rejects++;
            kvc_direct = false;
        }
        if (kvc_direct) {
            results[i] = {};
            continue;
        }

        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        auto& op = seg_to_op_map[seg];
        op.replicas.emplace_back(replica);
        op.batched_slices.emplace_back(slices_it->second);
        op.key_indexes.emplace_back(i);
        op.cache_used.emplace_back(cache_used);
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        auto future = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::READ);
        if (!future) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
            continue;
        }
        op.futures.emplace_back(std::move(*future));
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        if (op.futures.empty()) {
            continue;
        }
        ErrorCode result = op.futures[0].get();
        if (result != ErrorCode::OK) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                // Release cache block even on failure (memcpy may have started)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
        } else {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                VLOG(1) << "Transfer completed successfully for key: "
                        << object_keys[index];

                // Release the cache block after transfer completes (memcpy is
                // done)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }

                auto checksum_result = VerifyObjectChecksum(
                    object_keys[index], op.batched_slices[idx],
                    calculate_total_size(op.replicas[idx]),
                    query_results[index].object_checksum);
                if (!checksum_result) {
                    results[index] = tl::unexpected(checksum_result.error());
                    continue;
                }
                results[index] = {};

                // Frequency admission: only promote frequently accessed keys.
                // Skip when cache was used (data served from local cache).
                if (idx < op.replicas.size() &&
                    idx < op.batched_slices.size() &&
                    ShouldAdmitToHotCache(
                        object_keys[index],
                        idx < op.cache_used.size() && op.cache_used[idx])) {
                    ProcessSlicesAsync(object_keys[index],
                                       op.batched_slices[idx],
                                       op.replicas[idx]);
                }
            }
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool prefer_alloc_in_same_node) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Validate input size consistency
    if (query_results.size() != object_keys.size()) {
        LOG(ERROR) << "Query results size (" << query_results.size()
                   << ") doesn't match object keys size (" << object_keys.size()
                   << ")";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }
    if (prefer_alloc_in_same_node) {
        return BatchGetWhenPreferSameNode(object_keys, query_results, slices);
    }

    // Collect all transfer operations for parallel execution
    // Tuple: (index, key, future, replica, cache_used)
    std::vector<std::tuple<size_t, std::string, TransferFuture,
                           Replica::Descriptor, bool>>
        pending_transfers;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
    // Record batch get transfer latency (Submit + Wait)
    auto t0_batch_get = std::chrono::steady_clock::now();

    // Collect cache hit statistics for the entire batch
    size_t total_cache_hits = 0;

    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& query_result = query_results[i];

        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Find the first complete replica for this key
        Replica::Descriptor replica;
        ErrorCode err =
            FindFirstCompleteReplica(query_result.replicas, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
            if (cache_used) {
                total_cache_hits++;
            }
        }

        // KVC P2a: 直读命中则跳过 TE（数据已在共享内存, 校验 READY 后 memcpy）
        bool kvc_direct = kvcflip::direct_read(replica, slices_it->second, 0);
        if (kvc_direct && kvcflip::direct_checksum_enabled()) {
            // R5: 直读命中后内容级校验（快算+首验标记）, 不符回退 TE
            if (!kvcflip::direct_checksum_ok(key, slices_it->second,
                                             query_result.object_checksum)) {
                kvcflip::g_checksum_rejects++;
                kvc_direct = false;
            }
        }
        if (kvc_direct) {
            results[i] = {};
            continue;
        }

        // Submit transfer operation asynchronously
        std::optional<TransferFuture> future;
        if (replica.is_nof_replica()) {
            auto contiguous_range = GetContiguousSliceRange(slices_it->second);
            if (!contiguous_range.has_value()) {
                LOG(ERROR) << "NoF transfer requires contiguous slices";
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            future = transfer_submitter_->submit(
                replica, slices_it->second, TransferRequest::READ,
                contiguous_range->ptr, contiguous_range->size);
        } else {
            future = transfer_submitter_->submit(replica, slices_it->second,
                                                 TransferRequest::READ);
        }
        if (!future) {
            // Release cache block if submit failed
            if (hot_cache_ && cache_used) {
                hot_cache_->ReleaseHotKey(key);
            }
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future), replica,
                                       cache_used);
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future, stored_replica, cache_used] :
         pending_transfers) {
        ErrorCode result = future.get();

        // Release the cache block after transfer completes (memcpy is done)
        if (hot_cache_ && cache_used) {
            hot_cache_->ReleaseHotKey(key);
        }
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;

            auto slices_it = slices.find(key);
            if (slices_it == slices.end()) {
                results[index] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            auto checksum_result = VerifyObjectChecksum(
                key, slices_it->second, calculate_total_size(stored_replica),
                query_results[index].object_checksum);
            if (!checksum_result) {
                results[index] = tl::unexpected(checksum_result.error());
                continue;
            }
            results[index] = {};

            // Frequency admission: only promote frequently accessed keys.
            // Skip when cache was used (data served from local cache).
            if (hot_cache_) {
                auto slices_it = slices.find(key);
                if (slices_it != slices.end() &&
                    ShouldAdmitToHotCache(key, cache_used)) {
                    ProcessSlicesAsync(key, slices_it->second, stored_replica);
                }
            }
        }
    }

    // As lease expired is a rare case, we check all the results with the same
    // time_point to avoid too many syscalls
    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    for (size_t i = 0; i < object_keys.size(); ++i) {
        if (results[i].has_value() && query_results[i].IsLeaseExpired(now)) {
            LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                         << object_keys[i];
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
    }

    auto us_batch_get = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0_batch_get)
                            .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_get_latency_us.observe(us_batch_get);
    }

    // Log overall cache hit statistics for the entire batch
    if (hot_cache_) {
        VLOG(1) << "BatchGet completed: num_keys=" << object_keys.size()
                << " total_cache_hits=" << total_cache_hits;
    } else {
        VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    }
    return results;
}

bool Client::RedirectToHotCache(const std::string& key,
                                Replica::Descriptor& replica) {
    if (!replica.is_memory_replica() || !hot_cache_) {
        return false;
    }

    auto& mem_desc = replica.get_memory_descriptor();
    HotMemBlock* blk = hot_cache_->GetHotKey(key);
    if (blk == nullptr) {
        return false;
    }

    if (mem_desc.buffer_descriptor.size_ != blk->size) {
        LOG(ERROR) << "Cache hit but size mismatch for key: " << key;
        return false;
    }

    mem_desc.buffer_descriptor.transport_endpoint_ =
        (metadata_connstring_ == P2PHANDSHAKE) ? GetTransportEndpoint()
                                               : local_hostname_;
    mem_desc.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(blk->addr);
    return true;
}

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    std::optional<uint64_t> object_checksum;
    if (object_checksum_enabled_) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            key, slices, CalculateSliceSize(slices));
        if (!checksum_result) {
            return tl::unexpected(checksum_result.error());
        }
        object_checksum = *checksum_result;
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    // Start put operation
    auto start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start put operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start put operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    ReplicaTransferSummary transfer_summary;
    for (const auto& replica : start_result.value()) {
        transfer_summary.RecordAllocatedReplica(replica);
        kvcflip::flip(replica, key, /*writing=*/true);   // KVC P1: PutStart
    }
    // R4: 记录传输失败的 memory replica, 撤销时联动 slot 置 INVALID
    std::vector<mooncake::Replica::Descriptor> kvc_failed_replicas;

    // Record Put transfer latency (all replicas)
    auto t0_put = std::chrono::steady_clock::now();

    // We must deal with disk replica first, then the disk putrevoke/putend can
    // be called surely
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                // Store to local file if storage backend is available
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;  // Only one disk replica is needed
            }
        }
    }

    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica() || replica.is_nof_replica()) {
            // Transfer data using allocated handles from all replicas
            const auto replica_type = replica.is_memory_replica()
                                          ? ReplicaType::MEMORY
                                          : ReplicaType::NOF_SSD;
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                transfer_summary.RecordFailure(replica_type, transfer_err);
                if (replica.is_memory_replica())
                    kvc_failed_replicas.push_back(replica);
                continue;
            }
            transfer_summary.RecordSuccess(replica_type);
            kvcflip::flip(replica, key, /*writing=*/false);  // KVC P1: 传输完成
        }
    }

    auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_put)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us_put);
    }

    const auto finalize_decision =
        DetermineFinalizeDecision(config, transfer_summary);

    if (finalize_decision.end_type.has_value()) {
        auto end_result = master_client_.PutEnd(
            ObjectMeta{key, object_checksum}, *finalize_decision.end_type);
        if (!end_result) {
            ErrorCode err = end_result.error();
            LOG(ERROR) << "Failed to end put operation: " << err;
            return tl::unexpected(err);
        }
    }

    if (finalize_decision.revoke_type.has_value()) {
        auto revoke_result =
            master_client_.PutRevoke(key, *finalize_decision.revoke_type);
        if (!revoke_result) {
            LOG(ERROR) << "Failed to revoke put operation";
            return tl::unexpected(revoke_result.error());
        }
        for (const auto& replica : kvc_failed_replicas)
            kvcflip::revoke(replica);   // KVC R4: 撤销联动 slot INVALID
    }

    if (!finalize_decision.success) {
        return tl::unexpected(finalize_decision.error);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Upsert(const ObjectKey& key,
                                             std::vector<Slice>& slices,
                                             const ReplicateConfig& config) {
    std::optional<uint64_t> object_checksum;
    if (object_checksum_enabled_) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            key, slices, CalculateSliceSize(slices));
        if (!checksum_result) {
            return tl::unexpected(checksum_result.error());
        }
        object_checksum = *checksum_result;
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    // Start upsert operation
    auto start_result =
        master_client_.UpsertStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start upsert operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start upsert operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    // Record transfer latency
    auto t0 = std::chrono::steady_clock::now();

    // Handle disk replicas first
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;
            }
        }
    }

    // Transfer to memory replicas
    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica()) {
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                auto revoke_result =
                    master_client_.UpsertRevoke(key, ReplicaType::MEMORY);
                if (!revoke_result) {
                    LOG(ERROR) << "Failed to revoke upsert operation";
                    return tl::unexpected(revoke_result.error());
                }
                return tl::unexpected(transfer_err);
            }
        }
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us);
    }

    // End upsert operation
    auto end_result = master_client_.UpsertEnd(ObjectMeta{key, object_checksum},
                                               ReplicaType::MEMORY);
    if (!end_result) {
        ErrorCode err = end_result.error();
        LOG(ERROR) << "Failed to end upsert operation: " << err;
        return tl::unexpected(err);
    }

    // Success-side invalidation: a concurrent read between the pre-upsert
    // RemoveHotKey() and UpsertEnd could have read the old value and submitted
    // an async hot-cache fill with a still-valid token. Invalidate again now
    // that the new value is committed so that stale fill cannot publish.
    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    if (client_cfg.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported for upsert";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    ComputeBatchObjectChecksums(ops);
    StartBatchUpsert(ops, client_cfg);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchUpsert(ops);
    return CollectResults(ops);
}

// TODO: `client.cpp` is too long, consider split it into multiple files
enum class PutOperationState {
    PENDING,
    MASTER_FAILED,
    TRANSFER_FAILED,
    FINALIZE_FAILED,
    SUCCESS
};

class PutOperation {
   public:
    struct PendingTransferRecord {
        ReplicaType replica_type;
        TransferFuture future;
        kvcflip::SlotRef kvc_slot;   // KVC P1: mark_writing 占据的 {region, slot}

        PendingTransferRecord(ReplicaType type,
                               TransferFuture&& transfer_future)
            : replica_type(type), future(std::move(transfer_future)) {}
        PendingTransferRecord(ReplicaType type,
                              TransferFuture&& transfer_future,
                              kvcflip::SlotRef slot)
            : replica_type(type), future(std::move(transfer_future)),
              kvc_slot(slot) {}
    };

    PutOperation(std::string_view k, const std::vector<Slice>& s)
        : key(k), slices(s) {
        // Initialize with a pending error state to ensure result is always set
        result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string key;
    std::vector<Slice> slices;
    std::optional<uint64_t> object_checksum;
    std::vector<std::vector<Slice>> batched_slices;

    // Enhanced state tracking
    PutOperationState state = PutOperationState::PENDING;
    tl::expected<void, ErrorCode> result;
    std::vector<Replica::Descriptor> replicas;
    std::vector<PendingTransferRecord> pending_transfers;

    size_t requested_memory_replicas = 0;
    size_t requested_nof_replicas = 0;
    ReplicaTransferSummary transfer_summary;

    // Error context for debugging
    std::optional<std::string> failure_context;

    // Helper methods for robust state management
    void SetSuccess() {
        state = PutOperationState::SUCCESS;
        result = {};
        failure_context.reset();
    }

    void SetTerminalError(ErrorCode error, PutOperationState terminal_state,
                          const std::string& context = "") {
        state = terminal_state;
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = context;
        }
    }

    void SetError(ErrorCode error, const std::string& context = "") {
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = toString(error) + ": " + context + "; " +
                              failure_context.value_or("");
        }

        // Update state based on current processing stage
        if (replicas.empty()) {
            state = PutOperationState::MASTER_FAILED;
        } else if (pending_transfers.empty()) {
            state = PutOperationState::TRANSFER_FAILED;
        } else {
            state = PutOperationState::FINALIZE_FAILED;
        }
    }

    void AppendFailureContext(const std::string& context) {
        if (context.empty()) {
            return;
        }
        if (!failure_context.has_value()) {
            failure_context = context;
            return;
        }
        failure_context = *failure_context + "; " + context;
    }

    void InitializeRequestedReplicas(const ReplicateConfig& config) {
        requested_memory_replicas = config.replica_num;
        requested_nof_replicas = config.nof_replica_num;
    }

    ReplicateConfig ToReplicateConfig() const {
        ReplicateConfig config;
        config.replica_num = requested_memory_replicas;
        config.nof_replica_num = requested_nof_replicas;
        return config;
    }

    void RecordAllocatedReplicas() {
        transfer_summary = ReplicaTransferSummary{};
        for (const auto& replica : replicas) {
            transfer_summary.RecordAllocatedReplica(replica);
        }
    }

    bool IsResolved() const { return state != PutOperationState::PENDING; }

    bool IsSuccessful() const {
        return state == PutOperationState::SUCCESS && result.has_value();
    }
};

std::vector<PutOperation> Client::CreatePutOperations(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<PutOperation> ops;
    ops.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
    }
    return ops;
}

void Client::ComputeBatchObjectChecksums(std::vector<PutOperation>& ops) {
    if (!object_checksum_enabled_) {
        return;
    }
    for (auto& op : ops) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            op.key, op.slices, CalculateSliceSize(op.slices));
        if (!checksum_result) {
            op.SetTerminalError(checksum_result.error(),
                                PutOperationState::MASTER_FAILED,
                                "Object checksum calculation failed");
            continue;
        }
        op.object_checksum = *checksum_result;
    }
}

void Client::StartBatchPut(std::vector<PutOperation>& ops,
                           const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    std::vector<size_t> active_indices;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());
    active_indices.reserve(ops.size());

    if (hot_cache_) {
        std::vector<std::string> hot_keys;
        hot_keys.reserve(ops.size());
        for (const auto& op : ops) hot_keys.emplace_back(op.key);
        hot_cache_->RemoveHotKeys(hot_keys);
    }

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (op.IsResolved()) {
            continue;
        }
        active_indices.emplace_back(i);
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    if (active_indices.empty()) {
        return;
    }

    auto start_responses =
        master_client_.BatchPutStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != active_indices.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << active_indices.size() << ", got "
                   << start_responses.size();
        for (size_t index : active_indices) {
            auto& op = ops[index];
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchPutStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < active_indices.size(); ++i) {
        auto& op = ops[active_indices[i]];
        op.InitializeRequestedReplicas(config);
        if (!start_responses[i]) {
            op.SetTerminalError(start_responses[i].error(),
                                PutOperationState::MASTER_FAILED,
                                "Master failed to start put operation");
        } else {
            op.replicas = start_responses[i].value();
            op.RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config, op.transfer_summary)) {
                op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                    PutOperationState::MASTER_FAILED,
                                    "Allocated replicas do not satisfy "
                                    "requested replica policy");
                continue;
            }
            // Operation continues to next stage - result remains INTERNAL_ERROR
            // until fully successful
            VLOG(1) << "Successfully started put for key " << op.key << " with "
                    << op.replicas.size() << " replicas";
        }
    }
}

void Client::StartBatchUpsert(std::vector<PutOperation>& ops,
                              const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    std::vector<size_t> active_indices;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());
    active_indices.reserve(ops.size());

    if (hot_cache_) {
        std::vector<std::string> hot_keys;
        hot_keys.reserve(ops.size());
        for (const auto& op : ops) hot_keys.emplace_back(op.key);
        hot_cache_->RemoveHotKeys(hot_keys);
    }

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (op.IsResolved()) {
            continue;
        }
        active_indices.emplace_back(i);
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    if (active_indices.empty()) {
        return;
    }

    auto start_responses =
        master_client_.BatchUpsertStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != active_indices.size()) {
        LOG(ERROR) << "BatchUpsertStart response size mismatch: expected "
                   << active_indices.size() << ", got "
                   << start_responses.size();
        for (size_t index : active_indices) {
            auto& op = ops[index];
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchUpsertStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < active_indices.size(); ++i) {
        auto& op = ops[active_indices[i]];
        if (!start_responses[i]) {
            op.SetError(start_responses[i].error(),
                        "Master failed to start upsert operation");
        } else {
            op.replicas = start_responses[i].value();
            VLOG(1) << "Successfully started upsert for key " << op.key
                    << " with " << op.replicas.size() << " replicas";
        }
    }
}

void Client::SubmitTransfers(std::vector<PutOperation>& ops) {
    if (std::all_of(ops.begin(), ops.end(),
                    [](const PutOperation& op) { return op.IsResolved(); })) {
        return;
    }
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        for (auto& op : ops) {
            if (op.IsResolved()) {
                continue;
            }
            op.SetTerminalError(ErrorCode::INVALID_PARAMS,
                                PutOperationState::TRANSFER_FAILED,
                                "TransferSubmitter not initialized");
        }
        return;
    }

    for (auto& op : ops) {
        // Skip operations that already failed in previous stages
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations that don't have replicas (failed in StartBatchPut)
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "No replicas available for transfer");
            continue;
        }

        // We must deal with disk replica first, then the disk putrevoke/putend
        // can be called surely
        if (storage_backend_) {
            for (auto it = op.replicas.rbegin(); it != op.replicas.rend();
                 ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    auto disk_descriptor = replica.get_disk_descriptor();
                    PutToLocalFile(op.key, op.slices, disk_descriptor);
                    break;  // Only one disk replica is needed
                }
            }
        }

        for (size_t replica_idx = 0; replica_idx < op.replicas.size();
             ++replica_idx) {
            const auto& replica = op.replicas[replica_idx];
            if (replica.is_memory_replica() || replica.is_nof_replica()) {
                const auto replica_type = replica.is_memory_replica()
                                              ? ReplicaType::MEMORY
                                              : ReplicaType::NOF_SSD;
                std::optional<TransferFuture> submit_result;
                if (replica.is_nof_replica()) {
                    auto contiguous_range = GetContiguousSliceRange(op.slices);
                    if (!contiguous_range.has_value()) {
                        std::string failure_context =
                            "NoF transfer requires contiguous slices for "
                            "replica " +
                            std::to_string(replica_idx);
                        op.transfer_summary.RecordFailure(
                            replica_type, ErrorCode::INVALID_PARAMS);
                        op.AppendFailureContext(failure_context);
                        continue;
                    }
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE,
                        contiguous_range->ptr, contiguous_range->size);
                } else {
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE);
                }

                if (!submit_result) {
                    std::string failure_context =
                        "Failed to submit transfer for replica " +
                        std::to_string(replica_idx);
                    op.transfer_summary.RecordFailure(replica_type,
                                                      ErrorCode::TRANSFER_FAIL);
                    op.AppendFailureContext(failure_context);
                    continue;
                }

                kvcflip::SlotRef kvc_slot = kvcflip::flip(replica, op.key,
                                                          /*writing=*/true);
                op.pending_transfers.emplace_back(
                    replica_type, std::move(submit_result.value()), kvc_slot);
            }
        }

        VLOG(1) << "Submitted " << op.pending_transfers.size()
                << " transfers for key " << op.key;
    }
}

void Client::WaitForTransfers(std::vector<PutOperation>& ops) {
    /* KVC P2e: 批量 flip —— 成功传输的 slot 先收集, 循环外集中发布,
     * 摊薄逐对象原子发布的 cacheline 开销 */
    std::vector<kvcflip::SlotRef> ready_batch;
    for (auto& op : ops) {
        // Skip operations that already failed or completed
        if (op.IsResolved()) {
            continue;
        }

        for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
            auto& pending_transfer = op.pending_transfers[i];
            ErrorCode transfer_result = pending_transfer.future.get();
            if (transfer_result != ErrorCode::OK) {
                op.transfer_summary.RecordFailure(pending_transfer.replica_type,
                                                  transfer_result);
                std::string error_context =
                    "Transfer " + std::to_string(i) + " failed";
                op.AppendFailureContext(error_context);
            } else {
                op.transfer_summary.RecordSuccess(
                    pending_transfer.replica_type);
                if (pending_transfer.kvc_slot.valid())
                    ready_batch.push_back(pending_transfer.kvc_slot);
            }
        }

        VLOG(1) << "Transfers finished for key " << op.key << ", success(mem="
                << op.transfer_summary.successful_memory_transfers
                << ", nof=" << op.transfer_summary.successful_nof_transfers
                << "), fail(mem=" << op.transfer_summary.failed_memory_transfers
                << ", nof=" << op.transfer_summary.failed_nof_transfers << ")";
    }

    for (const auto &ref : ready_batch) {   // KVC P2e: 集中发布
        kvcflip::ready_slot(ref);
    }
}

void Client::FinalizeBatchPut(std::vector<PutOperation>& ops) {
    struct BatchFinalizeGroup {
        std::vector<std::string> keys;
        std::vector<size_t> indices;
    };

    BatchFinalizeGroup end_all_group;
    BatchFinalizeGroup end_memory_group;
    BatchFinalizeGroup end_nof_group;
    BatchFinalizeGroup revoke_all_group;
    BatchFinalizeGroup revoke_memory_group;
    BatchFinalizeGroup revoke_nof_group;

    std::vector<size_t> pending_finalize_actions(ops.size(), 0);
    std::vector<bool> should_succeed(ops.size(), false);
    std::vector<ErrorCode> terminal_errors(ops.size(), ErrorCode::OK);
    std::vector<std::optional<ErrorCode>> finalize_rpc_errors(ops.size());

    auto add_group_entry = [](BatchFinalizeGroup& group, const std::string& key,
                              size_t index) {
        group.keys.emplace_back(key);
        group.indices.emplace_back(index);
    };

    auto add_finalize_action =
        [&](const std::optional<ReplicaType>& replica_type, bool is_end,
            const std::string& key, size_t index) {
            if (!replica_type.has_value()) {
                return;
            }
            switch (*replica_type) {
                case ReplicaType::ALL:
                    add_group_entry(is_end ? end_all_group : revoke_all_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::MEMORY:
                    add_group_entry(
                        is_end ? end_memory_group : revoke_memory_group, key,
                        index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::NOF_SSD:
                    add_group_entry(is_end ? end_nof_group : revoke_nof_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
    default:
                    LOG(ERROR) << "Unexpected replica type in batch finalize: "
                               << *replica_type;
                    finalize_rpc_errors[index] = ErrorCode::INVALID_PARAMS;
                    break;
            }
        };

    auto complete_finalize_action = [&](size_t index) {
        if (pending_finalize_actions[index] > 0) {
            --pending_finalize_actions[index];
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful() && !op.replicas.empty()) {
                terminal_errors[i] = op.result.has_value()
                                         ? ErrorCode::INTERNAL_ERROR
                                         : op.result.error();
                add_finalize_action(ReplicaType::ALL, false, op.key, i);
            }
            continue;
        }
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "Operation has no replicas to finalize");
            continue;
        }

        const auto finalize_decision = DetermineFinalizeDecision(
            op.ToReplicateConfig(), op.transfer_summary);
        should_succeed[i] = finalize_decision.success;
        terminal_errors[i] = finalize_decision.error;
        add_finalize_action(finalize_decision.end_type, true, op.key, i);
        add_finalize_action(finalize_decision.revoke_type, false, op.key, i);
    }

    auto process_end_group = [&](BatchFinalizeGroup& group,
                                 ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        std::vector<ObjectMeta> object_metas;
        object_metas.reserve(group.indices.size());
        for (size_t i = 0; i < group.indices.size(); ++i) {
            object_metas.emplace_back(ObjectMeta{
                group.keys[i], ops[group.indices[i]].object_checksum});
        }
        auto responses = master_client_.BatchPutEnd(object_metas, replica_type);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutEnd key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    auto process_revoke_group = [&](BatchFinalizeGroup& group,
                                    ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        for (size_t i = 0; i < group.indices.size(); ++i) {
            // KVC R4: 撤销联动 slot INVALID（按 op 持有的 memory replicas）
            for (const auto& r : ops[group.indices[i]].replicas)
                kvcflip::revoke(r);
        }
        auto responses =
            master_client_.BatchPutRevoke(group.keys, replica_type);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutRevoke key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    process_end_group(end_all_group, ReplicaType::ALL);
    process_end_group(end_memory_group, ReplicaType::MEMORY);
    process_end_group(end_nof_group, ReplicaType::NOF_SSD);
    process_revoke_group(revoke_all_group, ReplicaType::ALL);
    process_revoke_group(revoke_memory_group, ReplicaType::MEMORY);
    process_revoke_group(revoke_nof_group, ReplicaType::NOF_SSD);

    auto append_finalize_error_context = [&](PutOperation& op, size_t index) {
        if (finalize_rpc_errors[index].has_value()) {
            op.AppendFailureContext("Batch finalization RPC failed: " +
                                    toString(*finalize_rpc_errors[index]));
        }
        if (pending_finalize_actions[index] != 0) {
            op.AppendFailureContext(
                "Operation has unfinished finalize actions");
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful()) {
                append_finalize_error_context(op, i);
            }
            continue;
        }
        if (finalize_rpc_errors[i].has_value() ||
            pending_finalize_actions[i] != 0) {
            if (!should_succeed[i] && terminal_errors[i] != ErrorCode::OK) {
                append_finalize_error_context(op, i);
                op.SetTerminalError(
                    terminal_errors[i], PutOperationState::TRANSFER_FAILED,
                    op.failure_context.value_or(
                        "Replica transfer failed before finalize"));
            } else if (finalize_rpc_errors[i].has_value()) {
                op.SetTerminalError(*finalize_rpc_errors[i],
                                    PutOperationState::FINALIZE_FAILED,
                                    "Batch finalization RPC failed");
            } else {
                op.SetTerminalError(
                    ErrorCode::INTERNAL_ERROR,
                    PutOperationState::FINALIZE_FAILED,
                    "Operation has unfinished finalize actions");
            }
            continue;
        }
        if (should_succeed[i]) {
            op.SetSuccess();
            continue;
        }
        op.SetTerminalError(terminal_errors[i],
                            PutOperationState::TRANSFER_FAILED,
                            op.failure_context.value_or(
                                "Replica transfer failed before finalize"));
    }
}

void Client::FinalizeBatchUpsert(std::vector<PutOperation>& ops) {
    std::vector<ObjectMeta> successful_object_metas;
    std::vector<size_t> successful_indices;
    std::vector<std::string> failed_keys;
    std::vector<size_t> failed_indices;

    successful_object_metas.reserve(ops.size());
    successful_indices.reserve(ops.size());
    failed_keys.reserve(ops.size());
    failed_indices.reserve(ops.size());

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];

        if (!op.IsResolved() && !op.replicas.empty() &&
            !op.pending_transfers.empty()) {
            successful_object_metas.emplace_back(
                ObjectMeta{op.key, op.object_checksum});
            successful_indices.emplace_back(i);
        } else if (op.state != PutOperationState::PENDING &&
                   !op.replicas.empty()) {
            failed_keys.emplace_back(op.key);
            failed_indices.emplace_back(i);
        }
    }

    // Process successful operations
    std::vector<std::string> finalized_keys;
    if (!successful_object_metas.empty()) {
        finalized_keys.reserve(successful_object_metas.size());
        auto end_responses =
            master_client_.BatchUpsertEnd(successful_object_metas);
        if (end_responses.size() != successful_object_metas.size()) {
            LOG(ERROR) << "BatchUpsertEnd response size mismatch: expected "
                       << successful_object_metas.size() << ", got "
                       << end_responses.size();
            for (size_t idx : successful_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertEnd response size mismatch");
            }
        } else {
            for (size_t i = 0; i < end_responses.size(); ++i) {
                const size_t op_idx = successful_indices[i];
                if (!end_responses[i]) {
                    LOG(ERROR) << "Failed to finalize upsert for key "
                               << successful_object_metas[i].key << ": "
                               << toString(end_responses[i].error());
                    ops[op_idx].SetError(end_responses[i].error(),
                                         "BatchUpsertEnd failed");
                } else {
                    ops[op_idx].SetSuccess();
                    finalized_keys.emplace_back(successful_object_metas[i].key);
                    VLOG(1) << "Successfully completed upsert for key "
                            << successful_object_metas[i].key;
                }
            }
        }
    }

    // Success-side invalidation for finalized upserts only: a concurrent read
    // between StartBatchUpsert()'s pre-invalidation and BatchUpsertEnd could
    // have read the old value and submitted an async hot-cache fill with a
    // still-valid token. Invalidate again now that the new values are
    // committed so those stale fills cannot publish.
    if (hot_cache_ && !finalized_keys.empty()) {
        hot_cache_->RemoveHotKeys(finalized_keys);
    }

    // Process failed operations that need cleanup
    if (!failed_keys.empty()) {
        auto revoke_responses = master_client_.BatchUpsertRevoke(failed_keys);
        if (revoke_responses.size() != failed_keys.size()) {
            LOG(ERROR) << "BatchUpsertRevoke response size mismatch: expected "
                       << failed_keys.size() << ", got "
                       << revoke_responses.size();
            for (size_t idx : failed_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertRevoke response size mismatch");
            }
        } else {
            for (size_t i = 0; i < revoke_responses.size(); ++i) {
                const size_t op_idx = failed_indices[i];
                if (!revoke_responses[i]) {
                    LOG(ERROR)
                        << "Failed to revoke upsert for key " << failed_keys[i]
                        << ": " << toString(revoke_responses[i].error());
                    std::string original_context =
                        ops[op_idx].failure_context.value_or("unknown error");
                    ops[op_idx].failure_context =
                        original_context + "; revoke also failed";
                } else {
                    LOG(INFO) << "Successfully revoked failed upsert for key "
                              << failed_keys[i];
                }
            }
        }
    }

    // Ensure all operations have definitive results
    for (auto& op : ops) {
        if (!op.IsResolved()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "Operation not resolved after finalization");
            LOG(ERROR) << "Operation for key " << op.key
                       << " was not properly resolved";
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::CollectResults(
    const std::vector<PutOperation>& ops) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(ops.size());

    int no_available_handle_count = 0;
    for (const auto& op : ops) {
        // With the new structure, result is always set (never nullopt)
        results.emplace_back(op.result);

        // Additional validation and logging for debugging
        if (!op.result.has_value()) {
            // If error == object already exist, consider as ok (Put semantics).
            // UpsertStart never returns this error, so this branch is
            // unreachable for BatchUpsert — kept for BatchPut compatibility.
            if (op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                results.back() = {};
                continue;
            }
            if (op.result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                LOG(ERROR) << "Operation for key " << op.key
                           << " failed: " << toString(op.result.error())
                           << (op.failure_context
                                   ? (" (" + *op.failure_context + ")")
                                   : "");
            }
        } else {
            VLOG(1) << "Operation for key " << op.key
                    << " completed successfully";
        }
    }
    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPut failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutWhenPreferSameNode(
    std::vector<PutOperation>& ops) {
    auto t0 = std::chrono::steady_clock::now();
    std::unordered_map<std::string, PutOperation> seg_to_ops{};
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        if (op.replicas.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No replicas available for transfer");
            continue;
        }
        auto replica = op.replicas[0];
        if (!replica.is_memory_replica()) {
            op.SetError(ErrorCode::INVALID_PARAMS, "only memory is supported.");
            continue;
        }
        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            op.SetError(ErrorCode::INVALID_PARAMS, "buffer size is 0.");
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        if (seg_to_ops.find(seg) == seg_to_ops.end()) {
            seg_to_ops.emplace(seg, PutOperation(op.key, op.slices));
        }
        // multiple replica-slices
        seg_to_ops.at(seg).batched_slices.emplace_back(op.slices);
        seg_to_ops.at(seg).replicas.emplace_back(replica);
    }
    std::vector<PutOperation> merged_ops;
    merged_ops.reserve(seg_to_ops.size());
    for (auto& seg_to_op : seg_to_ops) {
        auto& op = seg_to_op.second;
        bool all_transfers_submitted = true;
        std::string failure_context;
        merged_ops.emplace_back(op.key, op.slices);
        auto& merged_op = merged_ops.back();
        merged_op.replicas = op.replicas;
        merged_op.transfer_summary.allocated_memory_replicas = 1;
        auto submit_result = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::WRITE);
        if (!submit_result) {
            failure_context = "Failed to submit batch transfer";
            all_transfers_submitted = false;
        } else {
            merged_op.pending_transfers.emplace_back(
                ReplicaType::MEMORY, std::move(submit_result.value()));
        }
        if (!all_transfers_submitted) {
            LOG(ERROR) << "Transfer submission failed for key " << op.key
                       << ": " << failure_context;
            merged_op.transfer_summary.RecordFailure(ReplicaType::MEMORY,
                                                     ErrorCode::TRANSFER_FAIL);
            merged_op.failure_context = failure_context;
            merged_op.pending_transfers.clear();
        } else {
            VLOG(1) << "Successfully submitted "
                    << merged_op.pending_transfers.size()
                    << " transfers for key " << merged_ops.back().key;
        }
    }
    WaitForTransfers(merged_ops);
    for (auto& op : merged_ops) {
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        seg_to_ops.at(seg).transfer_summary = op.transfer_summary;
        seg_to_ops.at(seg).failure_context = op.failure_context;
    }
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        op.transfer_summary.successful_memory_transfers =
            seg_to_ops.at(seg).transfer_summary.successful_memory_transfers > 0
                ? 1
                : 0;
        op.transfer_summary.failed_memory_transfers =
            seg_to_ops.at(seg).transfer_summary.failed_memory_transfers > 0 ? 1
                                                                            : 0;
        op.transfer_summary.first_error =
            seg_to_ops.at(seg).transfer_summary.first_error;
        op.failure_context = seg_to_ops.at(seg).failure_context;
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }
    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    ComputeBatchObjectChecksums(ops);
    if (client_cfg.prefer_alloc_in_same_node) {
        if (client_cfg.nof_replica_num > 0) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                          "NoF replicas";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        if (client_cfg.replica_num != 1) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                          "replica_num != 1";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        StartBatchPut(ops, client_cfg);
        return BatchPutWhenPreferSameNode(ops);
    }
    StartBatchPut(ops, client_cfg);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
Client::StartBatchPutForSizes(const std::vector<std::string>& keys,
                              const std::vector<uint64_t>& object_sizes,
                              const ReplicateConfig& config) {
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (keys.size() != object_sizes.size()) {
        LOG(ERROR) << "StartBatchPutForSizes size mismatch: keys="
                   << keys.size() << ", sizes=" << object_sizes.size();
        return results;
    }

    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    std::vector<std::vector<Slice>> batched_slices(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        batched_slices[i] = {Slice{nullptr, object_sizes[i]}};
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    StartBatchPut(ops, client_cfg);

    for (size_t i = 0; i < ops.size(); ++i) {
        if (ops[i].IsResolved()) {
            results[i] = tl::unexpected(ops[i].result.error());
            continue;
        }
        results[i] = std::move(ops[i].replicas);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutEnd(
    const std::vector<ObjectMeta>& object_metas, ReplicaType replica_type) {
    return master_client_.BatchPutEnd(object_metas, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutRevoke(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_.BatchPutRevoke(keys, replica_type);
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key, bool force) {
    if (hot_cache_) {
        hot_cache_->BumpKeyGeneration(key);
    }

    auto result = master_client_.Remove(key, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveFile(key);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    return {};
}

tl::expected<long, ErrorCode> Client::RemoveByRegex(const ObjectKey& str,
                                                    bool force) {
    if (hot_cache_) {
        hot_cache_->BumpCacheEpoch();
    }

    auto result = master_client_.RemoveByRegex(str, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveByRegex(str);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    if (result.value() > 0 && hot_cache_) {
        hot_cache_->BumpCacheEpoch();
        hot_cache_->RemoveHotKeysByRegex(str);
    }
    return result.value();
}

tl::expected<long, ErrorCode> Client::RemoveAll(bool force) {
    if (hot_cache_) {
        hot_cache_->BumpCacheEpoch();
    }

    auto result = master_client_.RemoveAll(force);
    if (result) {
        if (storage_backend_) {
            storage_backend_->RemoveAll();
        }
    }
    if (result && result.value() > 0 && hot_cache_) {
        hot_cache_->RemoveAllHotKeys();
    }
    return result;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchRemove(
    const std::vector<ObjectKey>& keys, bool force) {
    if (hot_cache_) {
        hot_cache_->BumpKeyGenerations(keys);
    }

    auto results = master_client_.BatchRemove(keys, force);

    if (hot_cache_) {
        std::vector<std::string> removed_keys;
        removed_keys.reserve(std::min(keys.size(), results.size()));
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i < results.size() && results[i].has_value()) {
                removed_keys.emplace_back(keys[i]);
            }
        }
        if (!removed_keys.empty()) {
            hot_cache_->RemoveHotKeys(removed_keys);
        }
    }

    return results;
}

tl::expected<void, ErrorCode> Client::EvictDiskReplica(
    const std::string& key, ReplicaType replica_type) {
    return master_client_.EvictDiskReplica(key, replica_type);
}

tl::expected<void, ErrorCode> Client::EvictDiskReplica(
    const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type) {
    return master_client_.EvictDiskReplica(key, tenant_id, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_.BatchEvictDiskReplica(keys, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, const std::string& tenant_id,
    ReplicaType replica_type) {
    return master_client_.BatchEvictDiskReplica(keys, tenant_id, replica_type);
}

std::vector<int> Client::GetNicNumaNodes() const {
    std::set<int> nodes;
    if (!transfer_engine_) return {};
    auto topo = transfer_engine_->getLocalTopology();
    if (!topo) return {};
    for (auto& [name, entry] : topo->getMatrix()) {
        if (name.rfind("cpu:", 0) != 0 || entry.preferred_hca.empty()) continue;
        int node = std::stoi(name.substr(4));
        nodes.insert(node);
    }
    return {nodes.begin(), nodes.end()};
}

tl::expected<void, ErrorCode> Client::MountSegment(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto result = MountSegmentAndGetId(buffer, size, protocol, location);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegmentImpl(
    std::unordered_map<UUID, Segment, boost::hash<UUID>>::iterator it) {
    auto unmount_result = master_client_.UnmountSegment(it->second.id);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    int rc = transfer_engine_->unregisterLocalMemory(
        reinterpret_cast<void*>(it->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // Otherwise, the segment is already unregistered from transfer
        // engine, we can continue
    }

    mounted_segments_.erase(it);
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                     size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
            it->second.size == size) {
            segment = it;
            break;
        }
    }
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found base=" << buffer << " size=" << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return UnmountSegmentImpl(segment);
}

tl::expected<UUID, ErrorCode> Client::MountSegmentAndGetId(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto check_result = CheckRegisterMemoryParams(buffer, size);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }

    UUID segment_id;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

        // Check if the segment overlaps with any existing segment
        for (auto& it : mounted_segments_) {
            auto& mtseg = it.second;
            uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
            uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
            uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
            uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
            if (std::max(l1, l2) < std::min(r1, r2)) {
                LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                           << " size1=" << mtseg.size << " base2=" << buffer
                           << " size2=" << size;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
        }

        int rc = transfer_engine_->registerLocalMemory((void*)buffer, size,
                                                       location, true, true);
        if (rc != 0) {
            LOG(ERROR) << "register_local_memory_failed base=" << buffer
                       << " size=" << size << ", error=" << rc;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        Segment segment;
        segment.id = generate_uuid();
        segment.name = local_hostname_;
        segment.base = reinterpret_cast<uintptr_t>(buffer);
        segment.size = size;
        segment.protocol = protocol;
        segment.host_id = host_id_;
        if (metadata_connstring_ == P2PHANDSHAKE) {
            segment.te_endpoint = transfer_engine_->getLocalIpAndPort();
        } else {
            segment.te_endpoint = local_hostname_;
        }

        auto mount_result = master_client_.MountSegment(segment);
        if (!mount_result) {
            ErrorCode err = mount_result.error();
            LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                       << " size=" << size << ", error=" << err;
            return tl::unexpected(err);
        }

        segment_id = segment.id;
        mounted_segments_[segment_id] = segment;
    }

    EnsureStorageControlPlaneStarted();
    return segment_id;
}

tl::expected<void, ErrorCode> Client::UnmountSegmentById(
    const UUID& segment_id, uint64_t grace_period_ms,
    std::function<void(const UUID&)> cleanup_callback) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.find(segment_id);
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found id=" << UuidToString(segment_id);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (grace_period_ms == 0) {
        return UnmountSegmentImpl(segment);
    }

    auto result =
        master_client_.GracefulUnmountSegment(segment_id, grace_period_ms);
    if (!result) {
        ErrorCode err = result.error();
        LOG(ERROR) << "Failed to graceful unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    gracefully_unmounting_segments_.emplace(segment->first, segment->second);
    if (cleanup_callback) {
        graceful_unmount_cleanup_callbacks_[segment->first] =
            std::move(cleanup_callback);
    }
    mounted_segments_.erase(segment);
    StartGracefulUnmountTimer(segment_id, grace_period_ms);
    return {};
}

bool Client::WaitForGracefulUnmountDelay(std::chrono::milliseconds delay) {
    std::unique_lock<std::mutex> lock(graceful_unmount_timer_mutex_);
    return graceful_unmount_timer_cv_.wait_for(
        lock, delay, [this]() { return graceful_unmount_timer_stopping_; });
}

void Client::StartGracefulUnmountTimer(const UUID& segment_id,
                                       uint64_t grace_period_ms) {
    auto delay =
        std::chrono::milliseconds(grace_period_ms) + std::chrono::seconds(10);
    task_thread_pool_.enqueue([this, segment_id, delay]() {
        if (this->WaitForGracefulUnmountDelay(delay)) {
            return;
        }
        this->OnGracefulUnmountTimer(segment_id, /*retry_left=*/3);
    });
}

void Client::OnGracefulUnmountTimer(const UUID& segment_id, int retry_left) {
    // Query master to confirm the segment has been removed
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        auto it = gracefully_unmounting_segments_.find(segment_id);
        if (it == gracefully_unmounting_segments_.end()) {
            // Already cleaned up (e.g. by destructor)
            return;
        }
    }

    auto status = master_client_.QuerySegmentStatusById(segment_id);
    bool removed = false;
    if (!status) {
        if (status.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            removed = true;
        } else {
            LOG(WARNING) << "Failed to query graceful unmount segment status: "
                         << toString(status.error());
        }
    } else if (status.value() == SegmentStatus::UNDEFINED) {
        removed = true;
    }

    if (removed) {
        std::function<void(const UUID&)> cleanup_callback;
        {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            auto it = gracefully_unmounting_segments_.find(segment_id);
            if (it != gracefully_unmounting_segments_.end()) {
                int rc = transfer_engine_->unregisterLocalMemory(
                    reinterpret_cast<void*>(it->second.base));
                if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
                    LOG(ERROR)
                        << "Failed to unregister TE MR for graceful unmount: "
                        << rc;
                }
                gracefully_unmounting_segments_.erase(it);
            }
            auto callback_it =
                graceful_unmount_cleanup_callbacks_.find(segment_id);
            if (callback_it != graceful_unmount_cleanup_callbacks_.end()) {
                cleanup_callback = std::move(callback_it->second);
                graceful_unmount_cleanup_callbacks_.erase(callback_it);
            }
        }
        if (cleanup_callback) {
            cleanup_callback(segment_id);
        }
        return;
    }

    if (retry_left > 0) {
        try {
            task_thread_pool_.enqueue([this, segment_id, retry_left]() {
                if (this->WaitForGracefulUnmountDelay(
                        std::chrono::seconds(10))) {
                    return;
                }
                this->OnGracefulUnmountTimer(segment_id, retry_left - 1);
            });
        } catch (const std::runtime_error& e) {
            VLOG(1) << "Skip graceful unmount retry enqueue: " << e.what();
        }
    } else {
        LOG(WARNING) << "Graceful unmount cleanup timeout for segment "
                     << UuidToString(segment_id);
    }
}

tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto check_result = CheckRegisterMemoryParams(addr, length);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto result = master_client_.ExistKey(key);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // Return the response directly as it's already in the correct
    // format
    return response;
}

void* Client::GetBaseAddr() { return transfer_engine_->getBaseAddr(); }

tl::expected<void, ErrorCode> Client::MountLocalDiskSegment(
    bool enable_offloading) {
    auto response =
        master_client_.MountLocalDiskSegment(client_id_, enable_offloading);

    if (!response) {
        LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                   << response.error();
        return response;
    }

    EnsureStorageControlPlaneStarted();
    return response;
}

tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeat(
    bool enable_offloading, std::vector<OffloadTaskItem>& offloading_objects) {
    auto response =
        master_client_.OffloadObjectHeartbeat(client_id_, enable_offloading);
    if (!response) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    offloading_objects = std::move(response.value());
    return {};
}

tl::expected<bool, ErrorCode> Client::PollRemoveAll() {
    return master_client_.PollRemoveAll();
}

tl::expected<void, ErrorCode> Client::ReportSsdCapacity(
    int64_t ssd_total_capacity_bytes) {
    auto response =
        master_client_.ReportSsdCapacity(client_id_, ssd_total_capacity_bytes);
    if (!response) {
        LOG(ERROR) << "ReportSsdCapacity failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices) {
    return BatchGetOffloadObject(transfer_engine_addr, keys, pointers,
                                 batch_slices,
                                 OffloadBufferAccess::kTransferEngine);
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices,
    OffloadBufferAccess buffer_access) {
    auto future = transfer_submitter_->submit_batch_get_offload_object(
        transfer_engine_addr, keys, pointers, batch_slices, buffer_access);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    VLOG(1) << "Using transfer strategy: " << future->strategy();
    auto result = future->get();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Transfer failed, error code is " << result;
        return tl::make_unexpected(result);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    auto response =
        master_client_.NotifyOffloadSuccess(client_id_, keys, metadatas);
    return response;
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    return master_client_.NotifyOffloadSuccess(client_id_, tasks, metadatas);
}

tl::expected<void, ErrorCode> Client::PromotionObjectHeartbeat(
    std::vector<PromotionTaskItem>& promotion_objects) {
    auto response = master_client_.PromotionObjectHeartbeat(client_id_);
    if (!response) {
        return tl::make_unexpected(response.error());
    }
    promotion_objects = std::move(response.value());
    return {};
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, size,
                                              preferred_segments);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, const std::string& tenant_id, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, tenant_id, size,
                                              preferred_segments);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key) {
    return master_client_.NotifyPromotionSuccess(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionSuccess(client_id_, key, tenant_id);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key) {
    return master_client_.NotifyPromotionFailure(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionFailure(client_id_, key, tenant_id);
}

ErrorCode Client::PromotionWrite(const Replica::Descriptor& memory_descriptor,
                                 std::vector<Slice>& slices) {
    return TransferWrite(memory_descriptor, slices);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, tenant_id, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return master_client_.CreateMoveTask(key, source, target);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    return master_client_.CreateMoveTask(key, tenant_id, source, target);
}

tl::expected<void, ErrorCode> Client::ExecuteReplicaTransfer(
    const std::string& key, const std::string& action_name,
    std::function<tl::expected<void, ErrorCode>()> end_fn,
    std::function<tl::expected<void, ErrorCode>()> revoke_fn,
    const Replica::Descriptor& source,
    const std::vector<Replica::Descriptor>& targets) {
    auto revoke_lambda = [&]() {
        auto revoke_result = revoke_fn();
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_" << action_name << "_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
    };

    // currently only memory source replica is supported
    if (!source.is_memory_replica()) {
        LOG(ERROR) << "action=replica_" << action_name << "_failed"
                   << ", key=" << key << ", error=invalid_replica_type";
        revoke_lambda();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate that source replica is in local memory
    if (!IsReplicaOnLocalMemory(source)) {
        LOG(ERROR) << "action=replica_" << action_name << "_failed"
                   << ", key=" << key
                   << ", error=source_replica_not_in_local_memory";
        revoke_lambda();
        return tl::unexpected(ErrorCode::REPLICA_NOT_IN_LOCAL_MEMORY);
    }

    // Split the source replica into slices for transfer
    // This avoids data copy because the source replica is already in memory
    const auto& buffer_descriptor =
        source.get_memory_descriptor().buffer_descriptor;
    void* buffer = reinterpret_cast<void*>(buffer_descriptor.buffer_address_);
    auto slices = split_into_slices(buffer, buffer_descriptor.size_);

    // Transfer to each target
    for (const auto& target : targets) {
        if (TransferWrite(target, slices) != ErrorCode::OK) {
            revoke_lambda();
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }
    }

    // Call end function to finalize
    auto end_result = end_fn();
    if (!end_result.has_value()) {
        revoke_lambda();
        return tl::unexpected(end_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& source,
    const std::vector<std::string>& targets) {
    return Copy(key, master_client_.tenant_id(), source, targets);
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::vector<std::string>& targets) {
    LOG(INFO) << "action=replica_copy_start" << ", key=" << key
              << ", targets_count=" << targets.size();

    // Call CopyStart first - it validates existence and allocates replicas
    auto start_result =
        master_client_.CopyStart(key, tenant_id, source, targets);
    if (!start_result.has_value()) {
        ErrorCode error = start_result.error();
        LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                   << ", source=" << source << ", error=copy_start_failed"
                   << ", error_code=" << error;
        return tl::unexpected(error);
    }

    const auto& response = start_result.value();
    if (response.targets.empty()) {
        LOG(INFO) << "action=replica_copy_skipped" << ", key=" << key
                  << ", info=target_replicas_already_exist";
        // Target replicas already exist, consider it success
        auto copy_end_result = master_client_.CopyEnd(key, tenant_id);
        if (!copy_end_result.has_value()) {
            ErrorCode error = copy_end_result.error();
            LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                       << ", error=copy_end_failed" << ", error_code=" << error;
            return tl::unexpected(error);
        }
        return {};
    }

    auto result = ExecuteReplicaTransfer(
        key, "copy", [&]() { return master_client_.CopyEnd(key, tenant_id); },
        [&]() { return master_client_.CopyRevoke(key, tenant_id); },
        response.source, response.targets);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_copy_success" << ", key=" << key
                  << ", target_count=" << response.targets.size();
    }

    return result;
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& source,
                                           const std::string& target) {
    return Move(key, master_client_.tenant_id(), source, target);
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& tenant_id,
                                           const std::string& source,
                                           const std::string& target) {
    LOG(INFO) << "action=replica_move_start" << ", key=" << key
              << ", source_segment=" << source << ", target_segment=" << target;

    // Call MoveStart first - it validates existence and allocates replica if
    // needed
    auto move_start_result =
        master_client_.MoveStart(key, tenant_id, source, target);
    if (!move_start_result.has_value()) {
        ErrorCode error = move_start_result.error();
        LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                   << ", error=move_start_failed" << ", error_code=" << error;
        // MoveStart already validated existence, so we just return the error
        return tl::unexpected(error);
    }

    const auto& response = move_start_result.value();
    if (!response.target.has_value()) {
        LOG(INFO) << "action=replica_move_skipped" << ", key=" << key
                  << ", info=target_replica_already_exists";
        // Target already exists, consider it success
        auto move_end_result = master_client_.MoveEnd(key, tenant_id);
        if (!move_end_result.has_value()) {
            ErrorCode error = move_end_result.error();
            LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                       << ", error=move_end_failed" << ", error_code=" << error;
            return tl::unexpected(error);
        }
        return {};
    }

    std::vector<Replica::Descriptor> targets = {response.target.value()};

    auto result = ExecuteReplicaTransfer(
        key, "move", [&]() { return master_client_.MoveEnd(key, tenant_id); },
        [&]() { return master_client_.MoveRevoke(key, tenant_id); },
        response.source, targets);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_move_success" << ", key=" << key
                  << ", source_segment=" << source
                  << ", target_segment=" << target;
    }

    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> Client::QueryTask(
    const UUID& task_id) {
    return master_client_.QueryTask(task_id);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> Client::FetchTasks(
    size_t batch_size) {
    return master_client_.FetchTasks(batch_size);
}

tl::expected<void, ErrorCode> Client::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return master_client_.MarkTaskToComplete(update_request);
}

ErrorCode Client::PrepareStorageBackend(const std::string& storage_root_dir,
                                        const std::string& fsdir,
                                        bool enable_eviction,
                                        uint64_t quota_bytes) {
    // Initialize storage backend
    auto backend_result =
        StorageBackend::Create(storage_root_dir, fsdir, enable_eviction);
    if (!backend_result) {
        LOG(ERROR) << "Failed to create storage backend: "
                   << backend_result.error();
        return backend_result.error();
    }
    // Initialize into a local first and only publish to storage_backend_
    // after a successful Init(): users of storage_backend_ only null-check
    // it, so it must never point to a backend whose Init() failed (using it
    // before successful Init() is undefined behavior). If Init() throws,
    // stack unwinding destroys the local and storage_backend_ stays clean.
    auto backend = std::move(backend_result.value());
    auto init_result = backend->Init(quota_bytes);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize StorageBackend. Error: "
                   << init_result.error() << ". The backend will be unusable.";
        return init_result.error();
    }
    storage_backend_ = std::move(backend);
    return ErrorCode::OK;
}

void Client::PutToLocalFile(const std::string& key,
                            const std::vector<Slice>& slices,
                            const DiskDescriptor& disk_descriptor) {
    if (!storage_backend_) return;

    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    std::string path = disk_descriptor.file_path;

    // Synchronous D2H staging + copy into std::string.
    // Done on the calling thread to guarantee GPU buffers are still valid
    // (BatchPut has not yet returned to Python, so blocks are not reused).
    std::string value;
    value.reserve(total_size);
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();

    for (const auto& slice : slices) {
        device::PointerInfo info{};
        auto* device =
            runtime_accelerator.FindDeviceForPointer(slice.ptr, &info);
        if (device) {
            device->SetContext(info.device_id);
            auto buf = pinned_buffer_pool_->Acquire(slice.size);
            if (!device->Copy(buf.data, slice.ptr, slice.size,
                              device::CopyDirection::kDeviceToHost)) {
                LOG(ERROR) << "D2H copy failed for key: " << key
                           << ", triggering PutRevoke for disk replica";
                pinned_buffer_pool_->Release(std::move(buf));
                // Must revoke to avoid phantom replica in master
                auto revoke_result =
                    master_client_.PutRevoke(key, ReplicaType::DISK);
                if (!revoke_result) {
                    LOG(ERROR)
                        << "Failed to revoke put operation for key: " << key;
                }
                return;
            }
            value.append(buf.data, slice.size);
            pinned_buffer_pool_->Release(std::move(buf));
        } else {
            value.append(static_cast<char*>(slice.ptr), slice.size);
        }
    }

    // Async StoreObject + PutEnd (unchanged from original)
    write_thread_pool_.enqueue([this, backend = storage_backend_, key,
                                value = std::move(value), path] {
        ReplicaType replica_type = ReplicaType::DISK;
        // Store the object
        auto store_result = backend->StoreObject(
            path, value, key,
            [this, replica_type](const std::vector<std::string>& evicted_keys)
                -> tl::expected<void, ErrorCode> {
                auto evict_results = master_client_.BatchEvictDiskReplica(
                    evicted_keys, replica_type);
                if (evict_results.size() != evicted_keys.size()) {
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                for (size_t i = 0; i < evict_results.size(); ++i) {
                    if (!evict_results[i]) {
                        if (evict_results[i].error() ==
                            ErrorCode::OBJECT_NOT_FOUND) {
                            VLOG(1) << "Master no longer tracks evicted key: "
                                    << evicted_keys[i];
                            continue;
                        }
                        LOG(WARNING)
                            << "Failed to notify master about evicted key: "
                            << evicted_keys[i]
                            << ", error: " << evict_results[i].error();
                        return tl::make_unexpected(evict_results[i].error());
                    }
                }
                return {};
            });

        if (!store_result) {
            // If storage failed, revoke the put operation
            LOG(ERROR) << "Failed to store object for key: " << key;
            auto revoke_result = master_client_.PutRevoke(key, replica_type);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation for key: " << key;
            }
            return;
        }

        // If storage succeeded, end the put operation
        auto end_result =
            master_client_.PutEnd(ObjectMeta{key, std::nullopt}, replica_type);
        if (!end_result) {
            LOG(ERROR) << "Failed to end put operation for key: " << key;
        }
    });
}

ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferRequest::OpCode op_code) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    std::optional<TransferFuture> future;
    if (replica_descriptor.is_nof_replica()) {
        auto contiguous_range = GetContiguousSliceRange(slices);
        if (!contiguous_range.has_value()) {
            LOG(ERROR) << "NoF transfer requires contiguous slices";
            return ErrorCode::INVALID_PARAMS;
        }
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, contiguous_range->ptr,
                                             contiguous_range->size);
    } else {
        future =
            transfer_submitter_->submit(replica_descriptor, slices, op_code);
    }
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

std::optional<TransferFuture> Client::SubmitRangeRead(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeRead(replica_descriptor, slices,
                                                src_offset);
}

std::optional<TransferFuture> Client::SubmitRangeWrite(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeWrite(replica_descriptor, slices,
                                                 dst_offset);
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferReadRanges(
    const std::vector<Replica::Descriptor>& replicas,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& src_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas.size() != slices.size() ||
        replicas.size() != src_offsets.size()) {
        LOG(ERROR) << "BatchTransferReadRanges size mismatch: replicas="
                   << replicas.size() << ", slices=" << slices.size()
                   << ", offsets=" << src_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (const auto& entry : slices) {
        fragment_count += entry.size();
    }

    // Every fragment of every entry goes into one scatter submit so the
    // transport sees the whole layer at once instead of one transfer per key.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(replicas.size());
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (slices[i].size() != src_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferReadRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << src_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }
        if (!replicas[i].is_memory_replica()) {
            LOG(ERROR) << "Range read requires a memory replica, entry=" << i;
            continue;
        }

        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        int64_t transferred = 0;
        for (size_t j = 0; j < slices[i].size(); ++j) {
            builder.Add(TransferRequest::READ, handle, slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(slices[i][j].size);
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatter(builder.ranges());
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch range read";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    (void)operation->wait();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Range read failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferWriteRanges(
    const std::vector<std::vector<Replica::Descriptor>>& replicas_per_entry,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& dst_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas_per_entry.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas_per_entry.size() != slices.size() ||
        replicas_per_entry.size() != dst_offsets.size()) {
        LOG(ERROR) << "BatchTransferWriteRanges size mismatch: entries="
                   << replicas_per_entry.size() << ", slices=" << slices.size()
                   << ", offsets=" << dst_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        for (const auto& replica : replicas_per_entry[i]) {
            if (replica.is_memory_replica()) {
                fragment_count += slices[i].size();
            }
        }
    }

    // One scatter submit covers every replica of every entry, so replication
    // fans out inside a single transfer instead of one submit per fragment.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(
        replicas_per_entry.size());
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        if (slices[i].size() != dst_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferWriteRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << dst_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }

        // Logical bytes: counted once per fragment, not per replica.
        int64_t transferred = 0;
        for (const auto& slice : slices[i]) {
            transferred += static_cast<int64_t>(slice.size);
        }
        bool submitted = false;
        for (const auto& replica : replicas_per_entry[i]) {
            if (!replica.is_memory_replica()) {
                continue;
            }
            const auto& handle =
                replica.get_memory_descriptor().buffer_descriptor;
            for (size_t j = 0; j < slices[i].size(); ++j) {
                builder.Add(TransferRequest::WRITE, handle, slices[i][j],
                            dst_offsets[i][j], &entry_errors[i]);
                submitted = true;
            }
        }
        if (!submitted) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatter(builder.ranges());
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch range write";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    (void)operation->wait();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Range write failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

ErrorCode Client::TransferReadInternal(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    auto future = SubmitRangeRead(replica_descriptor, slices, src_offset);
    if (!future) {
        LOG(ERROR) << "Failed to submit range read operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferWrite(const Replica::Descriptor& replica_descriptor,
                                std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferWriteRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    auto future = SubmitRangeWrite(replica_descriptor, slices, dst_offset);
    if (!future) {
        LOG(ERROR) << "Failed to submit range write operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();
    return future->get();
}

ErrorCode Client::TransferRead(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_nof_replica()) {
        auto& nof_desc = replica_descriptor.get_nof_descriptor();
        total_size = nof_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    } else if (replica_descriptor.is_local_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_local_disk_descriptor();
        total_size = disk_desc.object_size;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

ErrorCode Client::TransferReadRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    return TransferReadInternal(replica_descriptor, slices, src_offset);
}

void Client::PollAndDispatchTasks() {
    if (task_running_.load()) {
        auto fetch_result = FetchTasks(kTaskBatchSize);
        if (fetch_result.has_value()) {
            const auto& tasks = fetch_result.value();
            if (!tasks.empty()) {
                LOG(INFO) << "action=task_poll_success"
                          << ", task_count=" << tasks.size();
                for (const auto& task_assignment : tasks) {
                    SubmitTask(task_assignment);
                }
            }
        } else {
            ErrorCode error = fetch_result.error();
            // Only log if it's not an RPC failure (which is expected
            // during connection failures)
            if (error != ErrorCode::RPC_FAIL) {
                LOG(WARNING)
                    << "action=task_poll_failed" << ", error_code=" << error;
            }
        }
    }
}

void Client::TaskPollThreadMain() {
    const auto poll_interval = std::chrono::milliseconds(1000);

    while (task_poll_running_.load()) {
        PollAndDispatchTasks();
        std::this_thread::sleep_for(poll_interval);
    }
}

void Client::SubmitTask(const TaskAssignment& assignment) {
    if (!task_running_.load()) {
        LOG(WARNING) << "action=task_rejected" << ", task_id=" << assignment.id
                     << ", reason=executor_stopped";
        return;
    }

    // Construct ClientTask from TaskAssignment
    ClientTask client_task;
    client_task.assignment = assignment;
    client_task.retry_count = 0;

    task_thread_pool_.enqueue(
        [this, client_task]() { ExecuteTask(client_task); });
}

void Client::ExecuteTask(const ClientTask& client_task) {
    const auto& assignment = client_task.assignment;
    ErrorCode result = ErrorCode::OK;

    try {
        switch (assignment.type) {
            case TaskType::REPLICA_COPY: {
                ReplicaCopyPayload payload;
                struct_json::from_json(payload, assignment.payload);
                auto copy_result = Copy(payload.key, payload.tenant_id,
                                        payload.source, payload.targets);
                if (copy_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = copy_result.error();
                }
                break;
            }
            case TaskType::REPLICA_MOVE: {
                ReplicaMovePayload payload;
                struct_json::from_json(payload, assignment.payload);
                auto move_result = Move(payload.key, payload.tenant_id,
                                        payload.source, payload.target);
                if (move_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = move_result.error();
                }
                break;
            }
            case TaskType::KVC_INVALIDATE: {
                // C2: master 驱逐推送 → 失效 shm slot（daemon 侧执行）
                KvcInvalidatePayload payload;
                struct_json::from_json(payload, assignment.payload);
                for (const auto& e : payload.entries)
                    kvcflip::invalidate_addr(e.buffer_address, e.size,
                                             e.key_hash);
                result = ErrorCode::OK;
                break;
            }
            default:
                LOG(ERROR) << "action=task_execution_failed"
                           << ", task_id=" << assignment.id
                           << ", error=unknown_task_type"
                           << ", task_type=" << assignment.type;
                result = ErrorCode::INVALID_PARAMS;
                break;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "action=task_execution_failed"
                   << ", task_id=" << assignment.id << ", error=exception"
                   << ", exception=" << e.what();
        result = ErrorCode::INTERNAL_ERROR;
    }

    if (result == ErrorCode::OK) {
        TaskCompleteRequest complete_request;
        complete_request.id = assignment.id;
        complete_request.status = TaskStatus::SUCCESS;
        complete_request.message = "Task completed successfully";
        auto complete_result =
            master_client_.MarkTaskToComplete(complete_request);
        if (!complete_result.has_value()) {
            LOG(WARNING) << "action=task_complete_failed"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << complete_result.error();
        }
    } else {
        uint32_t current_retry_count = client_task.retry_count;
        // Only retry on allocation failures (NO_AVAILABLE_HANDLE)
        // Other errors (e.g., OBJECT_NOT_FOUND, REPLICA_NOT_FOUND) should
        // not be retried
        bool should_retry =
            (result == ErrorCode::NO_AVAILABLE_HANDLE) &&
            (current_retry_count < assignment.max_retry_attempts);

        if (should_retry) {
            ClientTask retry_task = client_task;
            retry_task.increment_retry();

            const auto retry_delay =
                std::chrono::milliseconds(50 * (current_retry_count + 1));
            std::this_thread::sleep_for(retry_delay);

            LOG(WARNING) << "action=task_execution_failed_retry"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << result
                         << ", retry_count=" << current_retry_count
                         << ", max_retry_count="
                         << assignment.max_retry_attempts << ", will_retry=true"
                         << ", retry_delay=" << retry_delay.count() << "ms";

            task_thread_pool_.enqueue(
                [this, retry_task]() { ExecuteTask(retry_task); });
        } else {
            LOG(ERROR) << "action=task_execution_failed"
                       << ", task_id=" << assignment.id
                       << ", error_code=" << result
                       << ", retry_count=" << current_retry_count
                       << ", max_retry_count=" << assignment.max_retry_attempts;
            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::FAILED;
            complete_request.message =
                toString(result) + " (max retries reached: " +
                std::to_string(assignment.max_retry_attempts) + ")";
            auto complete_result =
                master_client_.MarkTaskToComplete(complete_request);
            if (!complete_result.has_value()) {
                LOG(WARNING) << "action=task_complete_failed"
                             << ", task_id=" << assignment.id
                             << ", error_code=" << complete_result.error();
            }
        }
    }
}

void Client::StorageHeartbeatThreadMain() {
    // How many failed pings before reconnecting via the HA coordinator
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    auto remount_segment = [this]() {
        // This lock must be held until the remount rpc is finished,
        // otherwise there will be corner cases, e.g., a segment is
        // unmounted successfully first, and then remounted again in
        // this thread.
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::vector<Segment> segments;
        for (auto it : mounted_segments_) {
            auto& segment = it.second;
            segments.emplace_back(segment);
        }
        auto remount_result = master_client_.ReMountSegment(segments);
        if (!remount_result) {
            ErrorCode err = remount_result.error();
            LOG(ERROR) << "Failed to remount segments: " << err;
        }
        // Re-publish Transfer Engine segment descriptors to the HTTP
        // metadata server.  When Master (which hosts the HTTP metadata
        // server in the same process) is killed and restarted, all
        // in-memory KV entries are lost.  ReMountSegment above only
        // restores Master-side allocation state; it does NOT write back
        // the transport-level segment descriptors.  Without this, remote
        // peers get HTTP 404 when querying our segment descriptor and
        // data transfers fail.
        auto metadata = transfer_engine_->getMetadata();
        if (metadata) {
            int rc = metadata->updateLocalSegmentDesc();
            if (rc != 0) {
                LOG(ERROR) << "Failed to re-publish segment descriptor "
                           << "to metadata server, rc=" << rc
                           << ", will retry in next heartbeat cycle";
                segment_desc_publish_pending_.store(true);
            } else {
                segment_desc_publish_pending_.store(false);
            }
            // Also re-publish RPC meta entry (mooncake/rpc_meta/<hostname>).
            // Remote peers need this to locate our RDMA RPC port for
            // handshake.  Like segment descriptors, this entry is lost
            // when the HTTP metadata server is cleared on Master restart.
            rc = metadata->rePublishRpcMetaEntry(local_hostname_);
            if (rc != 0) {
                LOG(ERROR) << "Failed to re-publish RPC meta entry "
                           << "to metadata server, rc=" << rc
                           << ", will retry in next heartbeat cycle";
                rpc_meta_publish_pending_.store(true);
            } else {
                rpc_meta_publish_pending_.store(false);
            }
        }
        // Note: LOCAL_DISK segment remount is NOT done here.
        // It is handled by FileStorage::Heartbeat() when it detects
        // SEGMENT_NOT_FOUND, which also triggers ScanMeta to
        // re-register offloaded object metadata.
    };
    // Use another thread to remount segments to avoid blocking the ping
    // thread
    std::future<void> remount_segment_future;

    while (storage_heartbeat_running_.load()) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping();
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            last_ping_success_.store(true);
            auto& ping_response = ping_result.value();
            if (ping_response.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            } else if (segment_desc_publish_pending_.load() &&
                       !remount_segment_future.valid()) {
                // Previous remount succeeded but updateLocalSegmentDesc()
                // failed (e.g. transient HTTP error).  Retry it directly
                // without re-running ReMountSegment.
                auto metadata = transfer_engine_->getMetadata();
                if (metadata) {
                    int rc = metadata->updateLocalSegmentDesc();
                    if (rc != 0) {
                        LOG(ERROR)
                            << "Retry: failed to re-publish segment "
                            << "descriptor to metadata server, rc=" << rc;
                    } else {
                        LOG(INFO) << "Retry: successfully re-published "
                                  << "segment descriptor to metadata server";
                        segment_desc_publish_pending_.store(false);
                    }
                }
            } else if (rpc_meta_publish_pending_.load() &&
                       !remount_segment_future.valid()) {
                // Previous remount succeeded but rePublishRpcMetaEntry()
                // failed.  Retry it directly.
                auto metadata = transfer_engine_->getMetadata();
                if (metadata) {
                    int rc = metadata->rePublishRpcMetaEntry(local_hostname_);
                    if (rc != 0) {
                        LOG(ERROR)
                            << "Retry: failed to re-publish RPC "
                            << "meta entry to metadata server, rc=" << rc;
                    } else {
                        LOG(INFO) << "Retry: successfully re-published "
                                  << "RPC meta entry to metadata server";
                        rpc_meta_publish_pending_.store(false);
                    }
                }
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        last_ping_success_.store(false);
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Failed to ping master";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Exceeded ping failure threshold. Reconnect based on mode.
        if (leader_coordinator_) {
            LOG(ERROR)
                << "Failed to ping master for " << ping_fail_count
                << " times; fetching latest master view and reconnecting";
            auto current_view = leader_coordinator_->ReadCurrentView();
            if (!current_view) {
                LOG(ERROR) << "Failed to get new master view: "
                           << toString(current_view.error());
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            if (!current_view.value().has_value()) {
                LOG(WARNING) << "No active master view is published yet";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            const auto& next_view = current_view.value().value();
            auto err = SwitchLeader(next_view);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to master "
                           << next_view.leader_address << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            LOG(INFO) << "Reconnected to master " << next_view.leader_address;
            ping_fail_count = 0;
        } else {
            const std::string current_master_address = direct_master_address_;
            LOG(ERROR) << "Failed to ping master for " << ping_fail_count
                       << " times (non-HA); reconnecting to "
                       << current_master_address;
            auto err = master_client_.Connect(current_master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Reconnect failed to " << current_master_address
                           << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            LOG(INFO) << "Reconnected to master " << current_master_address;
            last_ping_success_.store(true);
            ping_fail_count = 0;
        }
    }
    // Explicitly wait for the remount segment thread to finish
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) {
    // Find the first complete replica
    for (size_t i = 0; i < replica_list.size(); ++i) {
        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
            replica = replica_list[i];
            return ErrorCode::OK;
        }
    }

    // No complete replica found
    return ErrorCode::INVALID_REPLICA;
}

tl::expected<Replica::Descriptor, ErrorCode> Client::GetPreferredReplica(
    const std::vector<Replica::Descriptor>& replica_list) {
    if (replica_list.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted_segments_.empty() || replica_list.size() == 1) {
        return replica_list[0];
    }

    std::unordered_set<std::string> local_endpoints;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        for (const auto& [segment_id, segment] : mounted_segments_) {
            local_endpoints.insert(segment.te_endpoint);
        }
    }

    // Prefer local MEMORY replicas first
    for (const auto& rep : replica_list) {
        if (rep.is_memory_replica()) {
            const auto& mem_desc = rep.get_memory_descriptor();
            const std::string& endpoint =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    // Then prefer local NOF_SSD replicas
    for (const auto& rep : replica_list) {
        if (rep.is_nof_replica()) {
            const auto& nof_desc = rep.get_nof_descriptor();
            const std::string& endpoint =
                nof_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    return replica_list[0];
}

size_t Client::GetLocalHotCacheSizeFromEnv() {
    if (const char* ev_size = std::getenv("MC_STORE_LOCAL_HOT_CACHE_SIZE")) {
        std::string ev_size_str(ev_size);
        std::string error_msg = "Invalid MC_STORE_LOCAL_HOT_CACHE_SIZE='" +
                                ev_size_str + "', disable local hot cache";
        // Check for negative values
        if (!ev_size_str.empty() && ev_size_str[0] == '-') {
            LOG(WARNING) << error_msg;
            return 0;
        }
        try {
            unsigned long long v = std::stoull(ev_size_str, nullptr, 10);
            if (v > 0) {
                return static_cast<size_t>(v);
            } else {
                LOG(WARNING) << error_msg;
                return 0;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_msg;
            return 0;
        }
    }
    return 0;
}

size_t Client::GetLocalHotBlockSizeFromEnv(size_t default_value) {
    if (const char* ev_block_size =
            std::getenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE")) {
        std::string ev_block_size_str(ev_block_size);
        std::string error_msg = "Invalid MC_STORE_LOCAL_HOT_BLOCK_SIZE='" +
                                ev_block_size_str +
                                "', using default block size";
        // Check for negative values
        if (!ev_block_size_str.empty() && ev_block_size_str[0] == '-') {
            LOG(WARNING) << error_msg;
            return default_value;
        }
        try {
            unsigned long long v = std::stoull(ev_block_size_str, nullptr, 10);
            if (v > 0) {
                return static_cast<size_t>(v);
            } else {
                LOG(WARNING) << error_msg;
                return default_value;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_msg;
            return default_value;
        }
    }
    return default_value;
}

ErrorCode Client::InitLocalHotCache() {
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
    admission_sketch_.reset();

    if (object_checksum_enabled_) {
        return ErrorCode::OK;
    }

    // Defaults: hot cache is disabled unless MC_STORE_LOCAL_HOT_CACHE_SIZE is
    // set to a positive value; when enabled, default block size is 16MB and
    // thread_num is 2.
    size_t block_size = 16 * 1024 * 1024;  // 16MB default block size
    size_t thread_num = 2;

    // Read MC_STORE_LOCAL_HOT_CACHE_SIZE from environment
    size_t total_cache = GetLocalHotCacheSizeFromEnv();
    if (total_cache == 0) {
        // Environment variable not set or invalid, disable cache
        return ErrorCode::OK;
    }

    // Read MC_STORE_LOCAL_HOT_BLOCK_SIZE from environment
    block_size = GetLocalHotBlockSizeFromEnv(block_size);

    // MC_STORE_LOCAL_HOT_CACHE_USE_SHM: "1" enables memfd-backed shm (default
    // off). When enabled, hot cache is shareable with dummy clients via IPC.
    bool use_shm = false;
    if (const char* ev = std::getenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM")) {
        use_shm = (std::string(ev) == "1");
    }

    // Enable hot cache
    {
        hot_cache_ =
            std::make_shared<LocalHotCache>(total_cache, block_size, use_shm);
        // Check if cache initialization was successful
        if (hot_cache_->GetCacheSize() == 0) {
            LOG(ERROR)
                << "Local hot cache creation failed: no blocks allocated. "
                << "total_cache=" << total_cache;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            return ErrorCode::INVALID_PARAMS;
        }

        int rc = transfer_engine_->registerLocalMemory(
            hot_cache_->GetBaseAddress(), hot_cache_->GetTotalSize(),
            kWildcardLocation, true, true);
        if (rc != 0) {
            LOG(ERROR)
                << "Failed to register local hot cache memory with transfer "
                   "engine, base="
                << hot_cache_->GetBaseAddress()
                << ", size=" << hot_cache_->GetTotalSize() << ", ret=" << rc;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            hot_cache_memory_registered_ = false;
            return ErrorCode::INVALID_PARAMS;
        }
        hot_cache_memory_registered_ = true;

        LOG(INFO) << "Local hot cache enabled with cache size=" << total_cache
                  << ", block size=" << block_size
                  << ", block amount=" << hot_cache_->GetCacheSize()
                  << ", shm=" << (use_shm ? "on" : "off")
                  << ", transfer engine registered=on";
        // Create async handler with 2 worker threads
        hot_cache_handler_ =
            std::make_unique<LocalHotCacheHandler>(hot_cache_, thread_num);
        admission_sketch_ = std::make_unique<CountMinSketch>();

        // MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD: minimum CMS count before a
        // key is admitted to hot cache (default 2).
        if (const char* ev =
                std::getenv("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD")) {
            std::string ev_str(ev);
            std::string error_msg =
                "Invalid MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD='" + ev_str +
                "', using default";
            try {
                unsigned long long v = std::stoull(ev_str, nullptr, 10);
                if (v > 0 && v <= 255) {
                    admission_threshold_ = static_cast<uint8_t>(v);
                } else {
                    LOG(WARNING) << error_msg;
                }
            } catch (const std::exception&) {
                LOG(WARNING) << error_msg;
            }
        }
    }
    return ErrorCode::OK;
}

void Client::UnregisterLocalHotCacheMemory() {
    if (!(hot_cache_ && hot_cache_memory_registered_)) {
        return;
    }
    if (!transfer_engine_) {
        hot_cache_memory_registered_ = false;
        return;
    }

    int rc = transfer_engine_->unregisterLocalMemory(
        hot_cache_->GetBaseAddress(), true);
    if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
        LOG(ERROR)
            << "Failed to unregister local hot cache memory from transfer "
               "engine, base="
            << hot_cache_->GetBaseAddress()
            << ", size=" << hot_cache_->GetTotalSize() << ", ret=" << rc;
    }
    hot_cache_memory_registered_ = false;
}

void Client::ProcessSlicesAsync(const std::string& key,
                                const std::vector<Slice>& slices,
                                const Replica::Descriptor& replica) {
    if (!(hot_cache_ && replica.is_memory_replica())) {
        return;
    }

    // Skip local data unless hot cache is in shm mode (shared with dummy
    // clients who need local data cached for zero-copy access).
    if (!hot_cache_->IsShm() && IsReplicaOnLocalMemory(replica)) {
        return;
    }

    // Identify TE transfer slices (non-local) and submit async put tasks
    for (size_t i = 0; i < slices.size(); ++i) {
        if (!hot_cache_handler_->SubmitPutTask(key, slices[i])) {
            LOG(ERROR) << "Failed to submit hot cache put task for key=" << key
                       << " slice_idx=" << i;
            return;
        }
    }
}

bool Client::IsReplicaOnLocalMemory(const Replica::Descriptor& replica) {
    if (!replica.is_memory_replica()) {
        return false;
    }
    const auto replica_transfer_endpoint =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    if (metadata_connstring_ == P2PHANDSHAKE) {
        return replica_transfer_endpoint == GetTransportEndpoint();
    }
    return local_hostname_ == replica_transfer_endpoint;
}

}  // namespace mooncake
