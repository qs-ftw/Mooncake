// store_kv_bench_native.cpp — 非 GIL C++ driver，复刻 store_kv_bench.py 的
// zcopy 批量直读路径（同 key 造法/同 env/同 batch 语义），用于钉 UB 直读的
// 真实上限（python driver 的每进程 GIL 天花板 ~570-700 MiB/s）。
//
// 与 python 版的对齐点：
//   - key = prefix 截断/补 '_' 到 (key_size-16) 位 + "%016d"（make_key 同款）
//   - payload = (object_id & 0xFF) 填充字节（PayloadFactory 无 --pattern 同款）
//   - 读 = batch_get_into(keys, ptrs, sizes)，length<0 或 != value_size 记失败
//   - 写 = batch_put_from(keys, ptrs, sizes, ReplicateConfig{replica_num})
//   - KVC_DIRECT_READ/KVC_POS_CACHE/KVC_UBME_OWNER_HOSTS 等 env 由 real_client
//     读取，本二进制无需任何特殊处理
// 与 python 版的差异（目的即在此）：
//   - N 个原生 std::thread 共享一个 RealClient（pybind 层本来就会释放 GIL，
//     并发 batch_get_into 是设计内用法），无 GIL、无 ctypes 转换
//   - key 字符串仅构造一次（读路径不重复格式化）；校验用 256 个预生成模板

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "pyclient.h"
#include "real_client.h"

static constexpr uint64_t KiB = 1024;
static constexpr uint64_t MiB = 1024 * KiB;

DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "HTTP metadata server");
DEFINE_string(local_hostname, "127.0.0.1:19500",
              "Unique local hostname:port for this instance");
DEFINE_string(protocol, "tcp", "Transfer protocol");
DEFINE_uint64(global_segment_size, 0, "Client segment size (0 = none)");
DEFINE_uint64(local_buffer_size, 32 * MiB, "Local zcopy buffer pool size");

DEFINE_string(key_prefix, "n1", "Key prefix");
DEFINE_uint64(key_size, 20, "Total key size (suffix is 16)");
DEFINE_uint64(num_objects, 100000, "Objects per prefix");
DEFINE_uint64(value_size, 4096, "Value size");
DEFINE_uint64(batch_size, 64, "Keys per batch");
DEFINE_uint64(replica_num, 1, "ReplicateConfig.replica_num");

DEFINE_bool(skip_fill, false, "Skip fill (reuse existing data, same key scheme)");
DEFINE_uint64(fill_threads, 1, "Fill writer threads (keep 1: serial-writer rule)");
DEFINE_uint64(read_threads, 8, "Concurrent reader threads (no GIL)");
DEFINE_uint64(runtime, 30, "Read phase seconds");
DEFINE_bool(verify, true, "Verify payload bytes on read");
DEFINE_uint64(seed, 1, "Per-thread RNG seed base");

namespace {

std::string make_key(const std::string &prefix, size_t key_size,
                     uint64_t object_id) {
    char suffix[17];
    snprintf(suffix, sizeof(suffix), "%016" PRIu64, object_id);
    const size_t suffix_len = 16;
    const size_t prefix_space = key_size - suffix_len;
    std::string prefix_part = prefix.substr(0, prefix_space);
    prefix_part.resize(prefix_space, '_');
    return prefix_part + suffix;
}

struct ThreadStats {
    uint64_t requests = 0;
    uint64_t kvs = 0;
    uint64_t failed_kvs = 0;
    uint64_t verify_failures = 0;
    uint64_t bytes = 0;
    std::vector<double> latencies_ms;
};

}  // namespace

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;

    auto client = mooncake::RealClient::create();
    int rc = client->setup_real(
        FLAGS_local_hostname, FLAGS_metadata_server, FLAGS_global_segment_size,
        FLAGS_local_buffer_size, FLAGS_protocol, /*rdma_devices=*/"",
        FLAGS_master_server);
    if (rc != 0) {
        LOG(ERROR) << "setup_real failed: " << rc;
        return 1;
    }
    LOG(INFO) << "setup ok: " << FLAGS_local_hostname;

    // 预构造全部 key 与 256 个校验模板（读循环零格式化、零分配）
    const size_t n = FLAGS_num_objects;
    std::vector<std::string> keys(n);
    for (size_t i = 0; i < n; ++i)
        keys[i] = make_key(FLAGS_key_prefix, FLAGS_key_size, i);
    std::vector<std::vector<char>> patterns(256);
    for (int b = 0; b < 256; ++b)
        patterns[b].assign(FLAGS_value_size, static_cast<char>(b));

    mooncake::ReplicateConfig config;
    config.replica_num = FLAGS_replica_num;

    // ---------- fill（默认单线程串行写，写完即释放缓冲） ----------
    if (!FLAGS_skip_fill) {
        if (FLAGS_fill_threads != 1) {
            LOG(ERROR) << "fill_threads must be 1 (serial-writer rule)";
            return 1;
        }
        const size_t batch = FLAGS_batch_size;
        auto buffer = client->client_buffer_allocator_->allocate(
            batch * FLAGS_value_size);
        if (!buffer) {
            LOG(ERROR) << "fill buffer allocate failed";
            return 1;
        }
        char *base = static_cast<char *>(buffer->ptr());
        std::vector<void *> ptrs(batch);
        std::vector<size_t> sizes(batch, FLAGS_value_size);
        std::vector<std::string> batch_keys(batch);
        for (size_t i = 0; i < batch; ++i)
            ptrs[i] = base + i * FLAGS_value_size;

        auto fill_worker = [&](uint64_t tid, uint64_t nthreads) {
            for (uint64_t start = tid * batch; start < n; start += nthreads * batch) {
                const size_t len = std::min<size_t>(batch, n - start);
                for (size_t j = 0; j < len; ++j) {
                    const int fill = static_cast<int>((start + j) & 0xFF);
                    memset(ptrs[j], fill, FLAGS_value_size);
                    batch_keys[j] = keys[start + j];
                }
                auto rets = client->batch_put_from(
                    std::vector<std::string>(batch_keys.begin(),
                                             batch_keys.begin() + len),
                    std::vector<void *>(ptrs.begin(), ptrs.begin() + len),
                    std::vector<size_t>(sizes.begin(), sizes.begin() + len),
                    config);
                for (size_t j = 0; j < len; ++j)
                    if (rets[j] != 0)
                        LOG(ERROR) << "put failed key=" << batch_keys[j]
                                   << " ret=" << rets[j];
            }
        };
        auto t0 = std::chrono::steady_clock::now();
        std::vector<std::thread> fillers;
        for (uint64_t t = 0; t < FLAGS_fill_threads; ++t)
            fillers.emplace_back(fill_worker, t, FLAGS_fill_threads);
        for (auto &t : fillers) t.join();
        auto t1 = std::chrono::steady_clock::now();
        LOG(INFO) << "fill done: " << n << " objects in "
                  << std::chrono::duration<double>(t1 - t0).count() << "s ("
                  << FLAGS_fill_threads << " writer thread(s))";
    } else {
        LOG(INFO) << "fill skipped (reuse existing data)";
    }

    // ---------- read（N 原生线程并发直读） ----------
    const size_t batch = FLAGS_batch_size;
    const size_t nthreads = FLAGS_read_threads;
    std::vector<mooncake::BufferHandle> handles;
    std::vector<char *> bases;
    for (size_t t = 0; t < nthreads; ++t) {
        auto h = client->client_buffer_allocator_->allocate(
            batch * FLAGS_value_size);
        if (!h) {
            LOG(ERROR) << "read buffer allocate failed (thread " << t << ")";
            return 1;
        }
        bases.push_back(static_cast<char *>(h->ptr()));
        handles.push_back(std::move(*h));
    }

    std::vector<ThreadStats> stats(nthreads);
    std::atomic<bool> stop{false};
    std::atomic<size_t> ready{0};
    std::vector<std::thread> readers;
    readers.reserve(nthreads);
    for (size_t t = 0; t < nthreads; ++t) {
        readers.emplace_back([&, t] {
            char *base = bases[t];
            std::vector<void *> ptrs(batch);
            std::vector<size_t> sizes(batch, FLAGS_value_size);
            for (size_t j = 0; j < batch; ++j)
                ptrs[j] = base + j * FLAGS_value_size;
            std::mt19937_64 rng(FLAGS_seed + t);
            ThreadStats &st = stats[t];
            std::vector<std::string> req(batch);
            // 洗牌全排列顺序取批：批内 key 严格无重复（重复 key 会让
            // batch_get_into 的部分目标 slot 不回填，残留上一批数据，
            // 校验必败——python 版同样抽到重复 key，只是从不开校验）
            std::vector<uint32_t> perm(n);
            for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
            size_t perm_pos = n;
            ready.fetch_add(1);
            while (!stop.load(std::memory_order_relaxed)) {
                if (perm_pos + batch > n) {
                    std::shuffle(perm.begin(), perm.end(), rng);
                    perm_pos = 0;
                }
                for (size_t j = 0; j < batch; ++j)
                    req[j] = keys[perm[perm_pos + j]];
                perm_pos += batch;
                auto s0 = std::chrono::steady_clock::now();
                auto lengths = client->batch_get_into(req, ptrs, sizes);
                auto s1 = std::chrono::steady_clock::now();
                st.requests++;
                for (size_t j = 0; j < batch; ++j) {
                    if (lengths[j] < 0 ||
                        static_cast<uint64_t>(lengths[j]) != FLAGS_value_size) {
                        st.failed_kvs++;
                        continue;
                    }
                    if (FLAGS_verify) {
                        const uint64_t object_id =
                            std::stoull(req[j].substr(FLAGS_key_size - 16));
                        const char *expect = patterns[object_id & 0xFF].data();
                        if (memcmp(ptrs[j], expect, FLAGS_value_size) != 0) {
                            st.verify_failures++;
                            static std::atomic<int> dumped{0};
                            if (dumped.fetch_add(1) < 10) {
                                const char *got = static_cast<char *>(ptrs[j]);
                                size_t off = 0;
                                while (off < FLAGS_value_size &&
                                       got[off] == expect[off])
                                    off++;
                                LOG(ERROR) << "verify_fail id=" << object_id
                                           << " req#" << st.requests
                                           << " slot=" << j
                                           << " first_bad_off=" << off
                                           << " expect=0x"
                                           << (unsigned)(unsigned char)
                                                  expect[off]
                                           << " got=0x"
                                           << (unsigned)(unsigned char)got[off];
                            }
                            continue;
                        }
                    }
                    st.kvs++;
                    st.bytes += FLAGS_value_size;
                }
                st.latencies_ms.push_back(
                    std::chrono::duration<double, std::milli>(s1 - s0).count());
            }
        });
    }
    while (ready.load() < nthreads) std::this_thread::yield();
    LOG(INFO) << "=== phase read_perf_native (threads=" << nthreads
              << ") ===";
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_runtime));
    stop.store(true);
    for (auto &t : readers) t.join();

    // ---------- 汇总 ----------
    uint64_t total_kvs = 0, total_failed = 0, total_vf = 0, total_bytes = 0;
    std::vector<double> all_lat;
    for (auto &st : stats) {
        total_kvs += st.kvs;
        total_failed += st.failed_kvs;
        total_vf += st.verify_failures;
        total_bytes += st.bytes;
        all_lat.insert(all_lat.end(), st.latencies_ms.begin(),
                       st.latencies_ms.end());
    }
    std::sort(all_lat.begin(), all_lat.end());
    auto pct = [&](double p) {
        if (all_lat.empty()) return 0.0;
        return all_lat[std::min<size_t>(all_lat.size() - 1,
                                        static_cast<size_t>(p * all_lat.size()))];
    };
    const double sec = FLAGS_runtime;
    const double kvs_per_s = total_kvs / sec;
    const double mib_s = total_bytes / sec / MiB;
    LOG(INFO) << "native bench: threads=" << nthreads
              << " prefix=" << FLAGS_key_prefix
              << " batch=" << batch << " value=" << FLAGS_value_size;
    for (size_t t = 0; t < nthreads; ++t)
        LOG(INFO) << "  thread " << t << ": kv/s="
                  << (uint64_t)(stats[t].kvs / sec)
                  << " failed=" << stats[t].failed_kvs
                  << " verify_fail=" << stats[t].verify_failures;
    LOG(INFO) << "=== phase read_perf_native stats: kvs=" << total_kvs
              << " failed_kvs=" << total_failed
              << " verify_failures=" << total_vf
              << " bytes=" << total_bytes << " duration=" << sec << "s"
              << " kv/s=" << kvs_per_s << " MiB/s=" << mib_s
              << " p50_ms=" << pct(0.50) << " p99_ms=" << pct(0.99) << " ===";
    fprintf(stderr, "RESULT threads=%zu kv/s=%.0f MiB/s=%.1f failed=%" PRIu64
            " verify_fail=%" PRIu64 " p50_ms=%.3f p99_ms=%.3f\n",
            nthreads, kvs_per_s, mib_s, total_failed, total_vf, pct(0.50),
            pct(0.99));
    return 0;
}
