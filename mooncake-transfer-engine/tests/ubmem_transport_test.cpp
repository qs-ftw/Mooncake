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

// UbmemTransport 测试：UB 共享内存设备（obmm shmdev）传输层。
//
// 单机模式（默认 gtest）：本机设备窗上 WRITE→READ 回读字节校验。
//   设备用 --ubmem_device 指定（如 /dev/obmm_shmdev13，务必选空闲窗口，
//   跑前 fuser 检查）；窗口大小 --ubmem_size（默认 64MiB）。
//   元数据用 --metadata_server（默认 http://127.0.0.1:8080，先起
//   python3 -m mooncake.http_metadata_server --port 8080）。
//
// 跨节点模式（gtest 之外的 main 参数）：
//   目标侧:  --mode=target  --local_server_name=hw01 \
//            --ubmem_device=/dev/obmm_shmdev13
//   发起侧:  --mode=initiator --local_server_name=hw02 \
//            --ubmem_device=/dev/obmm_shmdev14 \
//            --target_server=hw01 --target_device=/dev/obmm_shmdev13 \
//            --peer_dev_map=hw01:/dev/obmm_shmdev13=/dev/obmm_shmdev5
//   发起侧经镜像设备对目标段 WRITE→READ 回读校验（offset 空间一致）。

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "transport/ubmem_transport/ubmem_transport.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "http://127.0.0.1:8080",
              "Metadata server (http metadata server or etcd)");
DEFINE_string(mode, "gtest",
              "gtest | initiator | target (cross-node manual mode)");
DEFINE_string(ubmem_device, "/dev/obmm_shmdev13",
              "Local UB shmdev path for this process");
DEFINE_uint64(ubmem_size, 64ull << 20, "Device window bytes to map");
DEFINE_string(target_server, "", "[initiator] peer server name");
DEFINE_string(target_device, "", "[initiator] peer device path");
DEFINE_string(peer_dev_map, "",
              "[initiator] MC_UBMEM_PEER_DEV_MAP content");
DEFINE_int64(verify_seconds, 60, "[target] wait before verify+exit");

static void *allocateMemoryPool(size_t size) { return malloc(size); }

class UbmemTransportTest : public ::testing::Test {
   public:
    std::shared_ptr<mooncake::TransferMetadata> metadata_client;
    uint8_t *addr = nullptr;
    uint8_t *base_addr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t offset_1 = 2 * 1024 * 1024;
    const size_t len = 2 * 1024 * 1024;
    Transport *xport;
    void **args;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    const size_t kDataLength = 4 * 1024;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("UbmemTransportTest");
        FLAGS_logtostderr = 1;

        setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
        setenv("MC_UBMEM_DEV_SIZE", std::to_string(FLAGS_ubmem_size).c_str(),
               1);

        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);
        xport = nullptr;

        args = (void **)malloc(2 * sizeof(void *));
        args[0] = nullptr;
        xport = engine->installTransport("ubmem", args);
        ASSERT_NE(xport, nullptr);

        UbmemTransport *ubmem_xport = dynamic_cast<UbmemTransport *>(xport);
        ASSERT_NE(ubmem_xport, nullptr);
        base_addr = (uint8_t *)ubmem_xport->getUbmemBaseAddr();
        ASSERT_NE(base_addr, nullptr);
        addr = (uint8_t *)allocateMemoryPool(kDataLength);

        int rc = engine->registerLocalMemory(base_addr + offset_1, len);
        ASSERT_EQ(rc, 0);

        segment_id = engine->openSegment(FLAGS_local_server_name.c_str());
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
        ASSERT_NE(segment_desc, nullptr);
        ASSERT_EQ(segment_desc->protocol, "ubmem");
        ASSERT_EQ(segment_desc->ubmem_name, FLAGS_ubmem_device);
    }

    void TearDown() override {
        free(args);
        free(addr);
        google::ShutdownGoogleLogging();
    }
};

TEST_F(UbmemTransportTest, WriteReadBack) {
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;

        auto batch_id = xport->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = offset_1;
        Status s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED) {
                ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
                completed = true;
            }
        }
        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());

        // READ 回读到新缓冲，字节校验
        void *verify = allocateMemoryPool(kDataLength);
        memset(verify, 0, kDataLength);
        batch_id = xport->allocateBatchID(1);
        TransferRequest read_entry;
        read_entry.opcode = TransferRequest::READ;
        read_entry.length = kDataLength;
        read_entry.source = (uint8_t *)verify;
        read_entry.target_id = segment_id;
        read_entry.target_offset = offset_1;
        s = engine->submitTransfer(batch_id, {read_entry});
        ASSERT_EQ(s, Status::OK());

        completed = false;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED) {
                ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
                completed = true;
            }
        }
        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
        ASSERT_EQ(memcmp(verify, addr, kDataLength), 0);
        free(verify);
    }
    engine->unregisterLocalMemory(base_addr + offset_1);
}

TEST_F(UbmemTransportTest, OutOfBoundsRejected) {
    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t *)(addr);
    entry.target_id = segment_id;
    entry.target_offset = FLAGS_ubmem_size - 1024;  // 越界（部分超出窗口）
    Status s = engine->submitTransfer(batch_id, {entry});
    LOG_ASSERT(s.ok());

    TransferStatus status;
    bool completed = false;
    while (!completed) {
        Status s = xport->getTransferStatus(batch_id, 0, status);
        ASSERT_EQ(s, Status::OK());
        if (status.s == TransferStatusEnum::COMPLETED ||
            status.s == TransferStatusEnum::FAILED)
            completed = true;
    }
    ASSERT_EQ(status.s, TransferStatusEnum::FAILED);  // 越界必须判失败
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

// ---- 跨节点手工模式（非 gtest）----

static int runTargetMode() {
    setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
    setenv("MC_UBMEM_DEV_SIZE", std::to_string(FLAGS_ubmem_size).c_str(), 1);
    auto engine = std::make_unique<TransferEngine>(false);
    auto hp = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hp.first.c_str(), hp.second);
    void *args[1] = {nullptr};
    Transport *xport = engine->installTransport("ubmem", args);
    if (!xport) {
        LOG(ERROR) << "target: installTransport failed";
        return 1;
    }
    UbmemTransport *ub = dynamic_cast<UbmemTransport *>(xport);
    uint8_t *base = (uint8_t *)ub->getUbmemBaseAddr();
    engine->registerLocalMemory(base + (2 << 20), 2 << 20);
    LOG(INFO) << "target ready: device=" << FLAGS_ubmem_device
              << " base=" << (void *)base << ", waiting "
              << FLAGS_verify_seconds << "s";
    sleep(FLAGS_verify_seconds);

    // 目标侧自查：发起侧写的 pattern 首字节为 'M'，后续 offset 相关
    const size_t len = 2 << 20, off = 2 << 20;
    uint8_t *window = base + off;
    bool ok = window[0] == 'M';
    for (size_t i = 1; ok && i < len; i += 4096)
        ok = (window[i] == (uint8_t)(i ^ (i >> 8)));
    LOG(INFO) << (ok ? "TARGET VERIFY PASS" : "TARGET VERIFY FAIL");
    return ok ? 0 : 2;
}

static int runInitiatorMode() {
    setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
    setenv("MC_UBMEM_DEV_SIZE", std::to_string(FLAGS_ubmem_size).c_str(), 1);
    if (!FLAGS_peer_dev_map.empty())
        setenv("MC_UBMEM_PEER_DEV_MAP", FLAGS_peer_dev_map.c_str(), 1);
    auto engine = std::make_unique<TransferEngine>(false);
    auto hp = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hp.first.c_str(), hp.second);
    void *args[1] = {nullptr};
    Transport *xport = engine->installTransport("ubmem", args);
    if (!xport) {
        LOG(ERROR) << "initiator: installTransport failed";
        return 1;
    }
    // SegmentID 无官方 INVALID 哨兵；0 视为打开失败
    auto segment_id = engine->openSegment(FLAGS_target_server.c_str());
    if (segment_id == 0) {
        LOG(ERROR) << "initiator: cannot open target segment "
                   << FLAGS_target_server;
        return 1;
    }

    const size_t len = 2 << 20, target_offset = 2 << 20;
    uint8_t *src = (uint8_t *)malloc(len);
    src[0] = 'M';
    for (size_t i = 1; i < len; ++i)
        src[i] = (uint8_t)(i ^ (i >> 8));

    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = len;
    entry.source = src;
    entry.target_id = segment_id;
    entry.target_offset = target_offset;
    Status s = engine->submitTransfer(batch_id, {entry});
    if (!s.ok()) {
        LOG(ERROR) << "initiator: submit WRITE failed: " << s;
        return 1;
    }
    TransferStatus status;
    while (xport->getTransferStatus(batch_id, 0, status).ok() &&
           status.s == TransferStatusEnum::WAITING)
        usleep(1000);
    LOG_IF(ERROR, status.s != TransferStatusEnum::COMPLETED)
        << "initiator: cross-node WRITE failed";
    xport->freeBatchID(batch_id);
    if (status.s != TransferStatusEnum::COMPLETED)
        return 1;

    // 经镜像 READ 回读校验
    uint8_t *verify = (uint8_t *)malloc(len);
    batch_id = xport->allocateBatchID(1);
    TransferRequest read_entry;
    read_entry.opcode = TransferRequest::READ;
    read_entry.length = len;
    read_entry.source = verify;
    read_entry.target_id = segment_id;
    read_entry.target_offset = target_offset;
    s = engine->submitTransfer(batch_id, {read_entry});
    if (!s.ok()) {
        LOG(ERROR) << "initiator: submit READ failed: " << s;
        return 1;
    }
    while (xport->getTransferStatus(batch_id, 0, status).ok() &&
           status.s == TransferStatusEnum::WAITING)
        usleep(1000);
    xport->freeBatchID(batch_id);
    if (status.s != TransferStatusEnum::COMPLETED) {
        LOG(ERROR) << "initiator: cross-node READ failed";
        return 1;
    }
    bool ok = memcmp(verify, src, len) == 0;
    LOG(INFO) << (ok ? "INITIATOR VERIFY PASS (write+read-back via mirror)"
                     : "INITIATOR VERIFY FAIL");
    free(src);
    free(verify);
    return ok ? 0 : 2;
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    // gtest 路径的日志初始化在 SetUp/TearDown（对齐 cxl 测试）；
    // 手工模式在各自入口初始化，避免双重 InitGoogleLogging
    if (mooncake::FLAGS_mode == "target") {
        google::InitGoogleLogging("ubmem_transport_test");
        FLAGS_logtostderr = 1;
        return mooncake::runTargetMode();
    }
    if (mooncake::FLAGS_mode == "initiator") {
        google::InitGoogleLogging("ubmem_transport_test");
        FLAGS_logtostderr = 1;
        return mooncake::runInitiatorMode();
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
