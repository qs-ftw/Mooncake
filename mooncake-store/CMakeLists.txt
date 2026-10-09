project(MooncakeStore VERSION 2.0.0)

option(ENABLE_KV_EVENTS
       "Build master KV events ZMQ publisher (requires libzmq when ON)" OFF)

# Extract version components for C++ usage
set(MOONCAKE_STORE_VERSION ${PROJECT_VERSION})

# Read display version from mooncake-wheel/pyproject.toml
set(PYPROJECT_TOML_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/pyproject.toml")
if(EXISTS "${PYPROJECT_TOML_PATH}")
    file(READ "${PYPROJECT_TOML_PATH}" PYPROJECT_CONTENT)
    string(REGEX MATCH "version = \"([^\"]+)\"" _ "${PYPROJECT_CONTENT}")
    set(MOONCAKE_DISPLAY_VERSION_BASE "${CMAKE_MATCH_1}")
endif()
if(NOT MOONCAKE_DISPLAY_VERSION_BASE)
    message(WARNING "Could not read display version from ${PYPROJECT_TOML_PATH}; falling back to MOONCAKE_STORE_VERSION")
    set(MOONCAKE_DISPLAY_VERSION_BASE "${MOONCAKE_STORE_VERSION}")
endif()

# Get short git commit hash for display
find_package(Git QUIET)
if(GIT_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_SHORT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
endif()
if(NOT GIT_SHORT_HASH)
    set(GIT_SHORT_HASH "unknown")
endif()
set(MOONCAKE_DISPLAY_VERSION "${MOONCAKE_DISPLAY_VERSION_BASE} (git: ${GIT_SHORT_HASH})")

configure_file(include/version.h.in include/version.h @ONLY)

if (STORE_USE_ETCD)
    set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)
endif()

if (STORE_USE_K8S_LEASE)
    set(K8S_LEASE_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/k8s-lease/)
    set(K8S_LEASE_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/k8s-lease/libk8s_lease_wrapper.so)
endif()

if (STORE_USE_REDIS)
    find_path(MOONCAKE_STORE_HIREDIS_INCLUDE_DIR hiredis/hiredis.h REQUIRED)
    find_library(MOONCAKE_STORE_HIREDIS_LIBRARY hiredis REQUIRED)
    message(STATUS "Redis HA backend: Enabled")
endif()

if (NOT STORE_USE_ETCD AND NOT STORE_USE_REDIS AND NOT STORE_USE_K8S_LEASE)
    message(STATUS "Store HA backends are disabled")
endif()

# KVC SDK (hpc-redis static-UB KV-cache medium): the adapter now includes
# <kvc/kvc_server.h> and links libkvc_server.so directly at build time
# (no dlopen/dlsym). Resolve the SDK tree once; override with -DKVC_SDK_ROOT=
# or the KVC_SDK_ROOT environment variable. Default assumes the Mooncake
# checkout sits inside the hpc-redis tree (Mooncake/ is a sibling of kvc-sdk/).
if(NOT DEFINED KVC_SDK_ROOT)
    if(DEFINED ENV{KVC_SDK_ROOT})
        set(KVC_SDK_ROOT "$ENV{KVC_SDK_ROOT}")
    else()
        set(KVC_SDK_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../kvc-sdk")
    endif()
endif()
get_filename_component(KVC_SDK_ROOT "${KVC_SDK_ROOT}" ABSOLUTE)
if(NOT EXISTS "${KVC_SDK_ROOT}/include/kvc/kvc_server.h")
    message(FATAL_ERROR
            "KVC SDK headers not found under KVC_SDK_ROOT=${KVC_SDK_ROOT}. "
            "The KVC adapter compiles against them directly; point "
            "KVC_SDK_ROOT at the hpc-redis kvc-sdk tree "
            "(default: <checkout>/../../hpc-redis/kvc-sdk).")
endif()
if(NOT EXISTS "${KVC_SDK_ROOT}/build/libkvc_server.so")
    message(FATAL_ERROR
            "libkvc_server.so not found under KVC_SDK_ROOT=${KVC_SDK_ROOT}/"
            "build — run 'make all' inside the kvc-sdk tree first.")
endif()
message(STATUS "KVC SDK (static-UB medium, direct link): ${KVC_SDK_ROOT}")

# Add include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/mooncake-store/proto/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/
    ${CMAKE_CURRENT_BINARY_DIR}/include/
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
    ${KVC_SDK_ROOT}/include
    ${ETCD_WRAPPER_INCLUDE}
)

# Add subdirectories
add_subdirectory(src)

if (STORE_USE_ETCD)
    add_subdirectory(tools)
endif()

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
endif()

if (BUILD_BENCHMARK)
    add_subdirectory(benchmarks)
endif()
