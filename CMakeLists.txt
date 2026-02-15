cmake_minimum_required(VERSION 3.20)
project(mdd_sim VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MDD_SIM_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(MDD_SIM_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

if(MDD_SIM_ENABLE_ASAN)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
endif()

if(MDD_SIM_ENABLE_UBSAN)
  add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=undefined)
endif()

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

set(PROTO_FILE ${PROTO_SRC_DIR}/mdd.proto)
set(PROTO_CC ${PROTO_OUT_DIR}/mdd.pb.cc)
set(PROTO_H ${PROTO_OUT_DIR}/mdd.pb.h)
set(GRPC_CC ${PROTO_OUT_DIR}/mdd.grpc.pb.cc)
set(GRPC_H ${PROTO_OUT_DIR}/mdd.grpc.pb.h)

add_custom_command(
  OUTPUT ${PROTO_CC} ${PROTO_H} ${GRPC_CC} ${GRPC_H}
  COMMAND $<TARGET_FILE:protobuf::protoc>
  ARGS
    --proto_path=${PROTO_SRC_DIR}
    --cpp_out=${PROTO_OUT_DIR}
    --grpc_out=${PROTO_OUT_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  VERBATIM
)

add_library(mdd_proto STATIC ${PROTO_CC} ${GRPC_CC})
target_include_directories(mdd_proto PUBLIC ${PROTO_OUT_DIR} ${PROTO_SRC_DIR})
target_link_libraries(mdd_proto PUBLIC protobuf::libprotobuf gRPC::grpc++ gRPC::grpc Threads::Threads)

add_library(mdd_common STATIC
  common/logging.cpp
  common/metrics.cpp
  common/config_loader.cpp
  common/recording.cpp
)
target_include_directories(mdd_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_OUT_DIR})
target_link_libraries(mdd_common PUBLIC mdd_proto)

add_library(mdd_server_lib STATIC
  server/instrument_config.cpp
  server/order_book.cpp
  server/simulator.cpp
  server/subscription_manager.cpp
  server/publisher.cpp
  server/market_data_service.cpp
)
target_include_directories(mdd_server_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_OUT_DIR})
target_link_libraries(mdd_server_lib PUBLIC mdd_common)

add_library(mdd_client_lib STATIC
  client/order_book_view.cpp
  client/apply_engine.cpp
  client/client_session.cpp
)
target_include_directories(mdd_client_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_OUT_DIR})
target_link_libraries(mdd_client_lib PUBLIC mdd_common)

add_executable(mdd_server server/main.cpp)
target_link_libraries(mdd_server PRIVATE mdd_server_lib)

add_executable(mdd_client client/main.cpp)
target_link_libraries(mdd_client PRIVATE mdd_client_lib)

add_executable(mdd_loadtest tools/loadtest.cpp)
target_link_libraries(mdd_loadtest PRIVATE mdd_client_lib mdd_server_lib)

add_executable(mdd_replay tools/replay.cpp)
target_link_libraries(mdd_replay PRIVATE mdd_server_lib)

include(CTest)
if(BUILD_TESTING)
  add_executable(test_order_book tests/test_order_book.cpp)
  target_link_libraries(test_order_book PRIVATE mdd_server_lib)
  add_test(NAME test_order_book COMMAND test_order_book)

  add_executable(test_client_apply tests/test_client_apply.cpp)
  target_link_libraries(test_client_apply PRIVATE mdd_client_lib)
  add_test(NAME test_client_apply COMMAND test_client_apply)

  add_executable(test_integration tests/test_integration.cpp)
  target_link_libraries(test_integration PRIVATE mdd_server_lib mdd_client_lib)
  add_test(NAME test_integration COMMAND test_integration)

  add_executable(test_replay_determinism tests/test_replay_determinism.cpp)
  target_link_libraries(test_replay_determinism PRIVATE mdd_server_lib mdd_common)
  add_test(NAME test_replay_determinism COMMAND test_replay_determinism)
endif()
