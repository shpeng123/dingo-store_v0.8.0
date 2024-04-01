// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)

#include <butil/logging.h>
#include <brpc/server.h>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>
#include "braft/raft.pb.h"
#include "braft/raft_service.h"
#include "braft/raft.h"
#include "braft/node.h"
#include "braft/node_manager.h"
#include "glog/logging.h"

namespace braft {

RaftServiceImpl::~RaftServiceImpl() {
    global_node_manager->remove_address(_addr);
}

void RaftServiceImpl::pre_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = 
                        global_node_manager->get(request->group_id(), peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    // TODO: should return butil::Status
    int rc = node->handle_pre_vote_request(request, response);
    if (rc != 0) {
        cntl->SetFailed(rc, "%s", berror(rc));
        return;
    }
}

void RaftServiceImpl::request_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = 
                        global_node_manager->get(request->group_id(), peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    int rc = node->handle_request_vote_request(request, response);
    if (rc != 0) {
        cntl->SetFailed(rc, "%s", berror(rc));
        return;
    }
}

void RaftServiceImpl::append_entries(google::protobuf::RpcController* cntl_base,
                            const AppendEntriesRequest* request,
                            AppendEntriesResponse* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = 
                        global_node_manager->get(request->group_id(), peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    return node->handle_append_entries_request(cntl, request, response, 
                                               done_guard.release());
}

void RaftServiceImpl::batch_append_entries(google::protobuf::RpcController* cntl_base,
                            const BatchAppendEntriesRequest* request,
                            BatchAppendEntriesResponse* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    // status buffer for response
    std::vector<BatchAppendEntriesResponse::Status> statuses;
    // if has error, the statuses will be set to response, else the statuses in response is null.
    bool has_error = false;
    int64_t start_time = butil::monotonic_time_ms();
    for (const auto& req : request->requests()) {
        PeerId peer_id;
        BatchAppendEntriesResponse::Status status;
        brpc::Controller cntl;

        if (0 != peer_id.parse(req.peer_id())) {
            auto* mut_response = response->add_responses();
            mut_response->set_term(0);
            mut_response->set_success(false);

            // prepare status for response
            status.set_error_code(EINVAL);
            status.set_error_msg("peer_id invalid");
            statuses.push_back(std::move(status));
            has_error = true;
            continue;
        }

        scoped_refptr<NodeImpl> node_ptr = 
                            global_node_manager->get(req.group_id(), peer_id);
        NodeImpl* node = node_ptr.get();
        if (!node) {
            auto* mut_response = response->add_responses();
            mut_response->set_term(0);
            mut_response->set_success(false);

            // prepare status for response
            status.set_error_code(ENOENT);
            status.set_error_msg("peer_id not exist");
            statuses.push_back(std::move(status));
            has_error = true;
            continue;
        }

        // for batch append_entries, we must set the required field before return
        // the BatchAppendEntries closure will passthrough the cntl failed code and
        // msg to each group, so the response can be set safely and to match proto2
        // required field check in the BatchAppendEntriesResponse
        auto *individual_response = response->add_responses();
        individual_response->set_success(false);

        node->handle_append_entries_request(&cntl, &req, individual_response, nullptr);
        if(cntl.Failed()) {
            status.set_error_code(cntl.ErrorCode());
            status.set_error_msg(cntl.ErrorText());
            has_error = true;
        } else {
            status.set_error_code(0);
            status.set_error_msg("");
        }
        statuses.push_back(std::move(status));
    }

    // if has_error, set status to response
    // else the statuses in response is null.
    // this is used to save network bandwidth.
    if (has_error) {
        for (auto& status : statuses) {
            auto* mut_status = response->add_statuses();
            *mut_status = std::move(status);
        }
    }
    
    int64_t eplased_time =  butil::monotonic_time_ms() - start_time;
    LOG_IF(INFO, eplased_time > 1000) << "batch_append_entries size: " << request->requests_size() << " eplased time: " << eplased_time;
}

void RaftServiceImpl::install_snapshot(google::protobuf::RpcController* cntl_base,
                              const InstallSnapshotRequest* request,
                              InstallSnapshotResponse* response,
                              google::protobuf::Closure* done) {
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        done->Run();
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = 
                        global_node_manager->get(request->group_id(), peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");

        done->Run();
        return;
    }

    node->handle_install_snapshot_request(cntl, request, response, done);
}

void RaftServiceImpl::timeout_now(::google::protobuf::RpcController* controller,
                                  const ::braft::TimeoutNowRequest* request,
                                  ::braft::TimeoutNowResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        done->Run();
        return;
    }

    scoped_refptr<NodeImpl> node_ptr = 
                        global_node_manager->get(request->group_id(), peer_id);
    NodeImpl* node = node_ptr.get();
    if (!node) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        done->Run();
        return;
    }

    node->handle_timeout_now_request(cntl, request, response, done);
}

}
