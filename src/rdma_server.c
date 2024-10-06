
#include "rdma_common.h"

/* RDMA 리소스를 관리할 구조체 */
struct rdma_server_resources
{
	struct rdma_event_channel *cm_event_channel;
	struct ibv_comp_channel *io_completion_channel;
	struct rdma_cm_id *cm_server_id;
	struct rdma_cm_id *cm_client_id;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_qp *qp;

	struct ibv_mr *server_buffer_mr;

	struct rdma_buffer_attr metadata_attr;
};
/* 서버 종료 및 리소스 정리 */
static int server_disconnect_and_cleanup(struct rdma_server_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event);
	if (ret)
	{
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event\n");
		return ret;
	}
	rdma_ack_cm_event(cm_event);

	rdma_destroy_qp(res->cm_client_id);
	rdma_destroy_id(res->cm_client_id);
	ibv_destroy_cq(res->cq);
	ibv_destroy_comp_channel(res->io_completion_channel);

	// 클라이언트가 write한 데이터를 출력
	printf("Client wrote the following data: %s\n", (char *)res->server_buffer_mr->addr);

	rdma_buffer_free(res->server_buffer_mr);
	ibv_dealloc_pd(res->pd);
	rdma_destroy_id(res->cm_server_id);
	rdma_destroy_event_channel(res->cm_event_channel);

	printf("Server shut-down is complete \n");
	return 0;
}

/* RDMA 서버 시작 */
static int server_prepare_connection(struct sockaddr_in *server_addr, struct rdma_server_resources *res)
{
	res->cm_event_channel = rdma_create_event_channel();
	if (!res->cm_event_channel)
	{
		rdma_error("Failed to create event channel, errno: %d\n", -errno);
		return -errno;
	}

	if (rdma_create_id(res->cm_event_channel, &res->cm_server_id, NULL, RDMA_PS_TCP))
	{
		rdma_error("Failed to create server cm id, errno: %d\n", -errno);
		return -errno;
	}

	if (rdma_bind_addr(res->cm_server_id, (struct sockaddr *)server_addr))
	{
		rdma_error("Failed to bind server address, errno: %d\n", -errno);
		return -errno;
	}

	if (rdma_listen(res->cm_server_id, 8))
	{
		rdma_error("Failed to listen on server address, errno: %d\n", -errno);
		return -errno;
	}

	printf("Server is listening at: %s, port: %d\n", inet_ntoa(server_addr->sin_addr), ntohs(server_addr->sin_port));

	struct rdma_cm_event *cm_event = NULL;
	int ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_CONNECT_REQUEST, &cm_event);
	if (ret)
	{
		rdma_error("Failed to get CM event\n");
		return ret;
	}

	res->cm_client_id = cm_event->id;
	rdma_ack_cm_event(cm_event);

	if (!res->cm_client_id)
	{
		rdma_error("Client id is NULL\n");
		return -EINVAL;
	}

	// 보호 도메인 할당
	res->pd = ibv_alloc_pd(res->cm_client_id->verbs);
	if (!res->pd)
	{
		rdma_error("Failed to allocate protection domain, errno: %d\n", -errno);
		return -errno;
	}

	// I/O 완료 채널 및 완료 큐 설정
	res->io_completion_channel = ibv_create_comp_channel(res->cm_client_id->verbs);
	if (!res->io_completion_channel)
	{
		rdma_error("Failed to create I/O completion event channel, errno: %d\n", -errno);
		return -errno;
	}

	res->cq = ibv_create_cq(res->cm_client_id->verbs, CQ_CAPACITY, NULL, res->io_completion_channel, 0);
	if (!res->cq)
	{
		rdma_error("Failed to create completion queue, errno: %d\n", -errno);
		return -errno;
	}

	if (ibv_req_notify_cq(res->cq, 0))
	{
		rdma_error("Failed to request notifications on CQ, errno: %d\n", -errno);
		return -errno;
	}

	// 큐 페어(QP) 설정
	struct ibv_qp_init_attr qp_init_attr = {
		.cap = {
			.max_recv_sge = MAX_SGE,
			.max_recv_wr = MAX_WR,
			.max_send_sge = MAX_SGE,
			.max_send_wr = MAX_WR,
		},
		.qp_type = IBV_QPT_RC,
		.recv_cq = res->cq,
		.send_cq = res->cq,
	};

	if (rdma_create_qp(res->cm_client_id, res->pd, &qp_init_attr))
	{
		rdma_error("Failed to create QP, errno: %d\n", -errno);
		return -errno;
	}

	res->qp = res->cm_client_id->qp;

	return 0;
}

static int server_alloc_memory(struct rdma_server_resources *res, uint32_t length)
{
	// 서버 버퍼 할당
	res->server_buffer_mr = rdma_buffer_alloc(res->pd, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

	if (!res->server_buffer_mr)
	{
		rdma_error("Failed to allocate server buffer\n");
		return -ENOMEM;
	}
}

/* 서버 메타데이터를 클라이언트로 전송 */
static int server_accept_client_and_send_metadata(struct rdma_server_resources *res)
{
	struct rdma_conn_param conn_param = {
		.initiator_depth = 3,
		.responder_resources = 3,
	};
	if (rdma_accept(res->cm_client_id, &conn_param))
	{
		rdma_error("Failed to accept client connection, errno: %d\n", -errno);
		return -errno;
	}

	struct rdma_cm_event *cm_event = NULL;
	if (process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event))
	{
		rdma_error("Failed to get CM event\n");
		return -errno;
	}

	rdma_ack_cm_event(cm_event);

	struct rdma_buffer_attr server_metadata_attr = {
		.address = (uint64_t)res->server_buffer_mr->addr,
		.length = (uint32_t)res->server_buffer_mr->length,
		.stag.local_stag = (uint32_t)res->server_buffer_mr->lkey,
	};

	struct ibv_mr *server_metadata_mr = rdma_buffer_register(res->pd, &server_metadata_attr, sizeof(server_metadata_attr), IBV_ACCESS_LOCAL_WRITE);

	struct ibv_sge send_sge = {
		.addr = (uint64_t)&server_metadata_attr,
		.length = sizeof(server_metadata_attr),
		.lkey = server_metadata_mr->lkey,
	};
	struct ibv_send_wr send_wr = {
		.sg_list = &send_sge,
		.num_sge = 1,
		.opcode = IBV_WR_SEND,
		.send_flags = IBV_SEND_SIGNALED,
	};
	struct ibv_send_wr *bad_send_wr = NULL;

	if (ibv_post_send(res->qp, &send_wr, &bad_send_wr))
	{
		rdma_error("Failed to post send WR, errno: %d\n", -errno);
		return -errno;
	}

	struct ibv_wc wc;
	if (process_work_completion_events(res->io_completion_channel, &wc, 1) != 1)
	{
		rdma_error("Failed to send server metadata\n");
		return -errno;
	}

	rdma_buffer_deregister(server_metadata_mr);
	return 0;
}

#define SERVER_DATA_LEN 100000

/* Main function */
int main(int argc, char **argv)
{
	int ret;
	struct sockaddr_in server_sockaddr;
	struct rdma_server_resources res;
	bzero(&res, sizeof(res));

	/* Initialize server address */
	bzero(&server_sockaddr, sizeof(server_sockaddr));
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = inet_addr("10.10.16.51");
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

	/* Start RDMA server */
	ret = server_prepare_connection(&server_sockaddr, &res);
	if (ret)
	{
		rdma_error("Failed to start RDMA server\n");
		return ret;
	}

	server_alloc_memory(&res, SERVER_DATA_LEN);

	/* Send server metadata */
	ret = server_accept_client_and_send_metadata(&res);
	if (ret)
	{
		rdma_error("Failed to send server metadata\n");
		return ret;
	}

	/* Disconnect and cleanup */
	ret = server_disconnect_and_cleanup(&res);
	if (ret)
	{
		rdma_error("Failed to cleanup server resources\n");
		return ret;
	}

	return 0;
}