#include "rdma_common.h"

/* RDMA 리소스를 관리할 구조체 */
struct rdma_client_resources
{
	struct rdma_event_channel *cm_event_channel;
	struct ibv_comp_channel *io_completion_channel;
	struct rdma_cm_id *cm_client_id;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_qp *qp;

	struct ibv_mr *client_src_mr;
	struct ibv_mr *client_dst_mr;
	struct rdma_buffer_attr metadata_attr;
	char *src;
	char *dst;
};

/* 클라이언트 종료 및 리소스 정리 */
static int client_disconnect_and_clean(struct rdma_client_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;

	ret = rdma_disconnect(res->cm_client_id);
	if (ret)
	{
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
	}

	ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event);
	if (ret)
	{
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n", ret);
	}

	rdma_ack_cm_event(cm_event);

	rdma_destroy_qp(res->cm_client_id);
	rdma_destroy_id(res->cm_client_id);

	ibv_destroy_cq(res->cq);
	ibv_destroy_comp_channel(res->io_completion_channel);

	rdma_buffer_deregister(res->client_src_mr);
	rdma_buffer_deregister(res->client_dst_mr);

	free(res->src);
	free(res->dst);

	ibv_dealloc_pd(res->pd);
	rdma_destroy_event_channel(res->cm_event_channel);
	printf("클라이언트 리소스 정리 완료 \n");
	return 0;
}

/* 메모리 비교 함수 */
static int check_src_dst(struct rdma_client_resources *res)
{
	return memcmp((void *)res->src, (void *)res->dst, strlen(res->src));
}
/* Helper function to post RDMA send operation */
static int rdma_post_send(struct ibv_qp *qp, enum ibv_wr_opcode opcode, uint64_t local_addr, uint32_t length, uint32_t lkey, uint64_t remote_addr, uint32_t rkey)
{
	int ret;
	struct ibv_sge send_sge = {
		.addr = local_addr,
		.length = length,
		.lkey = lkey,
	};

	struct ibv_send_wr send_wr = {
		.sg_list = &send_sge,
		.num_sge = 1,
		.opcode = opcode,
		.send_flags = IBV_SEND_SIGNALED,
		.wr.rdma.remote_addr = remote_addr,
		.wr.rdma.rkey = rkey,
	};

	struct ibv_send_wr *bad_send_wr = NULL;

	/* Post the send request */
	ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	if (ret)
	{
		rdma_error("Failed to post send operation, errno: %d\n", -errno);
		if (bad_send_wr)
		{
			fprintf(stderr, "Error: bad_send_wr at %p\n", (void *)bad_send_wr);
		}
		return -errno;
	}

	return 0;
}

/* Remote Memory Write */
static int client_remote_memory_write(struct rdma_client_resources *res)
{
	/* Register source buffer */
	res->client_src_mr = rdma_buffer_register(res->pd, res->src, strlen(res->src),
											  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	if (!res->client_src_mr)
	{
		rdma_error("Failed to register source buffer\n");
		return -ENOMEM;
	}

	/* Post RDMA write operation */
	int ret = rdma_post_send(res->qp, IBV_WR_RDMA_WRITE,
							 (uint64_t)res->client_src_mr->addr, res->client_src_mr->length, res->client_src_mr->lkey,
							 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
		return ret;

	/* Process the work completion */
	struct ibv_wc wc;
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1)
	{
		rdma_error("Failed to get 1 work completion, ret = %d\n", ret);
		return ret;
	}

	printf("Client WRITE completed: %s\n", res->src);
	return 0;
}

/* Remote Memory Read */
static int client_remote_memory_read(struct rdma_client_resources *res)
{
	/* Register destination buffer */
	res->client_dst_mr = rdma_buffer_register(res->pd, res->dst, strlen(res->src),
											  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
	if (!res->client_dst_mr)
	{
		rdma_error("Failed to register destination buffer\n");
		return -ENOMEM;
	}

	/* Post RDMA read operation */
	int ret = rdma_post_send(res->qp, IBV_WR_RDMA_READ,
							 (uint64_t)res->client_dst_mr->addr, res->client_dst_mr->length, res->client_dst_mr->lkey,
							 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
		return ret;

	/* Process the work completion */
	struct ibv_wc wc;
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1)
	{
		rdma_error("Failed to get 1 work completion, ret = %d\n", ret);
		return ret;
	}

	printf("Client READ completed: %s\n", res->dst);
	return 0;
}

/* Helper function for RDMA CM event handling */
static int cm_event_ack_and_process(struct rdma_event_channel *event_channel, enum rdma_cm_event_type expected_event, struct rdma_cm_event **cm_event)
{
	int ret = process_rdma_cm_event(event_channel, expected_event, cm_event);
	if (ret)
		return ret;

	ret = rdma_ack_cm_event(*cm_event);
	if (ret)
	{
		rdma_error("Failed to acknowledge CM event, errno: %d\n", -errno);
		return -errno;
	}

	return 0;
}

/* 클라이언트 연결 준비 함수 */
static int client_prepare_connection(struct sockaddr_in *s_addr, struct rdma_client_resources *res)
{
	/* RDMA 이벤트 채널 생성 */
	res->cm_event_channel = rdma_create_event_channel();
	if (!res->cm_event_channel)
	{
		rdma_error("Failed to create cm event channel, errno: %d\n", -errno);
		return -errno;
	}

	/* RDMA 클라이언트 ID 생성 */
	int ret = rdma_create_id(res->cm_event_channel, &res->cm_client_id, NULL, RDMA_PS_TCP);
	if (ret)
	{
		rdma_error("Failed to create cm id, errno: %d\n", -errno);
		return -errno;
	}

	/* 주소 해석 */
	ret = rdma_resolve_addr(res->cm_client_id, NULL, (struct sockaddr *)s_addr, 2000);
	if (ret)
	{
		rdma_error("Failed to resolve address, errno: %d\n", -errno);
		return -errno;
	}

	/* 주소 해석 이벤트 확인 */
	struct rdma_cm_event *cm_event = NULL;
	ret = cm_event_ack_and_process(res->cm_event_channel, RDMA_CM_EVENT_ADDR_RESOLVED, &cm_event);
	if (ret)
		return ret;

	/* 경로 해석 */
	ret = rdma_resolve_route(res->cm_client_id, 2000);
	if (ret)
	{
		rdma_error("Failed to resolve route, errno: %d\n", -errno);
		return -errno;
	}

	/* 경로 해석 이벤트 확인 */
	ret = cm_event_ack_and_process(res->cm_event_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, &cm_event);
	if (ret)
		return ret;

	/* 보호 도메인 할당 */
	res->pd = ibv_alloc_pd(res->cm_client_id->verbs);
	if (!res->pd)
	{
		rdma_error("Failed to alloc pd, errno: %d\n", -errno);
		return -errno;
	}

	/* IO 완료 채널 생성 */
	res->io_completion_channel = ibv_create_comp_channel(res->cm_client_id->verbs);
	if (!res->io_completion_channel)
	{
		rdma_error("Failed to create IO completion event channel, errno: %d\n", -errno);
		return -errno;
	}

	/* 완료 큐(CQ) 생성 */
	res->cq = ibv_create_cq(res->cm_client_id->verbs, CQ_CAPACITY, NULL, res->io_completion_channel, 0);
	if (!res->cq)
	{
		rdma_error("Failed to create CQ, errno: %d\n", -errno);
		return -errno;
	}

	/* CQ 알림 요청 */
	ret = ibv_req_notify_cq(res->cq, 0);
	if (ret)
	{
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		return -errno;
	}

	/* 큐 페어(QP) 설정 */
	struct ibv_qp_init_attr qp_init_attr = {
		.cap = {
			.max_recv_sge = MAX_SGE,
			.max_recv_wr = MAX_WR,
			.max_send_sge = MAX_SGE,
			.max_send_wr = MAX_WR},
		.qp_type = IBV_QPT_RC,
		.recv_cq = res->cq,
		.send_cq = res->cq};

	ret = rdma_create_qp(res->cm_client_id, res->pd, &qp_init_attr);
	if (ret)
	{
		rdma_error("Failed to create QP, errno: %d\n", -errno);
		return -errno;
	}

	res->qp = res->cm_client_id->qp;
	return 0;
}

/* 수신 버퍼 미리 등록 함수 */
static int client_prepare_recv_metadata(struct rdma_client_resources *res)
{
	struct ibv_sge recv_sge;
	struct ibv_recv_wr recv_wr;
	struct ibv_recv_wr *bad_recv_wr = NULL;

	/* 서버 메타데이터 메모리 등록 */
	struct ibv_mr *server_metadata_mr = rdma_buffer_register(res->pd, &res->metadata_attr, sizeof(res->metadata_attr), IBV_ACCESS_LOCAL_WRITE);
	if (!server_metadata_mr)
	{
		rdma_error("Failed to register server metadata MR\n");
		return -ENOMEM;
	}

	/* 수신 작업 요청 설정 */
	recv_sge.addr = (uint64_t)server_metadata_mr->addr;
	recv_sge.length = (uint32_t)server_metadata_mr->length;
	recv_sge.lkey = (uint32_t)server_metadata_mr->lkey;

	/* Work request 설정 */
	memset(&recv_wr, 0, sizeof(recv_wr));
	recv_wr.sg_list = &recv_sge;
	recv_wr.num_sge = 1;

	/* 수신 작업 미리 등록 */
	int ret = ibv_post_recv(res->qp, &recv_wr, &bad_recv_wr);
	if (ret)
	{
		rdma_error("Failed to pre-post the receive buffer, errno: %d\n", ret);
		if (bad_recv_wr)
		{
			fprintf(stderr, "Error: bad_recv_wr at %p\n", (void *)bad_recv_wr);
		}
		return ret;
	}

	rdma_buffer_deregister(server_metadata_mr);
	return 0;
}

/* Metadata Exchange */
static int client_connect_to_server_and_get_metadata(struct rdma_client_resources *res)
{
	int ret = client_prepare_recv_metadata(res);
	if (ret)
	{
		return ret;
	}
	printf("Lets connect\n");

	struct rdma_conn_param conn_param;
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 3;

	sleep(1);

	ret = rdma_connect(res->cm_client_id, &conn_param);
	if (ret)
	{
		rdma_error("Failed to connect to remote host, errno: %d\n", -errno);
		return -errno;
	}

	printf("try connect\n");
	sleep(1);

	struct rdma_cm_event *cm_event = NULL;
	ret = cm_event_ack_and_process(res->cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event);
	if (ret)
		return ret;

	printf("The client is connected successfully\n");

	struct ibv_wc wc;

	/* Wait for the completion of metadata recv */
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1)
	{
		rdma_error("Failed to get 1 work completions, ret = %d\n", ret);
		return ret;
	}

	printf("The client recevied metadata successfully\n");

	return 0;
}

/* Main function */
int main(int argc, char **argv)
{
	struct rdma_client_resources res;
	memset(&res, 0, sizeof(res));

	const char *text_to_send = "textstring";
	res.src = calloc(strlen(text_to_send), 1);
	if (!res.src)
	{
		rdma_error("Failed to allocate memory: -ENOMEM\n");
		return -ENOMEM;
	}
	strncpy(res.src, text_to_send, strlen(text_to_send));

	res.dst = calloc(strlen(text_to_send), 1);
	if (!res.dst)
	{
		rdma_error("Failed to allocate destination memory, -ENOMEM\n");
		free(res.src);
		return -ENOMEM;
	}

	int ret;

	struct sockaddr_in server_sockaddr;
	memset(&server_sockaddr, 0, sizeof(server_sockaddr));
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = inet_addr("10.10.16.51");
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

	ret = client_prepare_connection(&server_sockaddr, &res);
	if (ret)
	{
		rdma_error("Failed to setup client connection, ret = %d\n", ret);
		return ret;
	}

	ret = client_connect_to_server_and_get_metadata(&res);
	if (ret)
	{
		return ret;
	}

	ret = client_remote_memory_write(&res);
	if (ret)
		return ret;

	ret = client_remote_memory_read(&res);
	if (ret)
		return ret;

	if (check_src_dst(&res))
	{
		rdma_error("src and dst buffers do not match\n");
	}
	else
	{
		printf("SUCCESS: source and destination buffers match\n");
	}

	sleep(10);
	ret = client_disconnect_and_clean(&res);
	return ret;
}