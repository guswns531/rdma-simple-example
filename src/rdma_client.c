#include "rdma_common.h"

struct buffer
{
	uint64_t flag;
	char str[1000000];
};

/* RDMA 리소스를 관리할 구조체 */
struct rdma_client_resources
{
	struct rdma_event_channel *cm_event_channel;
	struct ibv_comp_channel *io_completion_channel;
	struct rdma_cm_id *cm_client_id;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_qp *qp;

	struct ibv_mr *client_buffer_mr;
	struct rdma_buffer_attr metadata_attr;

	struct buffer client_buffer;
};

/* 에러 메시지를 출력하고 에러 코드를 반환하는 함수 */
static inline int handle_error(const char *message)
{
	perror(message); // perror를 사용하여 오류 메시지와 errno 값을 출력
	return -errno;	 // errno 값을 반환
}

static int client_disconnect(struct rdma_client_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;

	ret = rdma_disconnect(res->cm_client_id);
	if (ret)
		handle_error("Failed to disconnect");

	ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event);
	if (ret)
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n", ret);

	rdma_ack_cm_event(cm_event);
	return 0;
}

/* 클라이언트 종료 및 리소스 정리 */
static int client_cleanup(struct rdma_client_resources *res)
{
	rdma_destroy_qp(res->cm_client_id);
	rdma_destroy_id(res->cm_client_id);

	ibv_destroy_cq(res->cq);
	ibv_destroy_comp_channel(res->io_completion_channel);

	rdma_buffer_deregister(res->client_buffer_mr);
	ibv_dealloc_pd(res->pd);
	rdma_destroy_event_channel(res->cm_event_channel);
	printf("클라이언트 리소스 정리 완료 \n");
	return 0;
}

/* Helper function to post RDMA send operation */
static int rdma_post_send(struct ibv_qp *qp, enum ibv_wr_opcode opcode, uint64_t local_addr, uint32_t length, uint32_t lkey, uint64_t remote_addr, uint32_t rkey)
{
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
	int ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	if (ret)
	{
		perror("Failed to post RDMA send operation");
		if (bad_send_wr)
		{
			fprintf(stderr, "Bad send WR at %p\n", (void *)bad_send_wr);
		}
		return -errno;
	}
	return 0;
}

/* Helper function to process work completions */
static int process_work_completion(struct ibv_comp_channel *io_completion_channel, int expected_wc)
{
	struct ibv_wc wc;
	int ret = process_work_completion_events(io_completion_channel, &wc, expected_wc);
	if (ret != expected_wc)
	{
		printf("Failed to get %d work completions, ret = %d\n", expected_wc, ret);
		return ret;
	}
	return 0;
}

/* Remote Memory Write */
static int client_rdma_write(struct rdma_client_resources *res)
{
	int ret = rdma_post_send(res->qp, IBV_WR_RDMA_WRITE,
							 (uint64_t)res->client_buffer_mr->addr, res->client_buffer_mr->length, res->client_buffer_mr->lkey,
							 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
		return handle_error("Failed to post RDMA write operation");

	ret = process_work_completion(res->io_completion_channel, 1);
	if (ret)
		return handle_error("Failed to process work completion for RDMA write");

	struct buffer *getdata = (struct buffer *)res->client_buffer_mr->addr;
	printf("Client Write completed: %ld, %s\n", getdata->flag, getdata->str);
	return 0;
}

/* Remote Memory Read */
static int client_rdma_read(struct rdma_client_resources *res)
{
	int ret = rdma_post_send(res->qp, IBV_WR_RDMA_READ,
							 (uint64_t)res->client_buffer_mr->addr, res->client_buffer_mr->length, res->client_buffer_mr->lkey,
							 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
		return handle_error("Failed to post RDMA read operation");

	ret = process_work_completion(res->io_completion_channel, 1);
	if (ret)
		return handle_error("Failed to process work completion for RDMA read");

	struct buffer *getdata = (struct buffer *)res->client_buffer_mr->addr;
	printf("Client Read completed: %ld, %s\n", getdata->flag, getdata->str);
	return 0;
}

/* Helper function to post RDMA Compare and Swap (CMP_AND_SWAP) operation */
static int rdma_post_compare_and_swap(struct ibv_qp *qp, uint64_t compare_val, uint64_t swap_val, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr, uint32_t rkey)
{
	struct ibv_sge send_sge = {
		.addr = local_addr,
		.length = sizeof(uint64_t),
		.lkey = lkey,
	};

	struct ibv_send_wr send_wr = {
		.sg_list = &send_sge,
		.num_sge = 1,
		.opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
		.send_flags = IBV_SEND_SIGNALED,
		.wr.atomic.remote_addr = remote_addr,
		.wr.atomic.rkey = rkey,
		.wr.atomic.compare_add = compare_val,
		.wr.atomic.swap = swap_val,
	};

	struct ibv_send_wr *bad_send_wr = NULL;
	int ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	if (ret)
	{
		perror("Failed to post CMP_AND_SWAP operation");
		if (bad_send_wr)
		{
			fprintf(stderr, "Bad CMP_AND_SWAP WR at %p\n", (void *)bad_send_wr);
		}
		return -errno;
	}
	return 0;
}

/* Remote CMP_AND_SWAP Operation */
static int client_rdma_compare_and_swap(struct rdma_client_resources *res, uint64_t compare_val, uint64_t swap_val)
{
	int ret = rdma_post_compare_and_swap(res->qp, compare_val, swap_val,
										 (uint64_t)res->client_buffer_mr->addr, res->client_buffer_mr->lkey,
										 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
		return handle_error("Failed to post CMP_AND_SWAP operation");

	ret = process_work_completion(res->io_completion_channel, 1);
	if (ret)
		return handle_error("Failed to process work completion for CMP_AND_SWAP");

	printf("CMP_AND_SWAP operation completed. Result: %lu\n", *(uint64_t *)res->client_buffer_mr->addr);
	return 0;
}

/* RDMA 이벤트를 처리하는 공통 함수 */
static int process_rdma_event(struct rdma_event_channel *event_channel, enum rdma_cm_event_type expected_event)
{
	struct rdma_cm_event *cm_event = NULL;
	if (process_rdma_cm_event(event_channel, expected_event, &cm_event))
		return handle_error("RDMA event processing failed");

	rdma_ack_cm_event(cm_event); // 이벤트를 확인한 후 acknowledge
	return 0;
}

/* 클라이언트 연결 준비 함수 */
static int client_prepare_connection(struct sockaddr_in *s_addr, struct rdma_client_resources *res)
{
	res->cm_event_channel = rdma_create_event_channel();
	if (!res->cm_event_channel)
		return handle_error("Failed to create event channel");

	if (rdma_create_id(res->cm_event_channel, &res->cm_client_id, NULL, RDMA_PS_TCP))
		return handle_error("Failed to create client ID");

	if (rdma_resolve_addr(res->cm_client_id, NULL, (struct sockaddr *)s_addr, 2000))
		return handle_error("Failed to resolve address");

	if (process_rdma_event(res->cm_event_channel, RDMA_CM_EVENT_ADDR_RESOLVED))
		return handle_error("Failed to process RDMA_CM_EVENT_ADDR_RESOLVED");

	if (rdma_resolve_route(res->cm_client_id, 2000))
		return handle_error("Failed to resolve route");

	if (process_rdma_event(res->cm_event_channel, RDMA_CM_EVENT_ROUTE_RESOLVED))
		return handle_error("Failed to process RDMA_CM_EVENT_ROUTE_RESOLVED");

	res->pd = ibv_alloc_pd(res->cm_client_id->verbs);
	if (!res->pd)
		return handle_error("Failed to allocate protection domain");

	res->io_completion_channel = ibv_create_comp_channel(res->cm_client_id->verbs);
	if (!res->io_completion_channel)
		return handle_error("Failed to create completion event channel");

	res->cq = ibv_create_cq(res->cm_client_id->verbs, CQ_CAPACITY, NULL, res->io_completion_channel, 0);
	if (!res->cq)
		return handle_error("Failed to create completion queue");

	if (ibv_req_notify_cq(res->cq, 0))
		return handle_error("Failed to request CQ notification");

	struct ibv_qp_init_attr qp_init_attr = {
		.cap = {
			.max_recv_sge = MAX_SGE,
			.max_recv_wr = MAX_WR,
			.max_send_sge = MAX_SGE,
			.max_send_wr = MAX_WR},
		.qp_type = IBV_QPT_RC,
		.recv_cq = res->cq,
		.send_cq = res->cq};

	if (rdma_create_qp(res->cm_client_id, res->pd, &qp_init_attr))
		return handle_error("Failed to create QP");

	res->qp = res->cm_client_id->qp;
	return 0;
}

/* 수신 버퍼 미리 등록 함수 */
static int client_prepare_recv_metadata(struct rdma_client_resources *res)
{
	struct ibv_mr *server_metadata_mr = rdma_buffer_register(res->pd, &res->metadata_attr, sizeof(res->metadata_attr), IBV_ACCESS_LOCAL_WRITE);
	if (!server_metadata_mr)
		return handle_error("Failed to register server metadata buffer");

	struct ibv_sge recv_sge = {
		.addr = (uint64_t)server_metadata_mr->addr,
		.length = (uint32_t)server_metadata_mr->length,
		.lkey = (uint32_t)server_metadata_mr->lkey};

	struct ibv_recv_wr recv_wr = {
		.sg_list = &recv_sge,
		.num_sge = 1};

	struct ibv_recv_wr *bad_recv_wr = NULL;
	if (ibv_post_recv(res->qp, &recv_wr, &bad_recv_wr))
	{
		if (bad_recv_wr)
			fprintf(stderr, "Error: bad_recv_wr at %p\n", (void *)bad_recv_wr);
		return handle_error("Failed to post receive request");
	}

	return 0;
}

/* Metadata Exchange */
static int client_connect_to_server(struct rdma_client_resources *res)
{
	if (client_prepare_recv_metadata(res))
		return handle_error("Failed to prepare receive metadata");

	printf("Connecting...\n");

	struct rdma_conn_param conn_param = {
		.initiator_depth = 3,
		.responder_resources = 3,
		.retry_count = 3};

	if (rdma_connect(res->cm_client_id, &conn_param))
		return handle_error("Failed to connect to remote host");

	if (process_rdma_event(res->cm_event_channel, RDMA_CM_EVENT_ESTABLISHED))
		return handle_error("Failed to process RDMA_CM_EVENT_ESTABLISHED");

	printf("Client connected successfully\n");

	struct ibv_wc wc;
	if (process_work_completion_events(res->io_completion_channel, &wc, 1) != 1)
		return handle_error("Failed to complete work event");

	printf("Client received metadata successfully\n");

	res->client_buffer_mr = rdma_buffer_register(res->pd, &res->client_buffer, sizeof(res->client_buffer),
												 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	if (!res->client_buffer_mr)
		return handle_error("Failed to register client buffer");

	printf("Client buffer allocated: %ld bytes\n", sizeof(res->client_buffer));
	return 0;
}

/* Main function */
int main(int argc, char **argv)
{
	struct rdma_client_resources res;
	memset(&res, 0, sizeof(res));

	if (argc < 2)
	{
		printf("Usage: %s <text_to_send>\n", argv[0]);
		return -1;
	}

	const char *text_to_send = argv[1];
	strncpy(res.client_buffer.str, text_to_send, strlen(text_to_send));

	struct sockaddr_in server_sockaddr;
	memset(&server_sockaddr, 0, sizeof(server_sockaddr));
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = inet_addr("10.10.16.51");
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

	if (client_prepare_connection(&server_sockaddr, &res))
		return handle_error("Failed to prepare client connection");

	if (client_connect_to_server(&res))
		return handle_error("Failed to connect to server");

	printf("Starting RDMA operations...\n");

	if (client_rdma_read(&res))
		return handle_error("Failed to perform initial RDMA read");

	res.client_buffer.flag = 0;
	memset(&res.client_buffer.str, 0, sizeof(res.client_buffer.str));

	if (client_rdma_compare_and_swap(&res, 0, 2))
		return handle_error("Failed to perform CMP_AND_SWAP");

	if (client_rdma_read(&res))
		return handle_error("Failed to perform RDMA read after CMP_AND_SWAP");

	res.client_buffer.flag = 5;
	strncpy(res.client_buffer.str, text_to_send, strlen(text_to_send));

	if (client_rdma_write(&res))
		return handle_error("Failed to perform RDMA write");

	res.client_buffer.flag = 3;
	memset(&res.client_buffer.str, 0, sizeof(res.client_buffer.str));

	if (client_rdma_read(&res))
		return handle_error("Failed to perform RDMA read after write");

	sleep(10);

	if (client_disconnect(&res))
		return handle_error("Failed to disconnect client");

	return client_cleanup(&res);
}