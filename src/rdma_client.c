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

static int client_disconnect(struct rdma_client_resources *res)
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
	// free(res->client_buffer.str);
	ibv_dealloc_pd(res->pd);
	rdma_destroy_event_channel(res->cm_event_channel);
	printf("클라이언트 리소스 정리 완료 \n");
	return 0;
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
static int client_rdma_write(struct rdma_client_resources *res)
{
	/* Post RDMA write operation */
	int ret = rdma_post_send(res->qp, IBV_WR_RDMA_WRITE,
							 (uint64_t)res->client_buffer_mr->addr, res->client_buffer_mr->length, res->client_buffer_mr->lkey,
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

	struct buffer *getdata = (struct buffer *)res->client_buffer_mr->addr;
	printf("Client Write completed: %ld, %s\n", getdata->flag, getdata->str);
	return 0;
}

/* Remote Memory Read */
static int client_rdma_read(struct rdma_client_resources *res)
{
	/* Post RDMA read operation */
	int ret = rdma_post_send(res->qp, IBV_WR_RDMA_READ,
							 (uint64_t)res->client_buffer_mr->addr, res->client_buffer_mr->length, res->client_buffer_mr->lkey,
							 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
	{
		printf("client read error");
		return ret;
	}

	/* Process the work completion */
	struct ibv_wc wc;
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1)
	{
		rdma_error("Failed to get 1 work completion, ret = %d\n", ret);
		return ret;
	}

	struct buffer *getdata = (struct buffer *)res->client_buffer_mr->addr;
	printf("Client read completed: %ld, %s\n", getdata->flag, getdata->str);
	return 0;
}

/* Helper function to post RDMA Compare and Swap (CMP_AND_SWAP) operation */
static int rdma_post_compare_and_swap(struct ibv_qp *qp, uint64_t compare_val, uint64_t swap_val, uint64_t local_addr, uint32_t lkey, uint64_t remote_addr, uint32_t rkey)
{
	int ret;
	struct ibv_sge send_sge = {
		.addr = local_addr,
		.length = sizeof(uint64_t), // CMP_AND_SWAP 연산은 64비트(8바이트) 값을 사용합니다.
		.lkey = lkey,
	};

	struct ibv_send_wr send_wr = {
		.sg_list = &send_sge,
		.num_sge = 1,
		.opcode = IBV_WR_ATOMIC_CMP_AND_SWP, // Atomic Compare and Swap
		.send_flags = IBV_SEND_SIGNALED,
		.wr.atomic.remote_addr = remote_addr, // 원격 주소
		.wr.atomic.rkey = rkey,				  // 원격 키
		.wr.atomic.compare_add = compare_val, // 비교할 값
		.wr.atomic.swap = swap_val,			  // 교환할 값
	};

	struct ibv_send_wr *bad_send_wr = NULL;

	/* Post the CMP_AND_SWAP request */
	ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	if (ret)
	{
		rdma_error("Failed to post CMP_AND_SWAP operation, errno: %d\n", -errno);
		if (bad_send_wr)
		{
			fprintf(stderr, "Error: bad_send_wr at %p\n", (void *)bad_send_wr);
		}
		return -errno;
	}

	return 0;
}

/* Remote CMP_AND_SWAP Operation */
static int client_rdma_compare_and_swap(struct rdma_client_resources *res, uint64_t compare_val, uint64_t swap_val)
{
	/* Post RDMA CMP_AND_SWAP operation */
	int ret = rdma_post_compare_and_swap(res->qp, compare_val, swap_val,
										 (uint64_t)res->client_buffer_mr->addr, res->client_buffer_mr->lkey,
										 res->metadata_attr.address, res->metadata_attr.stag.remote_stag);
	if (ret)
		return ret;

	/* Process the work completion */
	struct ibv_wc wc;
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1)
	{
		rdma_error("Failed to get 1 work completion for CMP_AND_SWAP, ret = %d\n", ret);
		return ret;
	}

	printf("CMP_AND_SWAP operation completed. Result: %lu\n", *(uint64_t *)res->client_buffer_mr->addr);
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
	ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_ADDR_RESOLVED, &cm_event);
	if (ret)
		return ret;

	rdma_ack_cm_event(cm_event);

	/* 경로 해석 */
	ret = rdma_resolve_route(res->cm_client_id, 2000);
	if (ret)
	{
		rdma_error("Failed to resolve route, errno: %d\n", -errno);
		return -errno;
	}

	/* 경로 해석 이벤트 확인 */
	ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, &cm_event);
	if (ret)
		return ret;
	rdma_ack_cm_event(cm_event);

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

	// rdma_buffer_deregister(server_metadata_mr);
	return 0;
}

/* Metadata Exchange */
static int client_connect_to_server(struct rdma_client_resources *res)
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
	ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event);
	if (ret)
		return ret;
	rdma_ack_cm_event(cm_event);
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

	/* Register source buffer */
	res->client_buffer_mr = rdma_buffer_register(res->pd, &res->client_buffer, sizeof(res->client_buffer),
												 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	if (!res->client_buffer_mr)
	{
		rdma_error("Failed to register source buffer %ld\n", sizeof(res->client_buffer));
		return -ENOMEM;
	}

	printf("The client allocted buffuer  %ld\n", sizeof(res->client_buffer));

	return 0;
}

/* Main function */
int main(int argc, char **argv)
{
	struct rdma_client_resources res;
	memset(&res, 0, sizeof(res));

	// 입력 인자 확인
	if (argc < 2)
	{
		rdma_error("Usage: %s <text_to_send>\n", argv[0]);
		return -1;
	}

	const char *text_to_send = argv[1]; // 첫 번째 인자로부터 데이터 받음

	strncpy(res.client_buffer.str, text_to_send, strlen(text_to_send));

	int ret;

	// 서버 소켓 주소 초기화
	struct sockaddr_in server_sockaddr;
	memset(&server_sockaddr, 0, sizeof(server_sockaddr));
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = inet_addr("10.10.16.51");
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

	// 클라이언트 연결 준비
	ret = client_prepare_connection(&server_sockaddr, &res);
	if (ret)
	{
		rdma_error("Failed to setup client connection, ret = %d\n", ret);
		return ret;
	}

	// 서버에 연결하고 메타데이터 수신
	ret = client_connect_to_server(&res);
	if (ret)
	{
		return ret;
	}
	printf("Hello\n");
	// 원격 메모리에서 데이터 읽기
	ret = client_rdma_read(&res);
	if (ret)
	{
		return ret;
	}

	res.client_buffer.flag = 0;
	memset(&res.client_buffer.str, 0, sizeof(res.client_buffer.str));

	ret = client_rdma_compare_and_swap(&res, 0, 2);
	if (ret)
	{
		return ret;
	}

	ret = client_rdma_read(&res);
	if (ret)
	{
		return ret;
	}

	res.client_buffer.flag = 5;
	strncpy(res.client_buffer.str, text_to_send, strlen(text_to_send));

	// 원격 메모리로 데이터 전송
	ret = client_rdma_write(&res);
	if (ret)
	{
		return ret;
	}

	res.client_buffer.flag = 3;
	memset(&res.client_buffer.str, 0, sizeof(res.client_buffer.str));

	// 원격 메모리에서 데이터 읽기
	ret = client_rdma_read(&res);
	if (ret)
	{
		return ret;
	}

	// 잠시 대기
	sleep(10);

	// 연결 해제 및 리소스 정리
	ret = client_disconnect(&res);
	ret = client_cleanup(&res);

	return ret;
}