
#include "rdma_common.h"

#define SERVER_DATA_LEN 100000

struct buffer
{
	uint64_t flag;
	char str[1000000];
};

/* 각 클라이언트의 RDMA 리소스를 관리할 구조체 */
struct rdma_connected_client_resources
{
	struct rdma_cm_id *cm_client_id;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	struct ibv_comp_channel *io_completion_channel;

	struct ibv_mr *server_buffer_mr;
};

/* RDMA 리소스를 관리할 구조체 */
struct rdma_server_resources
{
	struct rdma_event_channel *cm_event_channel;
	struct rdma_cm_id *cm_server_id;
	int num_clients;									 // 현재 연결된 클라이언트 수
	struct rdma_connected_client_resources **client_res; // 클라이언트 배열

	struct buffer server_buffer;
};

/* 에러 메시지 출력과 코드 반환을 단일 함수로 처리 */
static inline int handle_error(const char *error_message)
{
	perror(error_message); // perror를 사용하여 오류 메시지와 errno 값을 출력
	return -errno;		   // errno 값을 반환
}

static int server_disconnect_wait(struct rdma_server_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event);
	if (ret)
		return handle_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event\n");

	printf("Disconnect RDMA cm id at %p \n", cm_event->id);
	rdma_ack_cm_event(cm_event);
}

/* 클라이언트 리소스를 초기화 */
static struct rdma_connected_client_resources *server_init_client_resources(struct rdma_cm_id *cm_id)
{
	struct rdma_connected_client_resources *client_res = calloc(1, sizeof(struct rdma_connected_client_resources));
	if (!client_res)
		return NULL;

	client_res->cm_client_id = cm_id;

	// 보호 도메인 할당
	client_res->pd = ibv_alloc_pd(cm_id->verbs);
	if (!client_res->pd)
	{
		free(client_res);
		return NULL;
	}

	// I/O 완료 채널 생성
	client_res->io_completion_channel = ibv_create_comp_channel(cm_id->verbs);
	if (!client_res->io_completion_channel)
	{
		ibv_dealloc_pd(client_res->pd);
		free(client_res);
		return NULL;
	}

	// 완료 큐 생성
	client_res->cq = ibv_create_cq(cm_id->verbs, CQ_CAPACITY, NULL, client_res->io_completion_channel, 0);
	if (!client_res->cq)
	{
		ibv_destroy_comp_channel(client_res->io_completion_channel);
		ibv_dealloc_pd(client_res->pd);
		free(client_res);
		return NULL;
	}

	// 완료 큐에 대한 알림 요청
	if (ibv_req_notify_cq(client_res->cq, 0))
	{
		ibv_destroy_cq(client_res->cq);
		ibv_destroy_comp_channel(client_res->io_completion_channel);
		ibv_dealloc_pd(client_res->pd);
		free(client_res);
		return NULL;
	}

	return client_res;
}

/* 서버 리소스를 초기화 */
static int server_prepare_connection(struct sockaddr_in *server_addr, struct rdma_server_resources *res)
{
	// 이벤트 채널 생성
	res->cm_event_channel = rdma_create_event_channel();
	if (!res->cm_event_channel)
		return handle_error("Failed to create event channel");

	// 서버 CM ID 생성
	if (rdma_create_id(res->cm_event_channel, &res->cm_server_id, NULL, RDMA_PS_TCP))
		return handle_error("Failed to create server cm id");

	// 서버 주소 바인딩
	if (rdma_bind_addr(res->cm_server_id, (struct sockaddr *)server_addr))
		return handle_error("Failed to bind server address");

	// 서버 리스닝 시작
	if (rdma_listen(res->cm_server_id, 8))
		return handle_error("Failed to listen on server address");

	printf("Server is listening at: %s, port: %d\n", inet_ntoa(server_addr->sin_addr), ntohs(server_addr->sin_port));
	return 0;
}

/* 메타데이터 전송 */
static int server_send_metadata(struct rdma_connected_client_resources *client_res, struct rdma_server_resources *res)
{
	printf("Send Metadata\n");

	client_res->server_buffer_mr = rdma_buffer_register(client_res->pd, &res->server_buffer, sizeof(res->server_buffer),
														IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
	if (!client_res->server_buffer_mr)
		return handle_error("Failed to register server buffer");

	struct rdma_buffer_attr server_metadata_attr = {
		.address = (uint64_t)client_res->server_buffer_mr->addr,
		.length = (uint32_t)client_res->server_buffer_mr->length,
		.stag.local_stag = (uint32_t)client_res->server_buffer_mr->lkey,
	};

	struct ibv_mr *server_metadata_mr = rdma_buffer_register(client_res->pd, &server_metadata_attr, sizeof(server_metadata_attr), IBV_ACCESS_LOCAL_WRITE);
	if (!server_metadata_mr)
		return handle_error("Failed to register server metadata");

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
	if (ibv_post_send(client_res->qp, &send_wr, &bad_send_wr))
		return handle_error("Failed to post send WR");

	struct ibv_wc wc;
	if (process_work_completion_events(client_res->io_completion_channel, &wc, 1) != 1)
		return handle_error("Failed to send server metadata");

	rdma_buffer_deregister(server_metadata_mr);
	return 0;
}

/* 클라이언트 연결 및 메타데이터 전송 */
static int server_accept_client(struct rdma_server_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_CONNECT_REQUEST, &cm_event);
	if (ret)
		return handle_error("Failed to get RDMA_CM_EVENT_CONNECT_REQUEST");

	printf("Connect RDMA cm id at %p\n", cm_event->id);

	struct rdma_connected_client_resources *client_res = server_init_client_resources(cm_event->id);
	if (!client_res)
		return handle_error("Failed to initialize client resources");

	rdma_ack_cm_event(cm_event);

	struct ibv_qp_init_attr qp_init_attr = {
		.cap = {
			.max_recv_sge = MAX_SGE,
			.max_recv_wr = MAX_WR,
			.max_send_sge = MAX_SGE,
			.max_send_wr = MAX_WR,
		},
		.qp_type = IBV_QPT_RC,
		.recv_cq = client_res->cq,
		.send_cq = client_res->cq,
	};

	if (rdma_create_qp(client_res->cm_client_id, client_res->pd, &qp_init_attr))
		return handle_error("Failed to create QP");

	client_res->qp = client_res->cm_client_id->qp;

	struct rdma_conn_param conn_param = {
		.initiator_depth = 3,
		.responder_resources = 3,
	};

	if (rdma_accept(client_res->cm_client_id, &conn_param))
		return handle_error("Failed to accept client connection");

	struct rdma_cm_event *cm_event2 = NULL;
	if (process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event2))
		return handle_error("Failed to get RDMA_CM_EVENT_ESTABLISHED");

	rdma_ack_cm_event(cm_event2);
	printf("Finish Connect RDMA cm id at %p\n", cm_event2->id);

	ret = server_send_metadata(client_res, res);
	if (ret)
		return ret;

	res->client_res = realloc(res->client_res, sizeof(struct rdma_connected_client_resources *) * (res->num_clients + 1));
	if (!res->client_res)
		return handle_error("Failed to realloc client resources");

	res->client_res[res->num_clients] = client_res;
	res->num_clients++;

	printf("New client connected. Total clients: %d\n", res->num_clients);
	return 0;
}

/* 서버 종료 및 클라이언트 리소스 해제 */
static int server_cleanup(struct rdma_server_resources *res)
{
	printf("Start Server shutdown\n");

	for (int i = 0; i < res->num_clients; i++)
	{
		struct rdma_connected_client_resources *client_res = res->client_res[i];

		rdma_destroy_qp(client_res->cm_client_id);
		rdma_destroy_id(client_res->cm_client_id);
		ibv_destroy_cq(client_res->cq);
		rdma_buffer_deregister(client_res->server_buffer_mr);
		ibv_dealloc_pd(client_res->pd);
		ibv_destroy_comp_channel(client_res->io_completion_channel);
		free(client_res);
	}

	rdma_destroy_id(res->cm_server_id);
	rdma_destroy_event_channel(res->cm_event_channel);
	free(res->client_res);

	printf("Server shutdown complete\n");
	return 0;
}

/* Main function */
int main(int argc, char **argv)
{
	if (argc < 2)
	{
		fprintf(stderr, "Usage: %s <text_to_send>\n", argv[0]);
		return -1;
	}

	struct rdma_server_resources res;
	memset(&res, 0, sizeof(res));

	// 서버 주소 초기화
	struct sockaddr_in server_sockaddr;
	memset(&server_sockaddr, 0, sizeof(server_sockaddr));
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = inet_addr("10.10.16.51");
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

	const char *text_to_send = argv[1];
	strncpy(res.server_buffer.str, text_to_send, strlen(text_to_send));

	// RDMA 서버 시작
	if (server_prepare_connection(&server_sockaddr, &res))
		return handle_error("Failed to start RDMA server");

	// 클라이언트 연결 처리
	for (int i = 0; i < 2; i++)
	{
		if (server_accept_client(&res))
			return handle_error("Failed to accept client");
	}

	for (int i = 0; i < 2; i++)
	{
		server_disconnect_wait(&res);
	}
	// 클라이언트가 작성한 데이터를 출력
	printf("Client wrote the following data: %ld, %s\n", res.server_buffer.flag, res.server_buffer.str);

	// 서버 종료 및 리소스 해제
	if (server_cleanup(&res))
		return handle_error("Failed to cleanup server resources");

	return 0;
}