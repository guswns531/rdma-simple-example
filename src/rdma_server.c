
#include "rdma_common.h"

#define SERVER_DATA_LEN 100000

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

	struct rdma_connected_client_resources **client_res; // 클라이언트 배열
	int num_clients;									 // 현재 연결된 클라이언트 수

	char *server_buffer;
};

static int server_disconnect_wait(struct rdma_server_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_DISCONNECTED, &cm_event);
	if (ret)
	{
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event\n");
		return ret;
	}
	rdma_ack_cm_event(cm_event);
}

/* 서버 종료 및 리소스 정리 */
static int server_cleanup(struct rdma_server_resources *res)
{
	server_disconnect_wait(res);
	// 클라이언트가 write한 데이터를 출력
	printf("Client wrote the following data: %s\n", (char *)res->server_buffer);

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
	free(res->server_buffer);
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

	return 0;
}

/* 서버 메타데이터를 클라이언트로 전송 */
static int server_accept_client_and_send_metadata(struct rdma_server_resources *res)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_CONNECT_REQUEST, &cm_event);
	if (ret)
	{
		rdma_error("Failed to get CM event\n");
		return ret;
	}
	printf("Server is get cm event at: ");
	show_rdma_cmid(cm_event->id);

	// 새 클라이언트 리소스 구조체 생성
	struct rdma_connected_client_resources *client_res = (struct rdma_connected_client_resources *)calloc(1, sizeof(struct rdma_connected_client_resources));
	if (!client_res)
	{
		rdma_error("Failed to allocate memory for client resources\n");
		return -ENOMEM;
	}

	client_res->cm_client_id = cm_event->id;
	rdma_ack_cm_event(cm_event);

	if (!client_res->cm_client_id)
	{
		rdma_error("Client id is NULL\n");
		return -EINVAL;
	}

	// 보호 도메인 할당
	client_res->pd = ibv_alloc_pd(client_res->cm_client_id->verbs);
	if (!client_res->pd)
	{
		rdma_error("Failed to allocate protection domain, errno: %d\n", -errno);
		return -errno;
	}

	// I/O 완료 채널 및 완료 큐 설정
	client_res->io_completion_channel = ibv_create_comp_channel(client_res->cm_client_id->verbs);
	if (!client_res->io_completion_channel)
	{
		rdma_error("Failed to create I/O completion event channel, errno: %d\n", -errno);
		return -errno;
	}

	client_res->cq = ibv_create_cq(client_res->cm_client_id->verbs, CQ_CAPACITY, NULL, client_res->io_completion_channel, 0);
	if (!client_res->cq)
	{
		rdma_error("Failed to create completion queue, errno: %d\n", -errno);
		return -errno;
	}

	if (ibv_req_notify_cq(client_res->cq, 0))
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
		.recv_cq = client_res->cq,
		.send_cq = client_res->cq,
	};

	if (rdma_create_qp(client_res->cm_client_id, client_res->pd, &qp_init_attr))
	{
		rdma_error("Failed to create QP, errno: %d\n", -errno);
		return -errno;
	}

	client_res->qp = client_res->cm_client_id->qp;

	struct rdma_conn_param conn_param = {
		.initiator_depth = 3,
		.responder_resources = 3,
	};
	if (rdma_accept(client_res->cm_client_id, &conn_param))
	{
		rdma_error("Failed to accept client connection, errno: %d\n", -errno);
		return -errno;
	}

	struct rdma_cm_event *cm_event2 = NULL;
	if (process_rdma_cm_event(res->cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event2))
	{
		rdma_error("Failed to get CM event\n");
		return -errno;
	}

	rdma_ack_cm_event(cm_event2);

	// 서버 server_buffer 연결
	client_res->server_buffer_mr = rdma_buffer_register(client_res->pd, res->server_buffer, SERVER_DATA_LEN, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

	struct rdma_buffer_attr server_metadata_attr = {
		.address = (uint64_t)client_res->server_buffer_mr->addr,
		.length = (uint32_t)client_res->server_buffer_mr->length,
		.stag.local_stag = (uint32_t)client_res->server_buffer_mr->lkey,
	};

	struct ibv_mr *server_metadata_mr = rdma_buffer_register(client_res->pd, &server_metadata_attr, sizeof(server_metadata_attr), IBV_ACCESS_LOCAL_WRITE);

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
	{
		rdma_error("Failed to post send WR, errno: %d\n", -errno);
		return -errno;
	}

	struct ibv_wc wc;
	if (process_work_completion_events(client_res->io_completion_channel, &wc, 1) != 1)
	{
		rdma_error("Failed to send server metadata\n");
		return -errno;
	}

	rdma_buffer_deregister(server_metadata_mr);

	// 클라이언트 배열에 새 클라이언트 추가
	res->client_res = realloc(res->client_res, sizeof(struct rdma_connected_client_resources *) * (res->num_clients + 1));
	res->client_res[res->num_clients] = client_res;
	res->num_clients++;

	printf("New client connected. Total clients: %d\n", res->num_clients);

	return 0;
}

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

	/* Allocate Server buffer*/
	res.server_buffer = calloc(1, SERVER_DATA_LEN);
	if (!res.server_buffer)
	{
		rdma_error("failed to allocate buffer, -ENOMEM\n");
		return ret;
	}

	/* Start RDMA server */
	ret = server_prepare_connection(&server_sockaddr, &res);
	if (ret)
	{
		rdma_error("Failed to start RDMA server\n");
		return ret;
	}

	/* Send server metadata */
	ret = server_accept_client_and_send_metadata(&res);
	if (ret)
	{
		rdma_error("Failed to send server metadata\n");
		return ret;
	}

	/* Disconnect and cleanup */
	ret = server_cleanup(&res);
	if (ret)
	{
		rdma_error("Failed to cleanup server resources\n");
		return ret;
	}

	return 0;
}