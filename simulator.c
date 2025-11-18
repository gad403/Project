#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <inttypes.h>

#define BILLION 1000000000L
#define MAX_PDUS 100000   // number of PDUs to simulate
#define HEADER_SIZE 8

// ------------------- Utility: timing -------------------
static inline uint64_t nsec_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * BILLION + ts.tv_nsec;
}

// ------------------- PDU Header -------------------
typedef struct {
    int16_t msg_len;
    int16_t stream_id;
    int32_t seq_num;
} pdu_header_t;

// ------------------- PDU Body -------------------
typedef struct {
    char msg_type;
    int64_t ts;
    double order_id_1;
    double order_id_2;  // only valid if msg_type == 'T'
    int32_t token;
    char order_side;
    int32_t price;
    int32_t qty;
} decoded_pdu_t;

// ------------------- Decode Helpers -------------------
static inline int16_t be16_to_cpu(const uint8_t *p)
{ return (p[0] << 8) | p[1]; }

static inline int32_t be32_to_cpu(const uint8_t *p)
{ return (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]; }

static inline int64_t be64_to_cpu(const uint8_t *p)
{
    return ((int64_t)p[0] << 56) | ((int64_t)p[1] << 48) |
           ((int64_t)p[2] << 40) | ((int64_t)p[3] << 32) |
           ((int64_t)p[4] << 24) | ((int64_t)p[5] << 16) |
           ((int64_t)p[6] << 8)  | (int64_t)p[7];
}

// ------------------- Decode Function -------------------
int decode_packet(const uint8_t *buf, size_t len, decoded_pdu_t *msg)
{
    if (len < HEADER_SIZE + 1)
        return -1;

    size_t off = 0;
    pdu_header_t hdr;
    hdr.msg_len   = be16_to_cpu(buf + off); off += 2;
    hdr.stream_id = be16_to_cpu(buf + off); off += 2;
    hdr.seq_num   = be32_to_cpu(buf + off); off += 4;

    msg->msg_type = buf[off++];

    if (msg->msg_type != 'T' && len < off + 8 + 8 + 4 + 1 + 4 + 4)
        return -2;
    if (msg->msg_type == 'T' && len < off + 8 + 8 + 8 + 4 + 4 + 4)
        return -3;

    msg->ts = be64_to_cpu(buf + off); off += 8;

    msg->order_id_1 = 0;
    msg->order_id_2 = 0;

    if (msg->msg_type != 'T') {
        memcpy(&msg->order_id_1, buf + off, 8); off += 8;
        msg->token = be32_to_cpu(buf + off); off += 4;
        msg->order_side = buf[off++];
        msg->price = be32_to_cpu(buf + off); off += 4;
        msg->qty   = be32_to_cpu(buf + off); off += 4;
    } else {
        memcpy(&msg->order_id_1, buf + off, 8); off += 8;
        memcpy(&msg->order_id_2, buf + off, 8); off += 8;
        msg->token = be32_to_cpu(buf + off); off += 4;
        msg->price = be32_to_cpu(buf + off); off += 4;
        msg->qty   = be32_to_cpu(buf + off); off += 4;
    }

    return hdr.msg_len;
}

// ------------------- Metrics -------------------
typedef struct {
    uint64_t pkt_no;
    double latency_ns;
    double throughput_pps;
} metric_sample_t;

metric_sample_t samples[MAX_PDUS];

// ------------------- Feed Simulation -------------------
size_t make_random_pdu(uint8_t *buf, int seq)
{
    static const char types[] = {'N', 'M', 'X', 'T'};
    char t = types[rand() % 4];
    size_t body_len = (t == 'T') ? (1 + 8 + 8 + 8 + 4 + 4 + 4)
                                 : (1 + 8 + 8 + 4 + 1 + 4 + 4);
    int16_t total_len = HEADER_SIZE + body_len;

    // Header
    buf[0] = (total_len >> 8) & 0xFF;
    buf[1] = (total_len & 0xFF);
    buf[2] = 0x00;
    buf[3] = 0x01;
    buf[4] = (seq >> 24) & 0xFF;
    buf[5] = (seq >> 16) & 0xFF;
    buf[6] = (seq >> 8) & 0xFF;
    buf[7] = seq & 0xFF;

    size_t off = 8;
    buf[off++] = t;

    int64_t ts = nsec_now() - (rand() % 100000); // pseudo timestamp
    for (int i = 7; i >= 0; i--) { buf[off++] = (ts >> (i * 8)) & 0xFF; }

    double oid1 = (double)(rand() % 1000000);
    double oid2 = (double)(rand() % 1000000);
    memcpy(buf + off, &oid1, 8); off += 8;
    if (t == 'T') { memcpy(buf + off, &oid2, 8); off += 8; }

    int32_t token = rand() % 1000;
    buf[off++] = (token >> 24) & 0xFF;
    buf[off++] = (token >> 16) & 0xFF;
    buf[off++] = (token >> 8) & 0xFF;
    buf[off++] = (token & 0xFF);

    if (t != 'T') {
        buf[off++] = (rand() % 2) ? 'B' : 'S';
    }

    int32_t price = 1000 + (rand() % 500);
    int32_t qty   = 10 + (rand() % 100);

    buf[off++] = (price >> 24) & 0xFF;
    buf[off++] = (price >> 16) & 0xFF;
    buf[off++] = (price >> 8) & 0xFF;
    buf[off++] = (price & 0xFF);

    buf[off++] = (qty >> 24) & 0xFF;
    buf[off++] = (qty >> 16) & 0xFF;
    buf[off++] = (qty >> 8) & 0xFF;
    buf[off++] = (qty & 0xFF);

    return total_len;
}

// ------------------- Main -------------------
int main(void)
{
    srand(time(NULL));

    uint8_t buf[256];
    decoded_pdu_t msg;
    uint64_t start = nsec_now();

    for (int i = 0; i < MAX_PDUS; i++) {
        size_t n = make_random_pdu(buf, i + 1);

        uint64_t t1 = nsec_now();
        decode_packet(buf, n, &msg);
        uint64_t t2 = nsec_now();

        double latency_ns = (double)(t2 - t1);
        double elapsed_s = (double)(t2 - start) / BILLION;
        double throughput = (i + 1) / elapsed_s;

        samples[i].pkt_no = i + 1;
        samples[i].latency_ns = latency_ns;
        samples[i].throughput_pps = throughput;
    }

    FILE *out = fopen("metrics.csv", "w");
    fprintf(out, "packet,latency_ns,throughput_pps\n");
    for (int i = 0; i < MAX_PDUS; i++)
        fprintf(out, "%" PRIu64 ",%.2f,%.2f\n",
                samples[i].pkt_no, samples[i].latency_ns, samples[i].throughput_pps);
    fclose(out);

    printf("Simulation complete. Metrics dumped to metrics.csv\n");
    return 0;
}
