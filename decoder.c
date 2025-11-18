#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

static uint16_t read_le16(const unsigned char *buf) {
    return (uint16_t)(buf[0] | (buf[1] << 8));
}

static uint32_t read_le32(const unsigned char *buf) {
    return (uint32_t)(buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24));
}

static uint64_t read_le64(const unsigned char *buf) {
    return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) |
           ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
           ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
           ((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
}

static double read_le_double(const unsigned char *buf) {
    double val;
    memcpy(&val, buf, sizeof(double));
    return val;
}

#define STREAM_HEADER_SIZE 8 

void print_precise_timestamp(FILE *fout, int64_t timestamp) {
    time_t seconds = timestamp / 1000000000LL;
    long nsec_part = timestamp % 1000000000LL;
    if (nsec_part < 0) {
        nsec_part += 1000000000LL;
        seconds -= 1;
    }
    struct tm t;
    gmtime_r(&seconds, &t);

    fprintf(fout, "\t%04d-%02d-%02d %02d:%02d:%02d.%09ld\t",
        t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
        t.tm_hour, t.tm_min, t.tm_sec,
        nsec_part);
}

void print_order_message(FILE *fout, const unsigned char *buffer) {
    char message_type = buffer[0];
    int64_t timestamp = (int64_t)read_le64(buffer + 1);
    double order_id = read_le_double(buffer + 9);
    int32_t token = (int32_t)read_le32(buffer + 17);
    char order_type = buffer[21];
    int32_t price = (int32_t)read_le32(buffer + 22);
    int32_t quantity = (int32_t)read_le32(buffer + 26);

    fprintf(fout,
        "%c",
        message_type);
    print_precise_timestamp(fout, timestamp);
    fprintf(fout,
        "%.0f\t%d\t%c\t%d\t%d\n",
        order_id,
        token,
        order_type,
        price,
        quantity);
}

void print_trade_message(FILE *fout, const unsigned char *buffer) {
    char message_type = buffer[0];
    int64_t timestamp = (int64_t)read_le64(buffer + 1);
    double buy_order_id = read_le_double(buffer + 9);
    double sell_order_id = read_le_double(buffer + 17);
    int32_t token = (int32_t)read_le32(buffer + 25);
    int32_t trade_price = (int32_t)read_le32(buffer + 29);
    int32_t trade_qty = (int32_t)read_le32(buffer + 33);


    fprintf(fout,
        "%c",
        message_type);
    print_precise_timestamp(fout, timestamp);
    fprintf(fout,
        "%.0f\t%.0f\t%d\t%d\t%d\n",
        buy_order_id,
        sell_order_id,
        token,
        trade_price,
        trade_qty);
}

int main(void) {
    FILE *fin = fopen("ticks.encoded.little", "rb");
    FILE *fout = fopen("ticks_decoded.little", "w");
    if (!fin || !fout) {
        perror("File open failed");
        return 1;
    }

    while (1) {
        unsigned char header_buf[STREAM_HEADER_SIZE];
        if (fread(header_buf, 1, STREAM_HEADER_SIZE, fin) != STREAM_HEADER_SIZE) break;

        uint16_t msg_len = read_le16(header_buf);
        uint16_t stream_id = read_le16(header_buf + 2);
        uint32_t seq_no = read_le32(header_buf + 4);    
        fprintf(fout,
            "%u\t%d\t%u\t",
            msg_len, stream_id, seq_no);

        if (msg_len < STREAM_HEADER_SIZE + 1) {
            fprintf(stderr, "Invalid message length: %u\n", msg_len);
            break;
        }

        unsigned char *buffer = malloc(msg_len - STREAM_HEADER_SIZE);
        if (!buffer) {
            fprintf(stderr, "Memory allocation failed\n");
            break;
        }

        if (fread(buffer, 1, msg_len - STREAM_HEADER_SIZE, fin) != (size_t)(msg_len - STREAM_HEADER_SIZE)) {
            free(buffer);
            break;
        }

        char message_type = buffer[0];
        if (message_type == 'N' || message_type == 'X' || message_type == 'M') {
            print_order_message(fout, buffer);
        } else if (message_type == 'T') {
            print_trade_message(fout, buffer);
        } else {
            fprintf(stderr, "Unknown message type: %c\n", message_type);
        }

        free(buffer);
    }

    fclose(fin);
    fclose(fout);
    return 0;
}
