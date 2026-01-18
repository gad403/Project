#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

static void write_le16(FILE *f, uint16_t v) {
    unsigned char b[2];
    b[0] = (unsigned char)(v & 0xFF);
    b[1] = (unsigned char)((v >> 8) & 0xFF);
    fwrite(b, 1, 2, f);
}

static void write_le32(FILE *f, uint32_t v) {
    unsigned char b[4];
    b[0] = (unsigned char)(v & 0xFF);
    b[1] = (unsigned char)((v >> 8) & 0xFF);
    b[2] = (unsigned char)((v >> 16) & 0xFF);
    b[3] = (unsigned char)((v >> 24) & 0xFF);
    fwrite(b, 1, 4, f);
}

static void write_le64(FILE *f, uint64_t v) {
    unsigned char b[8];
    for (int i = 0; i < 8; i++) b[i] = (unsigned char)((v >> (8 * i)) & 0xFF);
    fwrite(b, 1, 8, f);
}

static void write_le_double(FILE *f, double d) {
    fwrite(&d, 1, 8, f);
}

static int64_t parse_timestamp(const char *ts) {
    struct tm t;
    memset(&t, 0, sizeof(t));

    int y, m, d, hh, mm, ss;
    long ns;

    if (sscanf(ts, "%d-%d-%d %d:%d:%d.%ld", &y, &m, &d, &hh, &mm, &ss, &ns) != 7)
        return 0;

    t.tm_year = y - 1900;
    t.tm_mon  = m - 1;
    t.tm_mday = d;
    t.tm_hour = hh;
    t.tm_min  = mm;
    t.tm_sec  = ss;

    time_t sec = timegm(&t);
    return (int64_t)sec * 1000000000LL + (int64_t)ns;
}

static int split_tabs(char *line, char *cols[], int max_cols) {
    int n = 0;
    char *p = line;
    while (n < max_cols) {
        cols[n++] = p;
        char *tab = strchr(p, '\t');
        if (!tab) break;
        *tab = '\0';
        p = tab + 1;
    }
    return n;
}

int main(void) {
    FILE *fin = fopen("ticks.decoded.little", "r");
    FILE *fout = fopen("ticks.encoded.little.new", "wb");
    if (!fin || !fout) {
        perror("File open failed");
        return 1;
    }

    char line[512];

    while (fgets(line, sizeof(line), fin)) {
        size_t len = strlen(line);
        while (len && (line[len - 1] == '\n' || line[len - 1] == '\r')) line[--len] = '\0';

        char *cols[16];
        int ncols = split_tabs(line, cols, 16);
        if (ncols < 5) continue;

        uint16_t msg_len  = (uint16_t)strtoul(cols[0], NULL, 10);
        uint16_t stream_id = (uint16_t)strtoul(cols[1], NULL, 10);
        uint32_t seq_no   = (uint32_t)strtoul(cols[2], NULL, 10);
        char msg_type     = cols[3][0];
        int64_t timestamp = parse_timestamp(cols[4]);

        write_le16(fout, msg_len);
        write_le16(fout, stream_id);
        write_le32(fout, seq_no);

        if (msg_type == 'N' || msg_type == 'X' || msg_type == 'M') {
            if (ncols < 10) return 2;

            double order_id = strtod(cols[5], NULL);
            int32_t token   = (int32_t)strtol(cols[6], NULL, 10);
            char side       = cols[7][0];
            int32_t price   = (int32_t)strtol(cols[8], NULL, 10);
            int32_t qty     = (int32_t)strtol(cols[9], NULL, 10);

            fputc(msg_type, fout);
            write_le64(fout, (uint64_t)timestamp);
            write_le_double(fout, order_id);
            write_le32(fout, (uint32_t)token);
            fputc(side, fout);
            write_le32(fout, (uint32_t)price);
            write_le32(fout, (uint32_t)qty);

        } else if (msg_type == 'T') {
            if (ncols < 10) return 3;

            double buy_id  = strtod(cols[5], NULL);
            double sell_id = strtod(cols[6], NULL);
            int32_t token  = (int32_t)strtol(cols[7], NULL, 10);
            int32_t price  = (int32_t)strtol(cols[8], NULL, 10);
            int32_t qty    = (int32_t)strtol(cols[9], NULL, 10);

            fputc('T', fout);
            write_le64(fout, (uint64_t)timestamp);
            write_le_double(fout, buy_id);
            write_le_double(fout, sell_id);
            write_le32(fout, (uint32_t)token);
            write_le32(fout, (uint32_t)price);
            write_le32(fout, (uint32_t)qty);
        }
    }

    fclose(fin);
    fclose(fout);
    return 0;
}
