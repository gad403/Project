#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#define MAX_STREAMS 5
#define SCRIPS_PER_STREAM 10
#define MAX_SCRIPS 50
#define REORDER_BUFFER_SIZE 2048
#define ORDER_INDEX_SIZE 100003
#define SNAPSHOT_FILE "orderbook_snapshot.json"


//gcc -O2 -Wall -Wextra orderbook_engine_benchmark.c -o orderbook_benchmark ./orderbook_benchmark 10000 (for running on WSL)
//gcc -O2 -Wall -Wextra orderbook_engine_benchmark.c -o orderbook_benchmark ./orderbook_benchmark 10000 (for resource usage)
//./orderbook_benchmark 1000
//./orderbook_benchmark 2500
//./orderbook_benchmark 5000
//./orderbook_benchmark 10000
//./orderbook_benchmark 20000  (multiple benchmark sizes, 50 cycles each, to see how performance scales with load)

typedef struct {
    char msg_typ;
    short stream_id;
    int seq_no;
    long timestamp;
    double order_id;
    double buy_order_id;
    double sell_order_id;
    int token;
    char buy_sell;
    int price;
    int qty;
    int trade_price;
    int trade_qty;
} TickEvent;

typedef struct Order {
    double order_id;
    int token;
    char side;
    int price;
    int qty;
    long timestamp;
    struct Order *next;
    struct Order *prev;
} Order;

typedef struct PriceLevel {
    int price;
    int total_qty;
    int order_count;
    Order *head;
    Order *tail;
    struct PriceLevel *next;
    struct PriceLevel *prev;
} PriceLevel;

typedef struct {
    int token;
    char symbol[32];
    PriceLevel *buy_levels;
    PriceLevel *sell_levels;
} OrderBook;

typedef struct OrderIndexNode {
    double order_id;
    Order *order_ptr;
    struct OrderIndexNode *next;
} OrderIndexNode;

typedef struct {
    int expected_seq_no;
    TickEvent buffer[REORDER_BUFFER_SIZE];
    int occupied[REORDER_BUFFER_SIZE];
} StreamState;

typedef struct {
    long long total_received;
    long long total_processed;

    long long duplicate_discarded;
    long long out_of_seq_buffered;
    long long buffered_replayed;

    long long new_count;
    long long modify_count;
    long long cancel_count;
    long long trade_count;

    long long matched_events;
    long long matched_qty;
    long long active_orders;

    long long snapshots_written;
    long long total_snapshot_ns;
    long long max_snapshot_ns;

    long long total_latency_ns;
    long long min_latency_ns;
    long long max_latency_ns;
} BenchmarkStats;

OrderBook books[MAX_SCRIPS];
StreamState streams[MAX_STREAMS + 1];
OrderIndexNode *order_index[ORDER_INDEX_SIZE];
BenchmarkStats stats;

const char *symbols[MAX_SCRIPS] = {
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY",
    "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
    "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "SUNPHARMA",
    "TITAN", "ULTRACEMCO", "BAJFINANCE", "WIPRO", "ONGC",
    "NTPC", "POWERGRID", "TATAMOTORS", "TATASTEEL", "JSWSTEEL",
    "ADANIENT", "ADANIPORTS", "COALINDIA", "HCLTECH", "TECHM",
    "NESTLEIND", "BRITANNIA", "CIPLA", "DRREDDY", "DIVISLAB",
    "GRASIM", "HEROMOTOCO", "BAJAJ-AUTO", "EICHERMOT", "M&M",
    "BPCL", "IOC", "HDFCLIFE", "SBILIFE", "BAJAJFINSV",
    "INDUSINDBK", "APOLLOHOSP", "TATACONSUM", "UPL", "SHREECEM"
};

static long long now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int token_to_index(int token) {
    int idx = token - 1001;
    if (idx < 0 || idx >= MAX_SCRIPS) return -1;
    return idx;
}

static unsigned hash_order_id(double order_id) {
    long long x = (long long)order_id;
    if (x < 0) x = -x;
    return (unsigned)(x % ORDER_INDEX_SIZE);
}

Order *find_order(double order_id) {
    unsigned h = hash_order_id(order_id);
    OrderIndexNode *cur = order_index[h];

    while (cur) {
        if ((long long)cur->order_id == (long long)order_id) {
            return cur->order_ptr;
        }
        cur = cur->next;
    }

    return NULL;
}

void add_order_to_index(Order *order) {
    unsigned h = hash_order_id(order->order_id);

    OrderIndexNode *node = (OrderIndexNode *)malloc(sizeof(OrderIndexNode));
    if (!node) {
        perror("malloc");
        exit(1);
    }

    node->order_id = order->order_id;
    node->order_ptr = order;
    node->next = order_index[h];
    order_index[h] = node;

    stats.active_orders++;
}

void remove_order_from_index(double order_id) {
    unsigned h = hash_order_id(order_id);
    OrderIndexNode *cur = order_index[h];
    OrderIndexNode *prev = NULL;

    while (cur) {
        if ((long long)cur->order_id == (long long)order_id) {
            if (prev) {
                prev->next = cur->next;
            } else {
                order_index[h] = cur->next;
            }

            free(cur);
            stats.active_orders--;
            return;
        }

        prev = cur;
        cur = cur->next;
    }
}

PriceLevel *create_price_level(int price) {
    PriceLevel *level = (PriceLevel *)calloc(1, sizeof(PriceLevel));
    if (!level) {
        perror("calloc");
        exit(1);
    }

    level->price = price;
    return level;
}

Order *create_order(TickEvent *event) {
    Order *order = (Order *)calloc(1, sizeof(Order));
    if (!order) {
        perror("calloc");
        exit(1);
    }

    order->order_id = event->order_id;
    order->token = event->token;
    order->side = event->buy_sell;
    order->price = event->price;
    order->qty = event->qty;
    order->timestamp = event->timestamp;

    return order;
}

PriceLevel *find_price_level(PriceLevel *head, int price) {
    PriceLevel *cur = head;

    while (cur) {
        if (cur->price == price) {
            return cur;
        }
        cur = cur->next;
    }

    return NULL;
}

void insert_price_level_sorted(PriceLevel **head, PriceLevel *level, char side) {
    if (*head == NULL) {
        *head = level;
        return;
    }

    PriceLevel *cur = *head;

    if ((side == 'B' && level->price > cur->price) ||
        (side == 'S' && level->price < cur->price)) {
        level->next = cur;
        cur->prev = level;
        *head = level;
        return;
    }

    while (cur->next) {
        if ((side == 'B' && level->price > cur->next->price) ||
            (side == 'S' && level->price < cur->next->price)) {
            break;
        }
        cur = cur->next;
    }

    level->next = cur->next;

    if (cur->next) {
        cur->next->prev = level;
    }

    cur->next = level;
    level->prev = cur;
}

void remove_price_level(OrderBook *book, PriceLevel *level, char side) {
    PriceLevel **head;

    if (side == 'B') {
        head = &book->buy_levels;
    } else {
        head = &book->sell_levels;
    }

    if (level->prev) {
        level->prev->next = level->next;
    } else {
        *head = level->next;
    }

    if (level->next) {
        level->next->prev = level->prev;
    }

    free(level);
}

void append_order_to_level(PriceLevel *level, Order *order) {
    if (!level->tail) {
        level->head = order;
        level->tail = order;
    } else {
        level->tail->next = order;
        order->prev = level->tail;
        level->tail = order;
    }

    level->total_qty += order->qty;
    level->order_count++;
}

void insert_passive_order(OrderBook *book, TickEvent *event) {
    PriceLevel **side_head;

    if (event->buy_sell == 'B') {
        side_head = &book->buy_levels;
    } else {
        side_head = &book->sell_levels;
    }

    PriceLevel *level = find_price_level(*side_head, event->price);

    if (!level) {
        level = create_price_level(event->price);
        insert_price_level_sorted(side_head, level, event->buy_sell);
    }

    Order *order = create_order(event);
    append_order_to_level(level, order);
    add_order_to_index(order);
}

void unlink_order_from_level(OrderBook *book, PriceLevel *level, Order *order) {
    if (order->prev) {
        order->prev->next = order->next;
    } else {
        level->head = order->next;
    }

    if (order->next) {
        order->next->prev = order->prev;
    } else {
        level->tail = order->prev;
    }

    level->total_qty -= order->qty;
    level->order_count--;

    char side = order->side;

    remove_order_from_index(order->order_id);
    free(order);

    if (level->order_count == 0) {
        remove_price_level(book, level, side);
    }
}

PriceLevel *find_level_containing_order(OrderBook *book, Order *order) {
    PriceLevel *cur;

    if (order->side == 'B') {
        cur = book->buy_levels;
    } else {
        cur = book->sell_levels;
    }

    while (cur) {
        Order *o = cur->head;

        while (o) {
            if (o == order) {
                return cur;
            }

            o = o->next;
        }

        cur = cur->next;
    }

    return NULL;
}

void match_active_order(OrderBook *book, TickEvent *event) {
    if (event->buy_sell == 'B') {
        while (event->qty > 0 && book->sell_levels) {
            PriceLevel *best_sell = book->sell_levels;

            if (event->price < best_sell->price) {
                break;
            }

            Order *passive = best_sell->head;
            int traded_qty;

            if (event->qty < passive->qty) {
                traded_qty = event->qty;
            } else {
                traded_qty = passive->qty;
            }

            event->qty -= traded_qty;
            passive->qty -= traded_qty;
            best_sell->total_qty -= traded_qty;

            stats.matched_events++;
            stats.matched_qty += traded_qty;

            if (passive->qty == 0) {
                unlink_order_from_level(book, best_sell, passive);
            }
        }
    } else if (event->buy_sell == 'S') {
        while (event->qty > 0 && book->buy_levels) {
            PriceLevel *best_buy = book->buy_levels;

            if (event->price > best_buy->price) {
                break;
            }

            Order *passive = best_buy->head;
            int traded_qty;

            if (event->qty < passive->qty) {
                traded_qty = event->qty;
            } else {
                traded_qty = passive->qty;
            }

            event->qty -= traded_qty;
            passive->qty -= traded_qty;
            best_buy->total_qty -= traded_qty;

            stats.matched_events++;
            stats.matched_qty += traded_qty;

            if (passive->qty == 0) {
                unlink_order_from_level(book, best_buy, passive);
            }
        }
    }
}

void handle_new_order(TickEvent *event) {
    int idx = token_to_index(event->token);

    if (idx < 0 || event->qty <= 0) {
        return;
    }

    OrderBook *book = &books[idx];

    match_active_order(book, event);

    if (event->qty > 0) {
        insert_passive_order(book, event);
    }
}

void handle_modify_order(TickEvent *event) {
    int idx = token_to_index(event->token);

    if (idx < 0 || event->qty <= 0) {
        return;
    }

    OrderBook *book = &books[idx];
    Order *old = find_order(event->order_id);

    if (old) {
        int old_idx = token_to_index(old->token);

        if (old_idx >= 0) {
            OrderBook *old_book = &books[old_idx];
            PriceLevel *level = find_level_containing_order(old_book, old);

            if (level) {
                unlink_order_from_level(old_book, level, old);
            }
        }
    }

    match_active_order(book, event);

    if (event->qty > 0) {
        insert_passive_order(book, event);
    }
}

void handle_cancel_order(TickEvent *event) {
    Order *order = find_order(event->order_id);

    if (!order) {
        return;
    }

    int idx = token_to_index(order->token);

    if (idx < 0) {
        return;
    }

    OrderBook *book = &books[idx];
    PriceLevel *level = find_level_containing_order(book, order);

    if (level) {
        unlink_order_from_level(book, level, order);
    }
}

void reduce_order_qty(double order_id, int trade_qty) {
    Order *order = find_order(order_id);

    if (!order) {
        return;
    }

    int idx = token_to_index(order->token);

    if (idx < 0) {
        return;
    }

    OrderBook *book = &books[idx];
    PriceLevel *level = find_level_containing_order(book, order);

    if (!level) {
        return;
    }

    int reduce_qty = trade_qty;

    if (reduce_qty > order->qty) {
        reduce_qty = order->qty;
    }

    order->qty -= reduce_qty;
    level->total_qty -= reduce_qty;

    if (order->qty <= 0) {
        if (order->prev) {
            order->prev->next = order->next;
        } else {
            level->head = order->next;
        }

        if (order->next) {
            order->next->prev = order->prev;
        } else {
            level->tail = order->prev;
        }

        level->order_count--;

        char side = order->side;

        remove_order_from_index(order->order_id);
        free(order);

        if (level->order_count == 0) {
            remove_price_level(book, level, side);
        }
    }
}

void handle_trade(TickEvent *event) {
    if (event->trade_qty <= 0) {
        return;
    }

    reduce_order_qty(event->buy_order_id, event->trade_qty);
    reduce_order_qty(event->sell_order_id, event->trade_qty);
}

void process_tick(TickEvent *event) {
    switch (event->msg_typ) {
        case 'N':
            stats.new_count++;
            handle_new_order(event);
            break;

        case 'M':
            stats.modify_count++;
            handle_modify_order(event);
            break;

        case 'X':
            stats.cancel_count++;
            handle_cancel_order(event);
            break;

        case 'T':
            stats.trade_count++;
            handle_trade(event);
            break;

        default:
            break;
    }
}

void timed_process_tick(TickEvent *event) {
    long long start_ns = now_ns();

    process_tick(event);

    long long end_ns = now_ns();
    long long latency = end_ns - start_ns;

    stats.total_processed++;
    stats.total_latency_ns += latency;

    if (latency < stats.min_latency_ns) {
        stats.min_latency_ns = latency;
    }

    if (latency > stats.max_latency_ns) {
        stats.max_latency_ns = latency;
    }
}

void handle_sequence(TickEvent event) {
    if (event.stream_id < 1 || event.stream_id > MAX_STREAMS) {
        return;
    }

    stats.total_received++;

    StreamState *s = &streams[event.stream_id];

    if (event.seq_no < s->expected_seq_no) {
        stats.duplicate_discarded++;
        return;
    }

    if (event.seq_no > s->expected_seq_no) {
        int pos = event.seq_no % REORDER_BUFFER_SIZE;
        s->buffer[pos] = event;
        s->occupied[pos] = 1;
        stats.out_of_seq_buffered++;
        return;
    }

    timed_process_tick(&event);
    s->expected_seq_no++;

    while (1) {
        int pos = s->expected_seq_no % REORDER_BUFFER_SIZE;

        if (!s->occupied[pos]) {
            break;
        }

        if (s->buffer[pos].seq_no != s->expected_seq_no) {
            break;
        }

        TickEvent buffered = s->buffer[pos];
        s->occupied[pos] = 0;

        stats.buffered_replayed++;
        timed_process_tick(&buffered);

        s->expected_seq_no++;
    }
}

TickEvent make_order_event(char type, short stream_id, int seq_no, long ts,
                           double order_id, int token, char side, int price, int qty) {
    TickEvent e;
    memset(&e, 0, sizeof(e));

    e.msg_typ = type;
    e.stream_id = stream_id;
    e.seq_no = seq_no;
    e.timestamp = ts;
    e.order_id = order_id;
    e.token = token;
    e.buy_sell = side;
    e.price = price;
    e.qty = qty;

    return e;
}

TickEvent make_trade_event(short stream_id, int seq_no, long ts,
                           double buy_order_id, double sell_order_id,
                           int token, int price, int qty) {
    TickEvent e;
    memset(&e, 0, sizeof(e));

    e.msg_typ = 'T';
    e.stream_id = stream_id;
    e.seq_no = seq_no;
    e.timestamp = ts;
    e.buy_order_id = buy_order_id;
    e.sell_order_id = sell_order_id;
    e.token = token;
    e.trade_price = price;
    e.trade_qty = qty;

    return e;
}

void write_levels(FILE *fp, PriceLevel *level, int max_depth) {
    int count = 0;

    while (level && count < max_depth) {
        fprintf(fp,
                "      {\"price\": %.2f, \"qty\": %d, \"orders\": %d}%s\n",
                level->price / 100.0,
                level->total_qty,
                level->order_count,
                (level->next && count + 1 < max_depth) ? "," : "");

        level = level->next;
        count++;
    }
}

void write_json_snapshot(void) {
    long long start_ns = now_ns();

    FILE *fp = fopen(SNAPSHOT_FILE, "w");

    if (!fp) {
        perror("snapshot open failed");
        return;
    }

    fprintf(fp, "{\n  \"books\": [\n");

    for (int i = 0; i < MAX_SCRIPS; i++) {
        fprintf(fp, "    {\n");
        fprintf(fp, "      \"symbol\": \"%s\",\n", books[i].symbol);
        fprintf(fp, "      \"token\": %d,\n", books[i].token);
        fprintf(fp, "      \"bids\": [\n");

        write_levels(fp, books[i].buy_levels, 5);

        fprintf(fp, "      ],\n");
        fprintf(fp, "      \"asks\": [\n");

        write_levels(fp, books[i].sell_levels, 5);

        fprintf(fp, "      ]\n");
        fprintf(fp, "    }%s\n", (i == MAX_SCRIPS - 1) ? "" : ",");
    }

    fprintf(fp, "  ]\n}\n");
    fclose(fp);

    long long end_ns = now_ns();
    long long snapshot_ns = end_ns - start_ns;

    stats.snapshots_written++;
    stats.total_snapshot_ns += snapshot_ns;

    if (snapshot_ns > stats.max_snapshot_ns) {
        stats.max_snapshot_ns = snapshot_ns;
    }
}

void init_books(void) {
    for (int i = 0; i < MAX_SCRIPS; i++) {
        books[i].token = 1001 + i;
        strncpy(books[i].symbol, symbols[i], sizeof(books[i].symbol) - 1);
        books[i].symbol[sizeof(books[i].symbol) - 1] = '\0';
        books[i].buy_levels = NULL;
        books[i].sell_levels = NULL;
    }

    for (int s = 1; s <= MAX_STREAMS; s++) {
        streams[s].expected_seq_no = 1;
    }

    stats.min_latency_ns = 9223372036854775807LL;
}

void simulate_initial_ticks(void) {
    long ts = 1000000000L;

    TickEvent e1 = make_order_event('N', 1, 1, ts++, 1000001, 1001, 'B', 10050, 500);
    TickEvent e2 = make_order_event('N', 1, 2, ts++, 1000002, 1001, 'B', 10045, 300);
    TickEvent e3 = make_order_event('N', 1, 3, ts++, 1000003, 1001, 'S', 10070, 400);
    TickEvent e4 = make_order_event('N', 1, 4, ts++, 1000004, 1001, 'S', 10080, 600);
    TickEvent e5 = make_order_event('N', 1, 5, ts++, 1000005, 1001, 'B', 10075, 250);
    TickEvent e6 = make_order_event('M', 1, 6, ts++, 1000002, 1001, 'B', 10055, 700);
    TickEvent e7 = make_order_event('X', 1, 7, ts++, 1000004, 1001, 'S', 0, 0);

    TickEvent duplicate = e7;
    TickEvent e9 = make_order_event('N', 1, 9, ts++, 1000009, 1001, 'S', 10100, 350);
    TickEvent e8 = make_order_event('N', 1, 8, ts++, 1000008, 1001, 'B', 10040, 150);

    handle_sequence(e1);
    handle_sequence(e2);
    handle_sequence(e3);
    handle_sequence(e4);
    handle_sequence(e5);
    handle_sequence(e6);
    handle_sequence(e7);
    handle_sequence(duplicate);
    handle_sequence(e9);
    handle_sequence(e8);
}

int best_bid_price(OrderBook *book) {
    if (!book->buy_levels) {
        return 0;
    }

    return book->buy_levels->price;
}

int best_ask_price(OrderBook *book) {
    if (!book->sell_levels) {
        return 0;
    }

    return book->sell_levels->price;
}

void run_benchmark(int cycles) {
    int next_seq[MAX_STREAMS + 1] = {0};
    double next_order_id = 9000000;
    long timestamp_ns = 5000000000L;

    for (int s = 1; s <= MAX_STREAMS; s++) {
        next_seq[s] = streams[s].expected_seq_no;
    }

    for (int cycle = 0; cycle < cycles; cycle++) {
        for (int i = 0; i < MAX_SCRIPS; i++) {
            int token = 1001 + i;
            short stream_id = (short)((i / SCRIPS_PER_STREAM) + 1);

            OrderBook *book = &books[i];

            int bid = best_bid_price(book);
            int ask = best_ask_price(book);

            int mid;

            if (bid > 0 && ask > 0) {
                mid = (bid + ask) / 2;
            } else {
                mid = 10000 + i * 25;
            }

            int event_selector = rand() % 100;
            TickEvent e;

            if (event_selector < 70) {
                char side;

                if (rand() % 2 == 0) {
                    side = 'B';
                } else {
                    side = 'S';
                }

                int price_shift = ((rand() % 11) - 5) * 5;
                int price = mid + price_shift;

                if (side == 'B' && ask > 0 && rand() % 4 == 0) {
                    price = ask;
                }

                if (side == 'S' && bid > 0 && rand() % 4 == 0) {
                    price = bid;
                }

                int qty = 50 + (rand() % 20) * 25;

                e = make_order_event(
                    'N',
                    stream_id,
                    next_seq[stream_id]++,
                    timestamp_ns++,
                    next_order_id++,
                    token,
                    side,
                    price,
                    qty
                );
            } else if (event_selector < 85) {
                char side;

                if (rand() % 2 == 0) {
                    side = 'B';
                } else {
                    side = 'S';
                }

                int price = mid + (((rand() % 7) - 3) * 5);
                int qty = 50 + (rand() % 10) * 25;

                e = make_order_event(
                    'M',
                    stream_id,
                    next_seq[stream_id]++,
                    timestamp_ns++,
                    next_order_id - 1,
                    token,
                    side,
                    price,
                    qty
                );
            } else if (event_selector < 95) {
                e = make_order_event(
                    'X',
                    stream_id,
                    next_seq[stream_id]++,
                    timestamp_ns++,
                    next_order_id - 1,
                    token,
                    'B',
                    0,
                    0
                );
            } else {
                e = make_trade_event(
                    stream_id,
                    next_seq[stream_id]++,
                    timestamp_ns++,
                    next_order_id - 2,
                    next_order_id - 3,
                    token,
                    mid,
                    50 + (rand() % 10) * 25
                );
            }

            handle_sequence(e);
        }

        if ((cycle + 1) % 50 == 0) {
            write_json_snapshot();
        }
    }
}

void print_summary(int cycles, long long elapsed_ns) {
    double elapsed_sec = elapsed_ns / 1000000000.0;
    double throughput = stats.total_processed / elapsed_sec;
    double avg_latency_us = (stats.total_processed > 0)
                            ? (stats.total_latency_ns / (double)stats.total_processed) / 1000.0
                            : 0.0;

    double min_latency_us = stats.min_latency_ns / 1000.0;
    double max_latency_us = stats.max_latency_ns / 1000.0;

    double avg_snapshot_us = (stats.snapshots_written > 0)
                             ? (stats.total_snapshot_ns / (double)stats.snapshots_written) / 1000.0
                             : 0.0;

    double max_snapshot_us = stats.max_snapshot_ns / 1000.0;

    printf("\n\nPERFORMANCE SUMMARY\n");
    printf("Cycles: %d\n", cycles);
    printf("Total received: %lld\n", stats.total_received);
    printf("Total processed: %lld\n", stats.total_processed);
    printf("Elapsed time: %.6f s\n", elapsed_sec);
    printf("Throughput: %.2f messages/sec\n", throughput);
    printf("Average latency: %.3f us\n", avg_latency_us);
    printf("Min latency: %.3f us\n", min_latency_us);
    printf("Max latency: %.3f us\n", max_latency_us);
    printf("Snapshots written: %lld\n", stats.snapshots_written);
    printf("Avg snapshot write time: %.3f us\n", avg_snapshot_us);
    printf("Max snapshot write time: %.3f us\n", max_snapshot_us);
    printf("Duplicate discarded: %lld\n", stats.duplicate_discarded);
    printf("Out-of-seq buffered: %lld\n", stats.out_of_seq_buffered);
    printf("Buffered replayed: %lld\n", stats.buffered_replayed);
    printf("New/Modify/Cancel/Trade: %lld / %lld / %lld / %lld\n",
           stats.new_count,
           stats.modify_count,
           stats.cancel_count,
           stats.trade_count);
    printf("Matched events: %lld | Matched qty: %lld\n",
           stats.matched_events,
           stats.matched_qty);
    printf("Active orders tracked: %lld\n", stats.active_orders);
}

int main(int argc, char *argv[]) {
    int cycles = 10000;

    if (argc >= 2) {
        cycles = atoi(argv[1]);
        if (cycles <= 0) {
            cycles = 10000;
        }
    }

    srand(42);

    init_books();
    simulate_initial_ticks();

    long long start_ns = now_ns();

    run_benchmark(cycles);

    long long end_ns = now_ns();

    print_summary(cycles, end_ns - start_ns);

    return 0;
}