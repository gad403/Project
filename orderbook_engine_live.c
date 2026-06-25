#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

/*
 * Academic Order Book Engine - Live Version
 * Features:
 * - Packed NSE-like TBT protocol structs
 * - 5 streams x 10 scrips = 50 tokens
 * - Duplicate discard
 * - Out-of-sequence buffering and replay
 * - N, M, X, T tick handling
 * - Crossed active order matching
 * - Continuously updates JSON snapshot every 250 ms
 */

#define MAX_STREAMS 5
#define SCRIPS_PER_STREAM 10
#define MAX_SCRIPS 50
#define REORDER_BUFFER_SIZE 2048
#define ORDER_INDEX_SIZE 100003
#define SNAPSHOT_FILE "orderbook_snapshot.json"

#pragma pack(push, 1)

typedef struct {
    short msg_len;
    short stream_id;
    int seq_no;
} TickHeader;

typedef struct {
    TickHeader header;
    char msg_typ;
    long timestamp;
    double order_id;
    int token;
    char buy_sell;
    int price;
    int qty;
} OrderTick;

typedef struct {
    TickHeader header;
    char msg_typ;
    long timestamp;
    double buy_order_id;
    double sell_order_id;
    int token;
    int trade_price;
    int trade_qty;
} TradeTick;

#pragma pack(pop)

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

OrderBook books[MAX_SCRIPS];
StreamState streams[MAX_STREAMS + 1];
OrderIndexNode *order_index[ORDER_INDEX_SIZE];

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
        if ((long long)cur->order_id == (long long)order_id) return cur->order_ptr;
        cur = cur->next;
    }
    return NULL;
}

void add_order_to_index(Order *order) {
    unsigned h = hash_order_id(order->order_id);
    OrderIndexNode *node = (OrderIndexNode *)malloc(sizeof(OrderIndexNode));
    if (!node) exit(1);
    node->order_id = order->order_id;
    node->order_ptr = order;
    node->next = order_index[h];
    order_index[h] = node;
}

void remove_order_from_index(double order_id) {
    unsigned h = hash_order_id(order_id);
    OrderIndexNode *cur = order_index[h];
    OrderIndexNode *prev = NULL;

    while (cur) {
        if ((long long)cur->order_id == (long long)order_id) {
            if (prev) prev->next = cur->next;
            else order_index[h] = cur->next;
            free(cur);
            return;
        }
        prev = cur;
        cur = cur->next;
    }
}

PriceLevel *create_price_level(int price) {
    PriceLevel *level = (PriceLevel *)calloc(1, sizeof(PriceLevel));
    if (!level) exit(1);
    level->price = price;
    return level;
}

Order *create_order(TickEvent *event) {
    Order *order = (Order *)calloc(1, sizeof(Order));
    if (!order) exit(1);
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
        if (cur->price == price) return cur;
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
    if (cur->next) cur->next->prev = level;
    cur->next = level;
    level->prev = cur;
}

void remove_price_level(OrderBook *book, PriceLevel *level, char side) {
    PriceLevel **head = (side == 'B') ? &book->buy_levels : &book->sell_levels;

    if (level->prev) level->prev->next = level->next;
    else *head = level->next;

    if (level->next) level->next->prev = level->prev;
    free(level);
}

void append_order_to_level(PriceLevel *level, Order *order) {
    if (!level->tail) {
        level->head = level->tail = order;
    } else {
        level->tail->next = order;
        order->prev = level->tail;
        level->tail = order;
    }
    level->total_qty += order->qty;
    level->order_count++;
}

void insert_passive_order(OrderBook *book, TickEvent *event) {
    PriceLevel **side_head = (event->buy_sell == 'B') ? &book->buy_levels : &book->sell_levels;
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
    if (order->prev) order->prev->next = order->next;
    else level->head = order->next;

    if (order->next) order->next->prev = order->prev;
    else level->tail = order->prev;

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
    PriceLevel *cur = (order->side == 'B') ? book->buy_levels : book->sell_levels;
    while (cur) {
        Order *o = cur->head;
        while (o) {
            if (o == order) return cur;
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
            if (event->price < best_sell->price) break;

            Order *passive = best_sell->head;
            int traded_qty = (event->qty < passive->qty) ? event->qty : passive->qty;

            event->qty -= traded_qty;
            passive->qty -= traded_qty;
            best_sell->total_qty -= traded_qty;

            if (passive->qty == 0) {
                unlink_order_from_level(book, best_sell, passive);
            }
        }
    } else if (event->buy_sell == 'S') {
        while (event->qty > 0 && book->buy_levels) {
            PriceLevel *best_buy = book->buy_levels;
            if (event->price > best_buy->price) break;

            Order *passive = best_buy->head;
            int traded_qty = (event->qty < passive->qty) ? event->qty : passive->qty;

            event->qty -= traded_qty;
            passive->qty -= traded_qty;
            best_buy->total_qty -= traded_qty;

            if (passive->qty == 0) {
                unlink_order_from_level(book, best_buy, passive);
            }
        }
    }
}

void handle_new_order(TickEvent *event) {
    int idx = token_to_index(event->token);
    if (idx < 0 || event->qty <= 0) return;

    OrderBook *book = &books[idx];
    match_active_order(book, event);

    if (event->qty > 0) {
        insert_passive_order(book, event);
    }
}

void handle_modify_order(TickEvent *event) {
    int idx = token_to_index(event->token);
    if (idx < 0 || event->qty <= 0) return;

    OrderBook *book = &books[idx];
    Order *old = find_order(event->order_id);

    if (old) {
        int old_idx = token_to_index(old->token);
        if (old_idx >= 0) {
            OrderBook *old_book = &books[old_idx];
            PriceLevel *level = find_level_containing_order(old_book, old);
            if (level) unlink_order_from_level(old_book, level, old);
        }
    }

    match_active_order(book, event);
    if (event->qty > 0) {
        insert_passive_order(book, event);
    }
}

void handle_cancel_order(TickEvent *event) {
    Order *order = find_order(event->order_id);
    if (!order) return;

    int idx = token_to_index(order->token);
    if (idx < 0) return;

    OrderBook *book = &books[idx];
    PriceLevel *level = find_level_containing_order(book, order);
    if (level) unlink_order_from_level(book, level, order);
}

void reduce_order_qty(double order_id, int trade_qty) {
    Order *order = find_order(order_id);
    if (!order) return;

    int idx = token_to_index(order->token);
    if (idx < 0) return;

    OrderBook *book = &books[idx];
    PriceLevel *level = find_level_containing_order(book, order);
    if (!level) return;

    int reduce_qty = trade_qty;
    if (reduce_qty > order->qty) reduce_qty = order->qty;

    order->qty -= reduce_qty;
    level->total_qty -= reduce_qty;

    if (order->qty <= 0) {
        if (order->prev) order->prev->next = order->next;
        else level->head = order->next;

        if (order->next) order->next->prev = order->prev;
        else level->tail = order->prev;

        level->order_count--;
        char side = order->side;
        remove_order_from_index(order->order_id);
        free(order);

        if (level->order_count == 0) remove_price_level(book, level, side);
    }
}

void handle_trade(TickEvent *event) {
    if (event->trade_qty <= 0) return;
    reduce_order_qty(event->buy_order_id, event->trade_qty);
    reduce_order_qty(event->sell_order_id, event->trade_qty);
}

void process_tick(TickEvent *event) {
    switch (event->msg_typ) {
        case 'N': handle_new_order(event); break;
        case 'M': handle_modify_order(event); break;
        case 'X': handle_cancel_order(event); break;
        case 'T': handle_trade(event); break;
        default: break;
    }
}

void handle_sequence(TickEvent event) {
    if (event.stream_id < 1 || event.stream_id > MAX_STREAMS) return;

    StreamState *s = &streams[event.stream_id];

    if (event.seq_no < s->expected_seq_no) {
        return;
    }

    if (event.seq_no > s->expected_seq_no) {
        int pos = event.seq_no % REORDER_BUFFER_SIZE;
        s->buffer[pos] = event;
        s->occupied[pos] = 1;
        return;
    }

    process_tick(&event);
    s->expected_seq_no++;

    while (1) {
        int pos = s->expected_seq_no % REORDER_BUFFER_SIZE;
        if (!s->occupied[pos]) break;
        if (s->buffer[pos].seq_no != s->expected_seq_no) break;

        TickEvent buffered = s->buffer[pos];
        s->occupied[pos] = 0;
        process_tick(&buffered);
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
}

void init_books(void) {
    for (int i = 0; i < MAX_SCRIPS; i++) {
        books[i].token = 1001 + i;
        strncpy(books[i].symbol, symbols[i], sizeof(books[i].symbol) - 1);
        books[i].buy_levels = NULL;
        books[i].sell_levels = NULL;
    }

    for (int s = 1; s <= MAX_STREAMS; s++) {
        streams[s].expected_seq_no = 1;
    }
}

void print_book_for_token(int token) {
    int idx = token_to_index(token);
    if (idx < 0) return;

    OrderBook *book = &books[idx];
    printf("\n===== %s (%d) =====\n", book->symbol, book->token);

    printf("SELL SIDE:\n");
    PriceLevel *ask = book->sell_levels;
    int c = 0;
    while (ask && c < 5) {
        printf("  Rs.%8.2f | Qty %6d | Orders %3d\n", ask->price / 100.0, ask->total_qty, ask->order_count);
        ask = ask->next;
        c++;
    }

    printf("BUY SIDE:\n");
    PriceLevel *bid = book->buy_levels;
    c = 0;
    while (bid && c < 5) {
        printf("  Rs.%8.2f | Qty %6d | Orders %3d\n", bid->price / 100.0, bid->total_qty, bid->order_count);
        bid = bid->next;
        c++;
    }
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

    int seq[MAX_STREAMS + 1] = {0, 10, 1, 1, 1, 1};
    double oid = 2000000;

    for (int i = 1; i < MAX_SCRIPS; i++) {
        int token = 1001 + i;
        short stream_id = (short)((i / SCRIPS_PER_STREAM) + 1);
        int base = 10000 + (i * 25);

        TickEvent b1 = make_order_event('N', stream_id, seq[stream_id]++, ts++, oid++, token, 'B', base - 10, 100 + i * 5);
        TickEvent b2 = make_order_event('N', stream_id, seq[stream_id]++, ts++, oid++, token, 'B', base - 20, 200 + i * 5);
        TickEvent s1 = make_order_event('N', stream_id, seq[stream_id]++, ts++, oid++, token, 'S', base + 10, 150 + i * 5);
        TickEvent s2 = make_order_event('N', stream_id, seq[stream_id]++, ts++, oid++, token, 'S', base + 20, 250 + i * 5);

        handle_sequence(b1);
        handle_sequence(b2);
        handle_sequence(s1);
        handle_sequence(s2);
    }
}

int best_bid_price(OrderBook *book) {
    if (!book->buy_levels) return 0;
    return book->buy_levels->price;
}

int best_ask_price(OrderBook *book) {
    if (!book->sell_levels) return 0;
    return book->sell_levels->price;
}

void run_live_engine(void) {
    int next_seq[MAX_STREAMS + 1] = {0};
    double next_order_id = 9000000;
    long timestamp_ns = 5000000000L;
    unsigned long cycle = 0;

    for (int s = 1; s <= MAX_STREAMS; s++) {
        next_seq[s] = streams[s].expected_seq_no;
    }

    printf("\nLive backend started. Press Ctrl+C to stop.\n");

    while (1) {
        for (int i = 0; i < MAX_SCRIPS; i++) {
            int token = 1001 + i;
            short stream_id = (short)((i / SCRIPS_PER_STREAM) + 1);
            OrderBook *book = &books[i];

            int bid = best_bid_price(book);
            int ask = best_ask_price(book);

            int mid;
            if (bid > 0 && ask > 0)
                mid = (bid + ask) / 2;
            else
                mid = 10000 + i * 25;

            char side = (rand() % 2 == 0) ? 'B' : 'S';
            int price_shift = ((rand() % 11) - 5) * 5;
            int price = mid + price_shift;

            if (side == 'B' && ask > 0 && rand() % 4 == 0)
                price = ask;

            if (side == 'S' && bid > 0 && rand() % 4 == 0)
                price = bid;

            int qty = 50 + (rand() % 20) * 25;

            TickEvent e = make_order_event(
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

            handle_sequence(e);
        }

        write_json_snapshot();
        printf("\rLive snapshot updated | cycle: %lu", cycle++);
        fflush(stdout);
        usleep(250000);
    }
}

int main(void) {
    srand((unsigned)time(NULL));

    init_books();
    simulate_initial_ticks();
    write_json_snapshot();

    print_book_for_token(1001);

    printf("\nInitial JSON snapshot written to: %s\n", SNAPSHOT_FILE);
    printf("Compile: gcc orderbook_engine_live.c -o orderbook\n");
    printf("Run:     ./orderbook\n");

    run_live_engine();

    return 0;
}
