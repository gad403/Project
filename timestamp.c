#include <stdio.h>
#include <time.h>
#include <inttypes.h>

int main() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    int64_t ns_since_epoch = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
    int64_t microseconds = ns_since_epoch / 1000;
    int64_t milliseconds = ns_since_epoch / 1000000;
    int64_t seconds      = ns_since_epoch / 1000000000;
    int64_t minutes      = seconds / 60;
    int64_t hours        = minutes / 60;
    int64_t days         = hours / 24;
    int64_t weeks        = days / 7;
    int64_t months       = days / 30;   
    int64_t years        = days / 365;  

    time_t sec_epoch = (time_t)seconds;
    struct tm local;
    localtime_r(&sec_epoch, &local);


    printf("Nanoseconds since epoch: %" PRId64 "\n", ns_since_epoch);
    printf("Microseconds: %" PRId64 "\n", microseconds);
    printf("Milliseconds: %" PRId64 "\n", milliseconds);
    printf("Seconds:      %" PRId64 "\n", seconds);
    printf("Minutes:      %" PRId64 "\n", minutes);
    printf("Hours:        %" PRId64 "\n", hours);
    printf("Days:         %" PRId64 "\n", days);
    printf("Weeks:        %" PRId64 "\n", weeks);
    printf("Months (~):   %" PRId64 "\n", months);
    printf("Years  (~):   %" PRId64 "\n", years);

    printf("\nCalendar Time (local): %04d-%02d-%02d %02d:%02d:%02d.%09ld\n",
           local.tm_year + 1900,
           local.tm_mon + 1,
           local.tm_mday,
           local.tm_hour,
           local.tm_min,
           local.tm_sec,
           ts.tv_nsec);

    return 0;
}
