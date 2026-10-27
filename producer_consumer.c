/*
 * IoT Sensor Data Processing System
 * Producer-Consumer synchronization using a bounded buffer,
 * POSIX semaphores (empty / full) and a mutex.
 *
 * Producers : Temperature, Humidity and Air Quality sensors
 * Consumer  : Data Processing Server
 * Buffer    : circular queue, 5 slots, FIFO order
 *
 * Build : gcc -Wall -pthread producer_consumer.c -o producer_consumer
 * Run   : ./producer_consumer
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define BUFFER_SIZE        5   /* fixed-size buffer: 5 slots          */
#define NUM_PRODUCERS      3   /* temperature, humidity, air quality  */
#define READINGS_PER_SENSOR 6  /* readings each sensor generates      */
#define TOTAL_ITEMS (NUM_PRODUCERS * READINGS_PER_SENSOR)

typedef struct {
    int    seq;            /* global arrival number (used to verify FIFO) */
    int    sensor_id;
    char   type[16];
    double value;
    char   unit[8];
} SensorData;

/* ---------- shared bounded buffer ---------- */
static SensorData buffer[BUFFER_SIZE];
static int in  = 0;        /* next slot to write  */
static int out = 0;        /* next slot to read   */
static int count = 0;      /* items currently in buffer (for display) */
static int next_seq = 1;   /* assigned inside the critical section */

/* ---------- synchronization ---------- */
static sem_t empty_slots;  /* counts free slots,   starts at BUFFER_SIZE */
static sem_t full_slots;   /* counts filled slots, starts at 0           */
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; /* guards buffer */
static pthread_mutex_t print_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int         id;
    const char *type;
    const char *unit;
    double      base;      /* typical value   */
    double      range;     /* random variation */
} SensorConfig;

static void log_msg(const char *fmt, ...) __attribute__((format(printf,1,2)));
#include <stdarg.h>
static void log_msg(const char *fmt, ...) {
    va_list ap;
    pthread_mutex_lock(&print_lock);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    fflush(stdout);
    pthread_mutex_unlock(&print_lock);
}

/* ---------- Producer: IoT sensor ---------- */
static void *producer(void *arg) {
    SensorConfig *cfg = (SensorConfig *)arg;
    unsigned int seed = (unsigned int)(cfg->id * 7919);

    for (int i = 0; i < READINGS_PER_SENSOR; i++) {
        usleep((100 + rand_r(&seed) % 200) * 1000);        /* sensing time */

        SensorData d;
        d.sensor_id = cfg->id;
        strncpy(d.type, cfg->type, sizeof d.type);
        strncpy(d.unit, cfg->unit, sizeof d.unit);
        d.value = cfg->base + ((rand_r(&seed) % 1000) / 1000.0) * cfg->range;

        /* if buffer is full, the producer BLOCKS here */
        if (sem_trywait(&empty_slots) != 0) {
            log_msg("[Sensor %d | %-11s] BUFFER FULL  -> waiting...\n",
                    cfg->id, cfg->type);
            sem_wait(&empty_slots);
        }

        pthread_mutex_lock(&mutex);            /* ---- critical section ---- */
        d.seq = next_seq++;
        buffer[in] = d;
        in = (in + 1) % BUFFER_SIZE;
        count++;
        log_msg("[Sensor %d | %-11s] PRODUCED #%02d value=%6.2f %-5s (buffer: %d/%d)\n",
                cfg->id, cfg->type, d.seq, d.value, d.unit, count, BUFFER_SIZE);
        pthread_mutex_unlock(&mutex);          /* ---- end critical ------- */

        sem_post(&full_slots);                 /* signal: one more item */
    }
    return NULL;
}

/* ---------- Consumer: Data Processing Server ---------- */
static void *consumer(void *arg) {
    (void)arg;
    unsigned int seed = 12345;
    int last_seq = 0, fifo_ok = 1;

    for (int i = 0; i < TOTAL_ITEMS; i++) {
        /* if buffer is empty, the consumer BLOCKS here */
        if (sem_trywait(&full_slots) != 0) {
            log_msg("[Server              ] BUFFER EMPTY -> waiting...\n");
            sem_wait(&full_slots);
        }

        pthread_mutex_lock(&mutex);            /* ---- critical section ---- */
        SensorData d = buffer[out];
        out = (out + 1) % BUFFER_SIZE;
        count--;
        log_msg("[Server              ] CONSUMED #%02d from Sensor %d (%s) (buffer: %d/%d)\n",
                d.seq, d.sensor_id, d.type, count, BUFFER_SIZE);
        pthread_mutex_unlock(&mutex);          /* ---- end critical ------- */

        sem_post(&empty_slots);                /* signal: one more free slot */

        if (d.seq != last_seq + 1) fifo_ok = 0;   /* FIFO check */
        last_seq = d.seq;

        usleep((250 + rand_r(&seed) % 250) * 1000);   /* processing time */
        log_msg("[Server              ] PROCESSED #%02d: %s = %.2f %s\n",
                d.seq, d.type, d.value, d.unit);
    }

    log_msg("\n=== Summary ===\nItems processed : %d\nFIFO order      : %s\n",
            TOTAL_ITEMS, fifo_ok ? "MAINTAINED" : "VIOLATED");
    return NULL;
}

int main(void) {
    SensorConfig sensors[NUM_PRODUCERS] = {
        {1, "Temperature", "C",   20.0, 15.0},
        {2, "Humidity",    "%",   40.0, 40.0},
        {3, "Air Quality", "AQI", 30.0, 120.0}
    };
    pthread_t prod[NUM_PRODUCERS], cons;

    sem_init(&empty_slots, 0, BUFFER_SIZE);
    sem_init(&full_slots,  0, 0);

    printf("IoT Sensor Data Processing System (buffer size = %d)\n\n", BUFFER_SIZE);

    pthread_create(&cons, NULL, consumer, NULL);
    for (int i = 0; i < NUM_PRODUCERS; i++)
        pthread_create(&prod[i], NULL, producer, &sensors[i]);

    for (int i = 0; i < NUM_PRODUCERS; i++) pthread_join(prod[i], NULL);
    pthread_join(cons, NULL);

    sem_destroy(&empty_slots);
    sem_destroy(&full_slots);
    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&print_lock);
    return 0;
}
