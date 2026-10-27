# IoT Sensor Data Processing System – Producer–Consumer Problem

Operating Systems – Internal Assignment 2 (5th Sem, Section B)

## Problem Statement
Three IoT sensors (Temperature, Humidity, Air Quality) act as **Producers** and write readings into a shared **bounded buffer of 5 slots**. A **Data Processing Server** (the **Consumer**) removes and processes the readings. Because all threads share the buffer, synchronization is needed to avoid race conditions.

## Objective
Implement the Producer–Consumer problem using a **mutex** and **counting semaphores** so shared data is handled safely.

## Design

| Item | Purpose | Initial value |
|------|---------|---------------|
| `empty_slots` (semaphore) | counts free slots; producer waits when buffer is **full** | 5 |
| `full_slots` (semaphore) | counts filled slots; consumer waits when buffer is **empty** | 0 |
| `mutex` | mutual exclusion on buffer, `in`, `out`, `count` | unlocked |

**Producer**
```
sem_wait(empty_slots)   // blocks if buffer full
lock(mutex)
  buffer[in] = data; in = (in+1) % 5
unlock(mutex)
sem_post(full_slots)
```

**Consumer**
```
sem_wait(full_slots)    // blocks if buffer empty
lock(mutex)
  data = buffer[out]; out = (out+1) % 5
unlock(mutex)
sem_post(empty_slots)
process(data)
```

### How each requirement is met
1. Sensors (producer threads) generate and insert data.
2. Server (consumer thread) removes and processes data.
3. Buffer is a fixed array of 5 slots (`BUFFER_SIZE`).
4. Producer blocks on `empty_slots` when the buffer is full.
5. Consumer blocks on `full_slots` when the buffer is empty.
6. Mutex + two semaphores guard the buffer.
7. **Race condition** is prevented: only one thread at a time updates `in`, `out`, `count` and the slots.
8. **FIFO**: circular queue with separate `in`/`out` indices; every item gets a sequence number inside the critical section and the consumer verifies they arrive in order.

## Build & Run (Linux / WSL / macOS-with-pthreads)
```
gcc -Wall -pthread producer_consumer.c -o producer_consumer
./producer_consumer
```
or `make run`.

Note: macOS does not support unnamed POSIX semaphores (`sem_init`); use Linux, WSL or an online Linux compiler.

## Expected Result
`sample_output.txt` shows a real run: producers print `BUFFER FULL -> waiting...`, the server prints `BUFFER EMPTY -> waiting...` when applicable, all 18 readings are processed, and the summary reports `FIFO order: MAINTAINED`.

## Visual Simulator (optional)
Open `index.html` in any browser to see the same Producer–Consumer logic animated: 3 sensors, the 5-slot circular buffer, the server, live `empty` / `full` / `mutex` values, and an event log. Use the speed sliders to trigger the buffer-full and buffer-empty cases.
