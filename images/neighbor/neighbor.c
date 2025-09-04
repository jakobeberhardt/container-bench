#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <errno.h>

static volatile int running = 1;

void signal_handler(int sig) {
    running = 0;
}

void print_usage(const char *prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("Options:\n");
    printf("  --duration SECONDS     Run for specified duration (default: 3600)\n");
    printf("  --buffer-size SIZE     Buffer size (e.g., 500MB, 1GB) (default: 100MB)\n");
    printf("  --pattern PATTERN      Access pattern: random/stride/sequential (default: random)\n");
    printf("  --help                 Show this help message\n");
}

long parse_size(const char *size_str) {
    char *endptr;
    double size = strtod(size_str, &endptr);
    
    if (endptr == size_str) {
        fprintf(stderr, "Invalid size format: %s\n", size_str);
        return -1;
    }
    
    long multiplier = 1;
    if (strlen(endptr) > 0) {
        if (strcasecmp(endptr, "KB") == 0) {
            multiplier = 1024;
        } else if (strcasecmp(endptr, "MB") == 0) {
            multiplier = 1024 * 1024;
        } else if (strcasecmp(endptr, "GB") == 0) {
            multiplier = 1024 * 1024 * 1024;
        } else {
            fprintf(stderr, "Unknown size suffix: %s\n", endptr);
            return -1;
        }
    }
    
    return (long)(size * multiplier);
}

void random_access(char *buffer, long size, int duration) {
    time_t start_time = time(NULL);
    srand(time(NULL));
    long operations = 0;
    
    printf("Starting random access pattern for %d seconds with buffer size %ld bytes\n", duration, size);
    
    while (running && (time(NULL) - start_time) < duration) {
        // Access multiple random locations per iteration for intensive memory pressure
        for (int burst = 0; burst < 10000 && running; burst++) {
            long offset = rand() % size;
            // Both read and write to create memory pressure
            volatile char temp = buffer[offset];
            buffer[offset] = (char)((temp + offset) % 256);
            operations++;
        }
    }
    
    printf("Random access completed: %ld operations\n", operations);
}

void stride_access(char *buffer, long size, int duration) {
    time_t start_time = time(NULL);
    long stride = 4096; // 4KB stride for cache-unfriendly access
    long pos = 0;
    long operations = 0;
    
    printf("Starting stride access pattern for %d seconds with buffer size %ld bytes\n", duration, size);
    
    while (running && (time(NULL) - start_time) < duration) {
        // Access multiple stride locations per iteration
        for (int burst = 0; burst < 1000 && running; burst++) {
            // Both read and write to create memory pressure
            volatile char temp = buffer[pos];
            buffer[pos] = (char)((temp + pos) % 256);
            pos = (pos + stride) % size;
            operations++;
        }
    }
    
    printf("Stride access completed: %ld operations\n", operations);
}

void sequential_access(char *buffer, long size, int duration) {
    time_t start_time = time(NULL);
    long pos = 0;
    long operations = 0;
    long total_passes = 0;
    
    printf("Starting sequential access pattern for %d seconds with buffer size %ld bytes\n", duration, size);
    
    while (running && (time(NULL) - start_time) < duration) {
        for (long i = 0; i < size && running && (time(NULL) - start_time) < duration; i += sizeof(long)) {
            volatile long *ptr = (long*)(buffer + i);
            *ptr = (*ptr + i) % 0xFFFFFFFF;
            operations++;
        }
        total_passes++;
        printf("Sequential pass %ld completed\n", total_passes);
    }
    
    printf("Sequential access completed: %ld operations, %ld passes\n", operations, total_passes);
}

int main(int argc, char *argv[]) {
    int duration = 3600; 
    long buffer_size = 100 * 1024 * 1024; 
    char pattern[32] = "random"; 
    
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--duration") == 0) {
            if (i + 1 < argc) {
                duration = atoi(argv[++i]);
                if (duration <= 0) {
                    fprintf(stderr, "Invalid duration: %s\n", argv[i]);
                    return 1;
                }
            } else {
                fprintf(stderr, "--duration requires a value\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--buffer-size") == 0) {
            if (i + 1 < argc) {
                buffer_size = parse_size(argv[++i]);
                if (buffer_size <= 0) {
                    fprintf(stderr, "Invalid buffer size: %s\n", argv[i]);
                    return 1;
                }
            } else {
                fprintf(stderr, "--buffer-size requires a value\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--pattern") == 0) {
            if (i + 1 < argc) {
                strncpy(pattern, argv[++i], sizeof(pattern) - 1);
                pattern[sizeof(pattern) - 1] = '\0';
                if (strcmp(pattern, "random") != 0 && 
                    strcmp(pattern, "stride") != 0 && 
                    strcmp(pattern, "sequential") != 0) {
                    fprintf(stderr, "Invalid pattern: %s. Use random, stride, or sequential\n", pattern);
                    return 1;
                }
            } else {
                fprintf(stderr, "--pattern requires a value\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }
    
    printf("Neighbor workload starting...\n");
    printf("Duration: %d seconds\n", duration);
    printf("Buffer size: %ld bytes (%.2f MB)\n", buffer_size, (double)buffer_size / (1024 * 1024));
    printf("Access pattern: %s\n", pattern);
    
    char *buffer = malloc(buffer_size);
    if (!buffer) {
        fprintf(stderr, "Failed to allocate %ld bytes: %s\n", buffer_size, strerror(errno));
        return 1;
    }
    
    printf("Initializing buffer and forcing allocation...\n");
    for (long i = 0; i < buffer_size; i += 4096) {
        buffer[i] = (char)(i % 256);
    }
    buffer[buffer_size - 1] = 0xFF;
    printf("Buffer initialization completed\n");
    
    if (strcmp(pattern, "random") == 0) {
        random_access(buffer, buffer_size, duration);
    } else if (strcmp(pattern, "stride") == 0) {
        stride_access(buffer, buffer_size, duration);
    } else if (strcmp(pattern, "sequential") == 0) {
        sequential_access(buffer, buffer_size, duration);
    }
    
    printf("Neighbor workload completed\n");
    free(buffer);
    return 0;
}
