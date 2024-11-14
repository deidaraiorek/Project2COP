#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>
#include <time.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 20

typedef struct {
    char *filename;
    unsigned char *compressed_data;
    int compressed_size;
} FileData;

FileData *file_data_list;
int total_files = 0;
pthread_mutex_t file_data_lock;

void *compress_file(void *arg) {
    int index = *(int *)arg;
    free(arg);

    // Allocate buffer for compressed data
    unsigned char buffer_in[BUFFER_SIZE];
    unsigned char buffer_out[BUFFER_SIZE];
    
    // Open and read the file
    FILE *f_in = fopen(file_data_list[index].filename, "rb");
    assert(f_in != NULL);
    int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
    fclose(f_in);

    // Initialize zlib stream
    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    int ret = deflateInit(&strm, Z_BEST_COMPRESSION);
    assert(ret == Z_OK);

    strm.avail_in = nbytes;
    strm.next_in = buffer_in;
    strm.avail_out = BUFFER_SIZE;
    strm.next_out = buffer_out;

    ret = deflate(&strm, Z_FINISH);
    assert(ret == Z_STREAM_END);

    int compressed_size = BUFFER_SIZE - strm.avail_out;
    deflateEnd(&strm);

    // Store compressed data
    pthread_mutex_lock(&file_data_lock);
    file_data_list[index].compressed_data = malloc(compressed_size);
    memcpy(file_data_list[index].compressed_data, buffer_out, compressed_size);
    file_data_list[index].compressed_size = compressed_size;
    pthread_mutex_unlock(&file_data_lock);

    return NULL;
}

int cmp(const void *a, const void *b) {
    return strcmp(((FileData *)a)->filename, ((FileData *)b)->filename);
}

int main(int argc, char **argv) {
    // Time computation
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    assert(argc == 2);

    // Initialize the directory and file list
    DIR *d = opendir(argv[1]);
    assert(d != NULL);

    struct dirent *dir;
    file_data_list = NULL;

    // Read .ppm files and store file names
    while ((dir = readdir(d)) != NULL) {
        int len = strlen(dir->d_name);
        if (len > 4 && strcmp(dir->d_name + len - 4, ".ppm") == 0) {
            file_data_list = realloc(file_data_list, (total_files + 1) * sizeof(FileData));
            file_data_list[total_files].filename = malloc(strlen(argv[1]) + len + 2);
            sprintf(file_data_list[total_files].filename, "%s/%s", argv[1], dir->d_name);
            total_files++;
        }
    }
    closedir(d);

    // Sort files lexicographically
    qsort(file_data_list, total_files, sizeof(FileData), cmp);

    // Initialize thread pool
    pthread_t threads[MAX_THREADS];
    pthread_mutex_init(&file_data_lock, NULL);

    // Spawn threads for file compression
    for (int i = 0; i < total_files; i++) {
        int *arg = malloc(sizeof(*arg));
        *arg = i;
        pthread_create(&threads[i % MAX_THREADS], NULL, compress_file, arg);

        // Wait for threads if reaching max limit
        if (i % MAX_THREADS == MAX_THREADS - 1 || i == total_files - 1) {
            for (int j = 0; j <= i % MAX_THREADS; j++) {
                pthread_join(threads[j], NULL);
            }
        }
    }

    // Write compressed data to the output file
    FILE *f_out = fopen("video.vzip", "wb");
    assert(f_out != NULL);

    int total_in = 0, total_out = 0;
    for (int i = 0; i < total_files; i++) {
        fwrite(&file_data_list[i].compressed_size, sizeof(int), 1, f_out);
        fwrite(file_data_list[i].compressed_data, sizeof(unsigned char), file_data_list[i].compressed_size, f_out);
        total_out += file_data_list[i].compressed_size;
        free(file_data_list[i].compressed_data);
        free(file_data_list[i].filename);
    }

    fclose(f_out);

    // Clean up
    free(file_data_list);
    pthread_mutex_destroy(&file_data_lock);

    // Calculate and display compression stats
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time: %.2f seconds\n", ((double)end.tv_sec + 1.0e-9 * end.tv_nsec) - ((double)start.tv_sec + 1.0e-9 * start.tv_nsec));

    return 0;
}
