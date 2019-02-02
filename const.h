#ifndef PB_CONST__INCLUDED
#define PB_CONST__INCLUDED

#define MAGIC  0xFFFFFFFF
#define DEVICE_APPS_TYPE 1

#define MAX_APP_SIZE 256
#define MAX_MESSAGE_SIZE 2*1024

typedef struct __attribute__((packed)){
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;

#endif  /* PB_CONST__INCLUDED */