#define PACKET_SIZE 32

#define MSG_GET_FILE 0

#define MSG_SEARCH_FILE 1

#define MSG_CREATE_FILE 2

#define MSG_READ_FILE 3

#define MSG_WRITE_FILE 4

#define MSG_CREATE_DIR 5

#define MSG_DELETE_DIR 6

#define MSG_LINK 7

#define MSG_UNLINK 8

#define MSG_SYNC 9

typedef struct UnknownPacket {
  short packet_type;
  char name[30];
} UnknownPacket;

typedef struct FilePacket {
  short packet_type; 
  char unused[10]; 

  int inum;
  int type;
  int size; 
  int nlink; 
  int reuse;
} FilePacket;

typedef struct DataPacket {
  short packet_type; 
  char unused[6];
  int arg1;
  int arg2;
  int arg3; 
  int arg4; 
  void *pointer;
} DataPacket;
