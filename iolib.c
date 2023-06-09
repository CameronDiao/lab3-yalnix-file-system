#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <comp421/yalnix.h>
#include <comp421/iolib.h>
#include <comp421/filesystem.h>
#include "packet.h"

/*****************
 * PATH HANDLING *
 *****************/

typedef struct PathIterator {
    struct PathIterator *head;
    struct PathIterator *next;
    char data[DIRNAMELEN];
} PathIterator;

/*
 * Fill target buffer.
 */
void SetDirectoryName(char *target, char *path, int start, int end);

/*
 * Parse path into linked list.
 */
PathIterator *ParsePath(char *pathname);

/*
 * Free PathIterator linked list.
 */
int DeletePathIterator(PathIterator *it);

PathIterator *CreatePathIterator(PathIterator *head) {
    PathIterator *it = malloc(sizeof(PathIterator));
    it->head = head;
    it->next = NULL;
    return it;
}

/*
 * Fill target buffer.
 */
 void SetDirectoryName(char *target, char *path, int start, int end) {
     int i;
     for (i = 0; i < DIRNAMELEN; i++) {
        if (i < end - start) {
            target[i] = path[start + i];
        } else {
            target[i] = '\0';
        }
     }
 }

/*
 * Parse pathname into linked list.
 */
PathIterator *ParsePath(char *pathname) {
    PathIterator *it = CreatePathIterator(NULL);
    PathIterator *head = it;

    int i = 0;
    int start = 0;
    int found = 0;
    char next;

    if (pathname[0] == '/') {
        SetDirectoryName(it->data, pathname, 0, 1);
        it->next = CreatePathIterator(head);
        it = it->next;
        i++;
        start = 1;
        found = 1;
    }

    while ((next = pathname[i]) != '\0') {
        if (found == 0 && next == '/') {
            SetDirectoryName(it->data, pathname, start, i);
            it->next = CreatePathIterator(head);
            it = it->next;
            found = 1;
        } else if (found == 1 && next != '/') {
            found = 0;
            start = i;
        }
        i++;
    }

    if (found) {
        SetDirectoryName(it->data, ".", 0, 1);
        it->next = CreatePathIterator(head);
    } else {
        SetDirectoryName(it->data, pathname, start, i);
        it->next = CreatePathIterator(head);
    }

    return head;
}

/*
 * Free PathIterator linked list.
 */
int DeletePathIterator(PathIterator *head) {
    PathIterator *prev;
    PathIterator *it = head;

    while (it != NULL) {
        prev = it;
        it = it->next;
        free(prev);
    }
    return 0;
}


/*******************
 * File Descriptor *
 *******************/

typedef struct FileDescriptor {
    int id; 
    int used; 
    int inum;
    int reuse; 
    int pos;
} FileDescriptor;

/*
 * Prepare new file descriptor.
 */
FileDescriptor *CreateFileDescriptor();

/*
 * Close file descriptor.
 */
int CloseFileDescriptor(int fd);

/*
 * Get file descriptor.
 */
FileDescriptor *GetFileDescriptor(int fd);

FileDescriptor open_file_table[MAX_OPEN_FILES];
int initialized = 0;

void IntializeOpenFileTable() {
    int i;
    if (initialized == 1) {
        return;
    }
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        open_file_table[i].id = i;
        open_file_table[i].used = 0;
        open_file_table[i].inum = 0;
        open_file_table[i].pos = 0;
    }
    initialized = 1;
}

/*
 * Prepare new file descriptor.
 */
FileDescriptor *CreateFileDescriptor() {
    int i;
    if (initialized == 0) {
        IntializeOpenFileTable();
    }
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        if (open_file_table[i].used == 0) {
            open_file_table[i].used = 1;
            break;
        }
    }
    if (i >= MAX_OPEN_FILES) {
        return NULL;
    }
    return &open_file_table[i];
}

/*
 * Close file descriptor.
 */
int CloseFileDescriptor(int fd_id) {
    if (fd_id < 0 || fd_id >= MAX_OPEN_FILES) {
        return -1;
    }
    if (initialized == 0) {
        return -1;
    }
    if (open_file_table[fd_id].used != 0) {
        open_file_table[fd_id].used = 0;
        return 0;
    }
    return -1;
}

/*
 * Get file descriptor.
 */
FileDescriptor *GetFileDescriptor(int fd_id) {
    if (fd_id < 0 || fd_id >= MAX_OPEN_FILES) {
        return NULL;
    }
    if (initialized == 0) {
        return NULL;
    }
    if (open_file_table[fd_id].used == 0) {
        return NULL;
    }
    return &open_file_table[fd_id];
}


int current_inum = ROOTINODE;

int IterateFilePath(char *pathname, int *parent_inum, struct Stat *stat, char *filename, int *reuse) {
    void *packet = malloc(PACKET_SIZE);
    ((DataPacket *)packet)->packet_type = MSG_SEARCH_FILE;
    int next_inum = current_inum;
    char *data;

    *parent_inum = current_inum;
    PathIterator *it = ParsePath(pathname);
    while (it->next != NULL) {
        data = it->data;
        if (data[0] != '/') {
            ((DataPacket *)packet)->arg1 = next_inum;
            ((DataPacket *)packet)->pointer = (void *)data;
            Send(packet, -FILE_SERVER);
            *parent_inum = next_inum;
            next_inum = ((FilePacket *)packet)->inum;
        } else {
            next_inum = ROOTINODE;
            *parent_inum = ROOTINODE;
        }
        it = it->next;

        if (next_inum == 0) {
            break;
        }
    }

    int last_not_found = 0;
    if (it->next == NULL && filename) {
        memcpy(filename, data, DIRNAMELEN);
    }
    if (it->next == NULL && next_inum == 0) {
        last_not_found = 1;
    }

    if (next_inum != 0 && stat) {
        stat->inum = ((FilePacket *)packet)->inum;
        stat->type = ((FilePacket *)packet)->type;
        stat->size = ((FilePacket *)packet)->size;
        stat->nlink = ((FilePacket *)packet)->nlink;
    }
    if (next_inum != 0 && reuse) {
        *reuse = ((FilePacket *)packet)->reuse;
    }

    DeletePathIterator(it);
    free(packet);

    if (next_inum != 0) {
        return 0;
    }
    if (last_not_found != 0) {
        return -1;
    }
    return -2;
}

/**
 * Create new file @ pathname.
 */
int Create(char *pathname) {
    TracePrintf(10, "\t┌─ [Create] path: %s\n", pathname);

    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        fprintf(stderr, "[Error] Invalid pathname\n");
        return -1;
    }

    FileDescriptor *fd = CreateFileDescriptor();
    if (fd == NULL) {
        fprintf(stderr, "[Error] File Descriptors are all used. Try closing others.\n");
        return -1;
    }

    int *parent_inum = malloc(sizeof(int));
    struct Stat *stat = malloc(sizeof(struct Stat));
    char filename[DIRNAMELEN];
    int result = IterateFilePath(pathname, parent_inum, stat, filename, NULL);

    if (result == -2) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        free(stat);
        CloseFileDescriptor(fd->id);
        return -1;
    }

    if (result == 0 && stat->type == INODE_DIRECTORY) {
        fprintf(stderr, "[Error] Cannot overwrite directory\n");
        free(parent_inum);
        free(stat);
        CloseFileDescriptor(fd->id);
        return -1;
    }

    void *packet = malloc(PACKET_SIZE);
    ((DataPacket *)packet)->packet_type = MSG_CREATE_FILE;
    ((DataPacket *)packet)->arg1 = *parent_inum;
    ((DataPacket *)packet)->pointer = (void *)filename;
    Send(packet, -FILE_SERVER);

    int new_inum = ((FilePacket *)packet)->inum;
    if (new_inum <= 0) {
        switch (new_inum) {
            case 0:
                fprintf(stderr, "[Error] File creation error\n");
                break;
            case -1:
                fprintf(stderr, "[Error] Cannot create file in non-directory.\n");
                break;
            case -2:
                fprintf(stderr, "[Error] Directory has reached max size limit.\n");
                break;
            case -3:
                fprintf(stderr, "[Error] Not enough inode left.\n");
                break;
            case -4:
                fprintf(stderr, "[Error] Not enough block left.\n");
                break;
            default:
                break;
        }
        free(packet);
        free(parent_inum);
        free(stat);
        CloseFileDescriptor(fd->id);
        return -1;
    }

    fd->inum = new_inum;
    fd->reuse = ((FilePacket *)packet)->reuse;
    fd->pos = 0;

    free(packet);
    free(parent_inum);
    free(stat);
    TracePrintf(10, "\t└─ [Create fd: %d]\n\n", fd->id);
    return fd->id;
}

/**
 * Opens file @ pathname
 */
int Open(char *pathname) {
    TracePrintf(10, "\t┌─ [Open] path: %s\n", pathname);
    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        fprintf(stderr, "[Error] Invalid pathname\n");
        return -1;
    }
    FileDescriptor *fd = CreateFileDescriptor();
    if (fd == NULL) {
        fprintf(stderr, "[Error] File Descriptors are all used. Try closing others.\n");
        return -1;
    }

    int *parent_inum = malloc(sizeof(int));
    struct Stat *stat = malloc(sizeof(struct Stat));
    int result = IterateFilePath(pathname, parent_inum, stat, NULL, &fd->reuse);

    if (result < 0) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        free(stat);
        CloseFileDescriptor(fd->id);
        return -1;
    }

    fd->pos = 0;
    fd->inum = stat->inum;

    free(parent_inum);
    free(stat);
    TracePrintf(10, "\t└─ [Open fd_id: %d]\n\n", fd->id);
    return fd->id;
}

/**
 * Closes file.
 */
int Close(int fd_id) {
    TracePrintf(10, "\t┌─ [Close] fd_id: %d\n", fd_id);

    if (CloseFileDescriptor(fd_id) < 0) {
        fprintf(stderr, "[Error] Provided fd is not open.\n");
        return -1;
    }

    TracePrintf(10, "\t└─ [Close]\n\n");
    return 0;
}

/**
 * Read file
 */
int Read(int fd_id, void *buf, int size) {
    TracePrintf(10, "\t┌─ [Read] fd_id: %d\n", fd_id);
    if (buf == NULL || size < 0) {
        fprintf(stderr, "[Error] Invalid arguments on buffer or size.\n");
        return -1;
    }

    FileDescriptor *fd = GetFileDescriptor(fd_id);
    if (fd == NULL) {
        fprintf(stderr, "[Error] Provided fd is not open.\n");
        return -1;
    }

    int result;
    DataPacket *packet = malloc(PACKET_SIZE);
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_READ_FILE;
    packet->arg1 = fd->inum;
    packet->arg2 = fd->pos;
    packet->arg3 = size;
    packet->arg4 = fd->reuse;
    packet->pointer = (void *)buf;
    Send(packet, -FILE_SERVER);

    result = packet->arg1;
    free(packet);

    if (result == -1) {
        fprintf(stderr, "[Error] Reuse count has changed. Please close this fd.\n");
        return -1;
    } else if (result == -2) {
        fprintf(stderr, "[Error] This file is freed.\n");
        return -1;
    } else if (result < 0) {
        return -1;
    }

    fd->pos += result;
    TracePrintf(10, "\t└─ [Read size: %d]\n\n", result);
    return result;
}

/**
 * Write from buffer to file fd_id.
 */
int Write(int fd_id, void *buf, int size) {
    TracePrintf(10, "\t┌─ [Write] fd_id: %d\n", fd_id);
    if (buf == NULL || size < 0) {
        fprintf(stderr, "[Error] Invalid arguments on buffer or size.\n");
        return -1;
    }

    FileDescriptor *fd = GetFileDescriptor(fd_id);
    if (fd == NULL) {
        fprintf(stderr, "[Error] Provided fd is not open.\n");
        return -1;
    }

    DataPacket *packet = malloc(PACKET_SIZE);
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_WRITE_FILE;
    packet->arg1 = fd->inum;
    packet->arg2 = fd->pos;
    packet->arg3 = size;
    packet->arg4 = fd->reuse;
    packet->pointer = (void *)buf;
    Send(packet, -FILE_SERVER);
    int result = packet->arg1;
    free(packet);

    if (result == -1) {
        fprintf(stderr, "[Error] Trying to write beyond max file size.\n");
        return -1;
    } else if (result == -2) {
        fprintf(stderr, "[Error] Trying to write to non-regular file.\n");
        return -1;
    } else if (result == -3) {
        fprintf(stderr, "[Error] Reuse count has changed. Please close this fd.\n");
        return -1;
    } else if (result == -4) {
        fprintf(stderr, "[Error] Not enough block left.\n");
        return -1;
    } else if (result < 0) {
        return -1;
    }
    fd->pos += result;

    TracePrintf(10, "\t└─ [Write size: %d]\n\n", result);
    return result;
}

/**
 * Changes file position.
 */
int Seek(int fd_id, int offset, int whence) {
    TracePrintf(10, "\t┌─ [Seek] fd_id: %d\n", fd_id);
    FileDescriptor *fd = GetFileDescriptor(fd_id);
    if (fd == NULL) {
        fprintf(stderr, "[Error] Provided fd is not open.\n");
        return -1;
    }

    FilePacket *packet = malloc(PACKET_SIZE);
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_GET_FILE;
    packet->inum = fd->inum;
    Send(packet, -FILE_SERVER);

    int size = packet->size;
    int reuse = packet->reuse;
    free(packet);

    if (fd->reuse != reuse) {
        fprintf(stderr, "[Error] Reuse count has changed. Please close this fd.\n");
        free(packet);
        return -1;
    }

    int new_pos;
    if (whence == SEEK_SET) {
        new_pos = offset;
    } else if (whence == SEEK_CUR) {
        new_pos = fd->pos + offset;
    } else if (whence == SEEK_END) {
        new_pos = size + offset;
    } else {
        fprintf(stderr, "[Error] Invalid whence provided.\n");
        return -1;
    }

    if (new_pos < 0) {
        fprintf(stderr, "[Error] Updated position cannot be negative.\n");
        return -1;
    }

    fd->pos = new_pos;

    TracePrintf(10, "\t└─ [Seek]\n\n");
    return new_pos;
}

/**
 * Creates hard link from newname to oldname.
 */
int Link(char *oldname, char *newname) {
    TracePrintf(10, "\t┌─ [Link]\n");

    if (oldname == NULL || strlen(oldname) > MAXPATHNAMELEN) {
        fprintf(stderr, "[Error] Invalid pathname\n");
        return -1;
    }

    if (newname == NULL || strlen(newname) > MAXPATHNAMELEN) {
        fprintf(stderr, "[Error] Invalid pathname\n");
        return -1;
    }

    int *old_parent_inum = malloc(sizeof(int));
    int *new_parent_inum = malloc(sizeof(int));
    struct Stat *old_stat = malloc(sizeof(struct Stat));
    char new_filename[DIRNAMELEN];
    int result1 = IterateFilePath(oldname, old_parent_inum, old_stat, NULL, NULL);

    if (result1 < 0) {
        fprintf(stderr, "[Error] Path %s not found\n", oldname);
        free(old_parent_inum);
        free(new_parent_inum);
        free(old_stat);
        return -1;
    }

    if (old_stat->type != INODE_REGULAR) {
        fprintf(stderr, "[Error] You can only create hard link on regular file.\n");
        free(old_parent_inum);
        free(new_parent_inum);
        free(old_stat);
        return -1;
    }

    int result2 = IterateFilePath(newname, new_parent_inum, NULL, new_filename, NULL);
    if (result2 == -2) {
        fprintf(stderr, "[Error] Path %s not found.\n", newname);
        free(old_parent_inum);
        free(new_parent_inum);
        free(old_stat);
        return -1;
    }

    if (result2 == 0) {
        fprintf(stderr, "[Error] File %s already exists.\n", newname);
        free(old_parent_inum);
        free(new_parent_inum);
        free(old_stat);
        return -1;
    }

    DataPacket *packet = malloc(PACKET_SIZE);
    packet->packet_type = MSG_LINK;
    packet->arg1 = old_stat->inum;
    packet->arg2 = *new_parent_inum;
    packet->pointer = (void *)new_filename;
    Send(packet, -FILE_SERVER);
    int result = packet->arg1;

    free(old_parent_inum);
    free(new_parent_inum);
    free(old_stat);
    free(packet);

    if (result < 0) {
        if (result == -1) {
            fprintf(stderr, "[Error] Unexpected CopyFrom error.\n");
        }
        else if (result == -2) {
            fprintf(stderr, "[Error] You can only create hard link on regular file.\n");
        }
        else if (result == -3) {
            fprintf(stderr, "[Error] Parent of new path is not a directory.\n");
        }
        else if (result == -4) {
            fprintf(stderr, "[Error] Not enough block left.\n");
        }
        return -1;
    }

    TracePrintf(10, "\t└─ [Link]\n\n");
    return 0;
}

/**
 * Removes directory entry for pathname.
 */
int Unlink(char *pathname) {
    TracePrintf(10, "\t┌─ [Unlink] pathname: %s\n", pathname);

    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        return -1;
    }

    int *parent_inum = malloc(sizeof(int));
    struct Stat *stat = malloc(sizeof(struct Stat));
    int result = IterateFilePath(pathname, parent_inum, stat, NULL, NULL);

    if (result < 0) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    if (stat->type == INODE_DIRECTORY) {
        fprintf(stderr, "[Error] Cannot unlink directory\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    DataPacket *packet = malloc(PACKET_SIZE);
    packet->packet_type = MSG_UNLINK;
    packet->arg1 = stat->inum;
    packet->arg2 = *parent_inum;
    Send(packet, -FILE_SERVER);
    result = packet->arg1;
    free(parent_inum);
    free(stat);
    free(packet);

    if (result < 0) {
        fprintf(stderr, "[Error] Unlink error.\n");
        return -1;
    }

    TracePrintf(10, "\t└─ [Unlink]\n\n");
    return 0;
}

/**
 * Make new directory at pathname.
 */
int MkDir(char *pathname) {
    TracePrintf(10, "\t┌─ [MkDir] path: %s\n", pathname);

    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        return -1;
    }

    char filename[DIRNAMELEN];
    int *parent_inum = malloc(sizeof(int));
    struct Stat *stat = malloc(sizeof(struct Stat));
    int result = IterateFilePath(pathname, parent_inum, stat, filename, NULL);

    if (result == 0) {
        fprintf(stderr, "[Error] File already exist\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    if (result == -2) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    int new_inum;
    void *packet = malloc(PACKET_SIZE);
    ((DataPacket *)packet)->packet_type = MSG_CREATE_DIR;
    ((DataPacket *)packet)->arg1 = *parent_inum;
    ((DataPacket *)packet)->pointer = (void *)filename;
    Send(packet, -FILE_SERVER);
    new_inum = ((FilePacket *)packet)->inum;

    free(packet);
    free(parent_inum);
    free(stat);

    if (new_inum == 0) {
        fprintf(stderr, "[Error] File creation error\n");
        return -1;
    }

    TracePrintf(10, "\t└─ [MkDir]\n\n");
    return 0;
}

/**
 * Delete directory located at pathname.
 */
int RmDir(char *pathname) {
    TracePrintf(10, "\t┌─ [RmDir] path: %s\n", pathname);

    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        return -1;
    }

    char filename[DIRNAMELEN];
    int *parent_inum = malloc(sizeof(int));
    struct Stat *stat = malloc(sizeof(struct Stat));
    int result = IterateFilePath(pathname, parent_inum, stat, filename, NULL);

    if (filename[0] == '.' && filename[1] == '\0') {
        fprintf(stderr, "[Error] Cannot RmDir .\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    if (filename[0] == '.' && filename[1] == '.' && filename[2] == '\0') {
        fprintf(stderr, "[Error] Cannot RmDir ..\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    if (result < 0) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    DataPacket *packet = malloc(PACKET_SIZE);
    packet->packet_type = MSG_DELETE_DIR;
    packet->arg1 = stat->inum;
    packet->arg2 = *parent_inum;
    Send(packet, -FILE_SERVER);
    result = packet->arg1;

    free(packet);
    free(parent_inum);
    free(stat);

    if (result == -1) {
        fprintf(stderr, "[Error] Cannot delete root directory.\n");
        return -1;
    } else if (result == -2) {
        fprintf(stderr, "[Error] Parent is not a directory.\n");
        return -1;
    } else if (result == -3) {
        fprintf(stderr, "[Error] Cannot call RmDir on regular file.\n");
        return -1;
    } else if (result == -4) {
        fprintf(stderr, "[Error] There are other directories left in this directory.\n");
        return -1;
    } else if (result < 0) {
        return -1;
    }

    TracePrintf(10, "\t└─ [RmDir]\n\n");
    return 0;
}

/**
 * Change current directory to directory @ pathname.
*/
int ChDir(char *pathname) {
    TracePrintf(10, "\t┌─ [ChDir] path: %s\n", pathname);

    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        return -1;
    }

    int *parent_inum = malloc(sizeof(int));
    struct Stat *stat = malloc(sizeof(struct Stat));
    int result = IterateFilePath(pathname, parent_inum, stat, NULL, NULL);

    if (result < 0) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    if (stat->type != INODE_DIRECTORY) {
        fprintf(stderr, "[Error] Not directory\n");
        free(parent_inum);
        free(stat);
        return -1;
    }

    current_inum = stat->inum;
    free(parent_inum);
    free(stat);
    TracePrintf(10, "\t└─ [ChDir]\n\n");
    return 0;
}

/**
 * Write file contents to statbuf.
 */
int Stat(char *pathname, struct Stat *statbuf) {
    TracePrintf(10, "\t┌─ [Stat] path: %s\n", pathname);

    if (pathname == NULL || strlen(pathname) > MAXPATHNAMELEN) {
        return -1;
    }

    int *parent_inum = malloc(sizeof(int));
    int result = IterateFilePath(pathname, parent_inum, statbuf, NULL, NULL);

    if (result < 0) {
        fprintf(stderr, "[Error] Path not found\n");
        free(parent_inum);
        return -1;
    }

    TracePrintf(10, "\t└─ [Stat]\n\n");
    return 0;
}

/**
 * Writes dirty caches to disk.
 */
int Sync() {
    void *packet = malloc(PACKET_SIZE);
    ((DataPacket *)packet)->packet_type = MSG_SYNC;
    ((DataPacket *)packet)->arg1 = 0; 
    Send(packet, -FILE_SERVER);
    free(packet);
    return 0;
}

/**
 * Shutdown by syncing cache, closing library
 */
int Shutdown() {
    void *packet = malloc(PACKET_SIZE);
    ((DataPacket *)packet)->packet_type = MSG_SYNC;
    ((DataPacket *)packet)->arg1 = 1; // Shut down 
    Send(packet, -FILE_SERVER);
    free(packet);
    return 0;
}
