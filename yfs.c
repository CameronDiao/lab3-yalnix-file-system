#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <comp421/yalnix.h>
#include <comp421/filesystem.h>
#include "yfs.h"
#include "packet.h"

#define DIRSIZE             (int)sizeof(struct dir_entry)
#define MAX_DIRECT_SIZE     BLOCKSIZE * NUM_DIRECT
#define MAX_INDIRECT_SIZE   BLOCKSIZE * (BLOCKSIZE / sizeof(int))
#define MAX_FILE_SIZE       (int)(MAX_DIRECT_SIZE + MAX_INDIRECT_SIZE)
#define INODE_PER_BLOCK     (BLOCKSIZE / INODESIZE)
#define DIR_PER_BLOCK       (BLOCKSIZE / DIRSIZE)
#define GET_DIR_COUNT(n)    (n / DIRSIZE)

/******************
 * INTEGER BUFFER *
 ******************/

struct integer_buf {
    int size;
    int *b;
    int out;
    int in;
    int empty;
    int full;
};

struct fs_header *file_system_header; 

struct integer_buf* free_inode_list;
struct integer_buf* free_block_list; 

struct block_cache* cache_for_blocks; 
struct inode_cache* cache_for_inodes; 


/**
 * Buffer constructor.
 */
struct integer_buf *GetBuffer(int size);

/**
 * Push value to buffer.
 */
void PushToBuffer(struct integer_buf *buf, int i);

/**
 * Pop value from buffer
 */
int PopFromBuffer(struct integer_buf *buf);

/*
 * Fill target buffer.
 */
void SetDirectoryName(char *target, char *path, int start, int end);

/**
 * Buffer contructor.
 */
struct integer_buf *GetBuffer(int size) {
    struct integer_buf* newBuf = malloc(sizeof(struct integer_buf));
    newBuf->size = size;
    newBuf->b = malloc(sizeof(int) * size);
    newBuf->in = 0;
    newBuf->out = 0;
    //New buffer should ALWAYS be empty
    newBuf->empty = 1;
    newBuf->full = 0;
    return newBuf;
}

/**
 * Push value to buffer.
 */
void PushToBuffer(struct integer_buf *buf, int i) {
    if (buf->full) {
        return;
    }
    else if (buf->empty) {
        buf->empty = 0;
    }
    buf->b[buf->in] = i;
    buf->in = buf->in + 1;
    if (buf->in >= buf->size) {
        buf->in = 0;
    }
    if (buf->in == buf->out) {
        buf->full = 1;
    }
}

/**
 * Pop value from buffer.
 */
int PopFromBuffer(struct integer_buf *buf) {
    if (buf->empty) {
        return '\0';
    }
    else if (buf->full) {
        buf->full = 0;
    }
    int next = buf->b[buf->out];
    buf->out = buf->out + 1;
    if (buf->out >= buf->size) {
        buf->out = 0;
    }
    if (buf->out == buf->in) {
        buf->empty = 1;
    }
    return next;
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


/*************************
 * Block and Inode Cache *
 *************************/

struct inode_cache {
    struct inode_cache_entry* top;
    struct inode_cache_entry* base; 
    struct inode_cache_entry** hash_set;
    int stack_size; 
    int hash_size;
};

struct inode_cache_entry {
    struct inode* inode; //Item that this entry represents. Can either be a block or inode
    int inum; //Inode/Block number of the cache entry.
    struct inode_cache_entry* prev_lru; 
    struct inode_cache_entry* next_lru;
    struct inode_cache_entry* prev_hash;
    struct inode_cache_entry* next_hash;
    int dirty; 
};

struct block_cache {
    struct block_cache_entry* top; 
    struct block_cache_entry* base; 
    struct block_cache_entry** hash_set;
    int stack_size; 
    int hash_size;
};

struct block_cache_entry {
    void* block;
    int block_number; 
    struct block_cache_entry* prev_lru; 
    struct block_cache_entry* next_lru; 
    struct block_cache_entry* prev_hash;
    struct block_cache_entry* next_hash;
    int dirty;
};

int inode_count;
int block_count;
struct block_cache* cache_for_blocks; 
struct inode_cache* cache_for_inodes; 

struct inode_cache *CreateInodeCache();

struct block_cache *CreateBlockCache(int num_blocks);

void AddToInodeCache(struct inode_cache *stack, struct inode *in, int inumber);

void AddToBlockCache(struct block_cache *stack, void* block, int block_number);

struct inode_cache_entry* LookUpInode(struct inode_cache *stack, int inumber);

struct block_cache_entry* LookUpBlock(struct block_cache *stack, int block_number);

void RaiseInodeCachePosition(struct inode_cache* stack, struct inode_cache_entry* recent_access);

void RaiseBlockCachePosition(struct block_cache *stack, struct block_cache_entry* recent_access);

void WriteBackInode(struct inode_cache_entry* out);

struct inode_cache_entry* GetInode(int inode_num);

struct block_cache_entry* GetBlock(int block_num);

int HashIndex(int key_value);

/*********************
 * Inode Cache Code *
 ********************/

/**
 * New Inode Cache
 */
struct inode_cache *CreateInodeCache(int num_inodes) {
    inode_count = num_inodes;

    struct inode_cache *new_cache = malloc(sizeof(struct inode_cache));
    new_cache->stack_size = 0;
    new_cache->hash_set = calloc((num_inodes/8) + 1, sizeof(struct inode_cache_entry));
    new_cache->hash_size = (num_inodes/8) + 1;
    cache_for_inodes = new_cache;

    struct inode* dummy_inode = malloc(sizeof(struct inode));
    int i;
    for (i = 1; i <= INODE_CACHESIZE; i++) {
        AddToInodeCache(new_cache, dummy_inode, -1 * i);
    }
    return new_cache;
}

/**
 * Add new inode to cache.
 */
void AddToInodeCache(struct inode_cache *stack, struct inode *inode, int inum) {
    if (stack->stack_size == INODE_CACHESIZE) {
        struct inode_cache_entry *entry = stack->base;
        int old_index = HashIndex(entry->inum);
        int new_index = HashIndex(inum);

        stack->base = stack->base->prev_lru;
        stack->base->next_lru = NULL;

        if (entry->dirty && entry->inum > 0) WriteBackInode(entry);
        if (entry->prev_hash != NULL && entry->next_hash != NULL) {
            entry->next_hash->prev_hash = entry->prev_hash;
            entry->prev_hash->next_hash = entry->next_hash;
        } else if (entry->prev_hash == NULL && entry->next_hash != NULL) {
            stack->hash_set[old_index] = entry->next_hash;
            entry->next_hash->prev_hash = NULL;
        } else if (entry->prev_hash != NULL && entry->next_hash == NULL) {
            entry->prev_hash->next_hash = NULL;
        } else {
            stack->hash_set[old_index] = NULL;
        }
        entry->inode = inode;
        entry->dirty = 0;
        entry->inum = inum;
        entry->prev_lru = NULL;
        entry->next_lru = stack->top;
        stack->top->prev_lru = entry;
        stack->top = entry;
        entry->prev_hash = NULL;
        if (stack->hash_set[new_index] != NULL) {
            stack->hash_set[new_index]->prev_hash = entry;
        }
        entry->next_hash = stack->hash_set[new_index];
        stack->hash_set[new_index] = entry;
    } else {
        struct inode_cache_entry* item = malloc(sizeof(struct inode_cache_entry));
        int index = HashIndex(inum);
        item->inum = inum;
        item->inode = inode;
        item->prev_hash = NULL;
        item->prev_lru = NULL;

        if (stack->hash_set[index] != NULL) {
            stack->hash_set[index]->prev_hash = item;
        }
        item->next_hash = stack->hash_set[index];
        stack->hash_set[index] = item;

        if (stack->stack_size == 0) {
            stack->base = item;
            stack->top = item;
        } else {
            item->next_lru = stack->top;
            stack->top->prev_lru = item;
            stack->top = item;
        }

        stack->stack_size++;
    }
}

/**
 * Look up inode in cache.
 */
struct inode_cache_entry* LookUpInode(struct inode_cache *stack, int inum) {
    struct inode_cache_entry* ice;
    for (ice = stack->hash_set[HashIndex(inum)]; ice != NULL; ice = ice->next_hash) {
        if (ice->inum == inum) {
            RaiseInodeCachePosition(stack, ice);
            return ice;
        }
    }
    return NULL;
}

/**
 * Write into inode.
 */
void WriteBackInode(struct inode_cache_entry* out) {
    struct block_cache_entry* inode_block_entry = GetBlock((out->inum / 8) + 1);
    void* inode_block = inode_block_entry->block;
    struct inode* overwrite = (struct inode *)inode_block + (out->inum % 8);
    inode_block_entry->dirty = 1;
    memcpy(overwrite, out->inode, sizeof(struct inode));
    out->dirty = 0;
}

/**
 * Pop inode and place at top of stack.
 */
void RaiseInodeCachePosition(struct inode_cache* stack, struct inode_cache_entry* recent_access) {
    bool compare_inums = recent_access->inum == stack->top->inum;
    bool compare_lrus = recent_access->prev_lru == NULL && recent_access->next_lru == NULL;
    if (compare_inums || compare_lrus) {
        return;
    }

    if (recent_access->inum == stack->base->inum) {
        recent_access->prev_lru->next_lru = NULL;
        stack->base = recent_access->prev_lru;

        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    } else {
        recent_access->next_lru->prev_lru = recent_access->prev_lru;
        recent_access->prev_lru->next_lru = recent_access->next_lru;

        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    }
}

/**
 * Search for inode.
 */
struct inode_cache_entry* GetInode(int inum) {
    struct inode_cache_entry* current = LookUpInode(cache_for_inodes, inum);
    if (current != NULL) {
        return current;
    }

    struct block_cache_entry* block_entry = GetBlock((inum / 8) + 1);
    void* inode_block = block_entry->block;

    AddToInodeCache(cache_for_inodes, (struct inode *)inode_block + (inum % 8), inum);
    return cache_for_inodes->top;
}

/*********************
 * Block Cache Code *
 ********************/

/**
 * Create cache for blocks.
 */
struct block_cache *CreateBlockCache(int num_blocks) {
    block_count = num_blocks;
    struct block_cache *new_cache = malloc(sizeof(struct block_cache));
    new_cache->stack_size = 0;
    new_cache->hash_set = calloc((num_blocks/8) + 1, sizeof(struct block_cache_entry));
    new_cache->hash_size = (num_blocks/8) + 1;
    cache_for_blocks = new_cache;
    return new_cache;
}

/**
 * Add inode to cache.
 */
void AddToBlockCache(struct block_cache *stack, void* block, int block_number) {
    if (stack->stack_size == BLOCK_CACHESIZE) {
        struct block_cache_entry *entry = stack->base;

        stack->base = stack->base->prev_lru;
        stack->base->next_lru = NULL;

        int old_index = HashIndex(entry->block_number);
        int new_index = HashIndex(block_number);

        if (entry->dirty && entry->block_number > 0) {
            WriteSector(entry->block_number, entry->block);
        }
        if (entry->next_hash != NULL && entry->prev_hash != NULL) {
            entry->prev_hash->next_hash = entry->next_hash;
            entry->next_hash->prev_hash = entry->prev_hash;
        } else if(entry->next_hash == NULL && entry->prev_hash != NULL) {
            entry->prev_hash->next_hash = NULL;
        } else if(entry->next_hash != NULL && entry->prev_hash == NULL) {
            stack->hash_set[old_index] = entry->next_hash;
            entry->next_hash->prev_hash = NULL;
        } else {
            stack->hash_set[old_index] = NULL;
        }

        entry->block = block;
        entry->dirty = 0;
        entry->block_number = block_number;
        entry->prev_lru = NULL;
        entry->next_lru = stack->top;
        stack->top->prev_lru = entry;
        stack->top = entry;
        entry->prev_hash = NULL;

        if (stack->hash_set[new_index] != NULL) {
            stack->hash_set[new_index]->prev_hash = entry;
        }
        entry->next_hash = stack->hash_set[new_index];
        stack->hash_set[new_index] = entry;
    } else {
        struct block_cache_entry* item = malloc(sizeof(struct inode_cache_entry));
        item->block_number = block_number;
        item->block = block;
        if (stack->hash_set[HashIndex(block_number)] != NULL) {
            stack->hash_set[HashIndex(block_number)]->prev_hash = item;
        }
        item->next_hash = stack->hash_set[HashIndex(block_number)];
        item->prev_hash = NULL;
        item->prev_lru = NULL;
        stack->hash_set[HashIndex(block_number)] = item;
        if (stack->stack_size == 0) {
            stack->top = item;
            stack->base = item;
        } else {
            item->next_lru = stack->top;
            stack->top->prev_lru = item;
            stack->top = item;
        }
        stack->stack_size = stack->stack_size + 1;
    }
}

/**
 * Search for block in cache.
 */
struct block_cache_entry* LookUpBlock(struct block_cache *stack, int block_number) {
    struct block_cache_entry* block;
    block = stack->hash_set[HashIndex(block_number)];
    while (block != NULL) {
        if (block->block_number == block_number) {
            RaiseBlockCachePosition(stack, block);
            return block;
        }
        block = block->next_hash;
    }
    return NULL;
}

/**
 * Pop block and place at top of cache.
 */
void RaiseBlockCachePosition(struct block_cache *stack, struct block_cache_entry* recent_access) {
    bool compare_bns = recent_access->block_number == stack->top->block_number;
    bool compare_lrus = recent_access->prev_lru == NULL && recent_access->next_lru == NULL;
    if (compare_bns || compare_lrus) {
        return;
    }
    if (recent_access->block_number == stack->base->block_number) {
        recent_access->prev_lru->next_lru = NULL;
        stack->base = recent_access->prev_lru;

        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    } else {
        recent_access->next_lru->prev_lru = recent_access->prev_lru;
        recent_access->prev_lru->next_lru = recent_access->next_lru;

        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    }
}


/**
 * Search for block.
 */
struct block_cache_entry* GetBlock(int block_num) {
    struct block_cache_entry *current = LookUpBlock(cache_for_blocks,block_num);
    if (current != NULL) {
        return current;
    }

    void *block_buf = malloc(SECTORSIZE);
    ReadSector(block_num, block_buf);
    AddToBlockCache(cache_for_blocks, block_buf, block_num);
    return cache_for_blocks->top;
}

int HashIndex(int key_value) {
    if (key_value > 0) {
        return key_value/8;
    } else {
        return key_value/(-8);
    }
}

/******************
 * Directory Name *
 ******************/
/*
 * Compare dirnames.
 */
int CompareDirname(char *dirname, char *other);

/*
 * Compare dirnames.
 */
 int CompareDirname(char *dirname, char *other) {
     int i;
     for (i = 0; i < DIRNAMELEN; i++) {
         if (dirname[i] != other[i]) {
            return -1;
         }

        if (dirname[i] == '\0') {
            break;
        }
     }

     return 0;
 }

/**
 * Search for value in array and swap with value at index.
 */
void SearchAndSwap(int arr[], int size, int value, int index) {
    int i = -1;
    int search_index = -1;
    while (i != value && index < size) {
        search_index++;
        i = arr[search_index];
    }
    if (search_index == index || i == -1 || i > size) {
        return;
    }
    int j = arr[index];
    arr[index] = value;
    arr[search_index] = j;
}

/**
 * Push free inodes to buffer.
 */
void GetFreeInodeList() {
    free_inode_list = GetBuffer(file_system_header->num_inodes);
    int i;
    for (i = 1; i <= file_system_header->num_inodes; i++) {
        struct inode_cache_entry* curr_inode = GetInode(i);
        struct inode *next = curr_inode->inode;
        if (next->type == INODE_FREE) {
            PushToBuffer(free_inode_list, i);
        }
    }
}

/**
 * List of blocks that have not been allocated yet.
 */
void GetFreeBlockList() {
    int inode_count = file_system_header->num_inodes + 1;
    int inode_block_count = (inode_count + INODE_PER_BLOCK - 1) / INODE_PER_BLOCK;

    int block_count = file_system_header->num_blocks - inode_block_count - 1;
    int *integer_buf = malloc(block_count * sizeof(int));
    int i;
    for (i = 0; i < block_count; i++) {
        integer_buf[i] = inode_block_count + i + 1;
    }

    int busy_blocks = 0;
    for (i = 1; i <= file_system_header->num_inodes; i ++) {
        struct inode_cache_entry* inode_entry = GetInode(i);

        int pos = 0;
        int j = 0;
        while (pos < inode_entry->inode->size && j < NUM_DIRECT) {
            SearchAndSwap(integer_buf, block_count, inode_entry->inode->direct[j], busy_blocks);
            busy_blocks = busy_blocks + 1;
            j = j + 1;
            pos += BLOCKSIZE;
        }

        if (pos < inode_entry->inode->size) {
            SearchAndSwap(integer_buf, file_system_header->num_blocks, inode_entry->inode->indirect, busy_blocks);
            busy_blocks = busy_blocks + 1;
            struct block_cache_entry* curr_block = GetBlock(inode_entry->inode->indirect);
            int *indirect_blocks = curr_block->block;
            j = 0;
            while (j < 128 && pos < inode_entry->inode->size) {
                SearchAndSwap(integer_buf, file_system_header->num_blocks, indirect_blocks[j], busy_blocks);
                busy_blocks = busy_blocks + 1;
                j = j + 1;
                pos += BLOCKSIZE;
            }
        }
    }

    free_block_list = malloc(sizeof(struct integer_buf));
    free_block_list->size = block_count;
    free_block_list->b = integer_buf;
    free_block_list->in = free_block_list->size;
    free_block_list->out = 0;
    free_block_list->empty = 0;
    free_block_list->full = 1;

    int k;
    for (k = 0; k < busy_blocks; k++) {
        PopFromBuffer(free_block_list);
    }
}

/*
 * Create new file inode.
 */
struct inode* CreateFileInode(int new_inum, int parent_inum, short type) {
    struct inode_cache_entry *inode_entry = GetInode(new_inum);
    struct inode *inode = inode_entry->inode;
    inode_entry->dirty = 1;
    inode->type = type;
    inode->size = 0;
    inode->nlink = 0;
    inode->reuse++;

    if (type == INODE_DIRECTORY) {
        inode->nlink = 1; 
        inode->size = sizeof(struct dir_entry) * 2;
        inode->direct[0] = PopFromBuffer(free_block_list);

        struct block_cache_entry *block_entry = GetBlock(inode->direct[0]);
        block_entry->dirty = 1;
        struct dir_entry *block = block_entry->block;
        block[0].inum = new_inum;
        block[1].inum = parent_inum;
        SetDirectoryName(block[0].name, ".", 0, 1);
        SetDirectoryName(block[1].name, "..", 0, 2);
    }

    return inode;
}

/*
 * Truncate File inode.
 */
struct inode* TruncateFileInode(int target_inum) {
    struct inode_cache_entry *entry = GetInode(target_inum);
    struct inode *inode = entry->inode;

    entry = GetInode(target_inum);
    entry->dirty = 1;
    inode = entry->inode;

    int block_count = (inode->size + BLOCKSIZE - 1) / BLOCKSIZE;
    int i;

    int iterate_count = block_count;
    if (block_count > NUM_DIRECT) {
        iterate_count = NUM_DIRECT;
        struct block_cache_entry *indirect_block_entry = GetBlock(inode->indirect);
        int *indirect_block = indirect_block_entry->block;
        indirect_block_entry->dirty = 1;
        for (i = 0; i < block_count - NUM_DIRECT; i++) {
            if (indirect_block[i] != 0) {
                PushToBuffer(free_block_list, indirect_block[i]);
            }
        }
        if (inode->indirect != 0) {
            PushToBuffer(free_block_list, inode->indirect);
        }
    }

    for (i = 0; i < iterate_count; i++) {
        if (inode->direct[i] != 0) {
            PushToBuffer(free_block_list, inode->direct[i]);
        }
    }

    inode->size = 0;
    return inode;
}

/*
 * Registerinum and dirname to directory inode.
 */
int RegisterDirectory(struct inode* parent_inode, int new_inum, char *dirname) {
    struct block_cache_entry *block_entry;
    struct dir_entry *block;
    int *indirect_block = NULL;
    int dir_index;
    int prev_index = -1;
    int outer_index; 
    int inner_index; 

    for (dir_index = 0; dir_index < GET_DIR_COUNT(parent_inode->size); dir_index++) {
        outer_index = dir_index / DIR_PER_BLOCK;
        inner_index = dir_index % DIR_PER_BLOCK;

        if (prev_index != outer_index) {
            if (outer_index >= NUM_DIRECT) {
                if (indirect_block == NULL) {
                    indirect_block = GetBlock(parent_inode->indirect)->block;
                }
                block_entry = GetBlock(indirect_block[outer_index - NUM_DIRECT]);
                block = block_entry->block;
            } else {
                block_entry = GetBlock(parent_inode->direct[outer_index]);
                block = block_entry->block;
            }
            prev_index = outer_index;
        }

        if (block[inner_index].inum == 0) {
            block[inner_index].inum = new_inum;
            SetDirectoryName(block[inner_index].name, dirname, 0, DIRNAMELEN);
            block_entry->dirty = 1;
            return 0;
        }
    }

    struct block_cache_entry *indirect_block_entry;
    if (parent_inode->size >= MAX_DIRECT_SIZE) {
        if (parent_inode->size == MAX_DIRECT_SIZE) {
            parent_inode->indirect = PopFromBuffer(free_block_list);
        }

        indirect_block_entry = GetBlock(parent_inode->indirect);
        indirect_block = indirect_block_entry->block;

        outer_index = (parent_inode->size - MAX_DIRECT_SIZE) / BLOCKSIZE;
        inner_index = GET_DIR_COUNT(parent_inode->size) % DIR_PER_BLOCK;

        if (inner_index == 0) {
            indirect_block[outer_index] = PopFromBuffer(free_block_list);
            indirect_block_entry->dirty = 1;
        }

        block_entry = GetBlock(indirect_block[outer_index]);
        block = block_entry->block;
    } else {
        outer_index = parent_inode->size / BLOCKSIZE;
        inner_index = GET_DIR_COUNT(parent_inode->size) % DIR_PER_BLOCK;
        if (inner_index == 0) {
            parent_inode->direct[outer_index] = PopFromBuffer(free_block_list);
        }

        block_entry = GetBlock(parent_inode->direct[outer_index]);
        block = block_entry->block;
    }

    block[inner_index].inum = new_inum;
    SetDirectoryName(block[inner_index].name, dirname, 0, DIRNAMELEN);
    block_entry->dirty = 1;
    parent_inode->size += DIRSIZE;
    return 1;
}

/*
 * Remove parent inode from directory
 */
int UnregisterDirectory(struct inode* parent_inode, int target_inum) {
    int *indirect_block = NULL;
    struct block_cache_entry *block_entry;
    struct dir_entry *block;
    int dir_index;
    int prev_index = -1;
    int outer_index;
    int inner_index;

    for (dir_index = 0; dir_index < GET_DIR_COUNT(parent_inode->size); dir_index++) {
        outer_index = dir_index / DIR_PER_BLOCK;
        inner_index = dir_index % DIR_PER_BLOCK;

        if (prev_index != outer_index) {
            if (outer_index >= NUM_DIRECT) {
                if (indirect_block == NULL) {
                    indirect_block = GetBlock(parent_inode->indirect)->block;
                }
                block_entry = GetBlock(indirect_block[outer_index - NUM_DIRECT]);
                block = block_entry->block;
            } else {
                block_entry = GetBlock(parent_inode->direct[outer_index]);
                block = block_entry->block;
            }
            prev_index = outer_index;
        }

        if (block[inner_index].inum == target_inum) {
            block[inner_index].inum = 0;
            block_entry->dirty = 1;
            return 0;
        }
    }

    return -1;
}

/*
 * Find inode that matches dirname.
 */
int SearchDirectory(struct inode *inode, char *dirname) {
    int *indirect_block = NULL;
    struct dir_entry *block;
    int dir_index;
    int prev_index = -1;
    int outer_index;
    int inner_index; 

    for (dir_index = 0; dir_index < GET_DIR_COUNT(inode->size); dir_index++) {
        outer_index = dir_index / DIR_PER_BLOCK;
        inner_index = dir_index % DIR_PER_BLOCK;

        if (prev_index != outer_index) {
            if (outer_index >= NUM_DIRECT) {
                if (indirect_block == NULL) {
                    indirect_block = GetBlock(inode->indirect)->block;
                }
                block = GetBlock(indirect_block[outer_index - NUM_DIRECT])->block;
            } else {
                block = GetBlock(inode->direct[outer_index])->block;
            }
            prev_index = outer_index;
        }

        if (block[inner_index].inum == 0) continue;
        if (CompareDirname(block[inner_index].name, dirname) == 0) {
            return block[inner_index].inum;
        }
    }

    return 0;
}

/*
 * Update directory stats given directory inode.
 */
int CleanDirectory(struct inode *inode) {
    int dirty = 0;
    struct block_cache_entry *indirect_block_entry;
    struct dir_entry *block;
    int *indirect_block = NULL;
    int prev_index = -1;

    int dir_index;
    for (dir_index = GET_DIR_COUNT(inode->size) - 1; dir_index > 0; dir_index--) {
        int outer_index = dir_index / DIR_PER_BLOCK;
        int inner_index = dir_index % DIR_PER_BLOCK;

        if (prev_index != outer_index) {
            if (outer_index >= NUM_DIRECT) {
                if (indirect_block == NULL) {
                    indirect_block_entry = GetBlock(inode->indirect);
                    indirect_block = indirect_block_entry->block;
                }
                struct block_cache_entry* temp_block = GetBlock(indirect_block[outer_index - NUM_DIRECT]);
                block = temp_block->block;
            } else {
                struct block_cache_entry* temp_block = GetBlock(inode->direct[outer_index]);
                block = temp_block->block;
            }

            if (prev_index > 0) {
                if (prev_index >= NUM_DIRECT) {
                    if (indirect_block[outer_index - NUM_DIRECT] != 0) {
                        PushToBuffer(free_block_list, indirect_block[prev_index - NUM_DIRECT]);
                    }

                    if (prev_index == NUM_DIRECT && inode->indirect != 0) {
                        PushToBuffer(free_block_list, inode->indirect);
                    }
                } else if (inode->direct[prev_index] != 0) {
                    PushToBuffer(free_block_list, inode->direct[prev_index]);
                }
            }
            prev_index = outer_index;
        }

        if (block[inner_index].inum != 0) {
            break;
        }

        dirty = 1;
        inode->size -= DIRSIZE;
    }

    return dirty;
}

/*************************
 * File Request Hanlders *
 *************************/

/**
 * Get file from packet.
*/
void GetFile(FilePacket *packet) {
    int inum = packet->inum;
    struct inode_cache_entry* inode_entry = GetInode(inum);

    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_SEARCH_FILE;
    packet->inum = inum;
    packet->type = inode_entry->inode->type;
    packet->size = inode_entry->inode->size;
    packet->nlink = inode_entry->inode->nlink;
    packet->reuse = inode_entry->inode->reuse;
}

/**
 * Search file from packet.
*/
void SearchFile(void *packet, int pid) {
    int inum = ((DataPacket *) packet)->arg1;
    void *target = ((DataPacket *) packet)->pointer;

    memset(packet, 0, PACKET_SIZE);
    ((FilePacket *) packet)->packet_type = MSG_SEARCH_FILE;
    ((FilePacket *) packet)->inum = 0;

    char dirname[DIRNAMELEN];
    if (CopyFrom(pid, dirname, target, DIRNAMELEN) < 0) {
        return;
    }
    struct inode *parent_inode = GetInode(inum)->inode;

    if (parent_inode->type != INODE_DIRECTORY) {
        return;
    }
    int target_inum = SearchDirectory(parent_inode, dirname);

    if (target_inum == 0) {
        return;
    }

    struct inode *target_inode = GetInode(target_inum)->inode;
    ((FilePacket *)packet)->inum = target_inum;
    ((FilePacket *)packet)->type = target_inode->type;
    ((FilePacket *)packet)->size = target_inode->size;
    ((FilePacket *)packet)->nlink = target_inode->nlink;
    ((FilePacket *)packet)->reuse = target_inode->reuse;

    // return 0;
}

/**
 * Create file.
*/
void CreateFile(void *packet, int pid, short type) {
    int parent_inum = ((DataPacket *)packet)->arg1;
    void *target = ((DataPacket *)packet)->pointer;

    memset(packet, 0, PACKET_SIZE);
    ((FilePacket *)packet)->packet_type = MSG_CREATE_FILE;

    char dirname[DIRNAMELEN];
    if (CopyFrom(pid, dirname, target, DIRNAMELEN) < 0) {
        ((FilePacket *)packet)->inum = 0;
        return;
    }

    struct inode_cache_entry *parent_entry = GetInode(parent_inum);
    struct inode *parent_inode = parent_entry->inode;

    if (parent_inode->type != INODE_DIRECTORY) {
        ((FilePacket *)packet)->inum = -1;
        return;
    }

    if (parent_inode->size >= MAX_FILE_SIZE) {
        ((FilePacket *)packet)->inum = -2;
        return;
    }

    int target_inum = SearchDirectory(parent_inode, dirname);
    struct inode *new_inode;
    if (target_inum > 0) {
        new_inode = TruncateFileInode(target_inum);
    } else {
        if (free_inode_list->size == 0) {
            ((FilePacket *)packet)->inum = -3;
            return;
        }
        if (free_block_list->size < 3) {
            ((FilePacket *)packet)->inum = -4;
            return;
        }

        target_inum = PopFromBuffer(free_inode_list);
        new_inode = CreateFileInode(target_inum, parent_inum, type);

        if (type == INODE_DIRECTORY) {
            parent_inode->nlink += 1;
        }
        parent_entry->dirty = RegisterDirectory(parent_inode, target_inum, dirname);
        new_inode->nlink = new_inode->nlink + 1;
    }

    ((FilePacket *)packet)->inum = target_inum;
    ((FilePacket *)packet)->type = new_inode->type;
    ((FilePacket *)packet)->size = new_inode->size;
    ((FilePacket *)packet)->nlink = new_inode->nlink;
    ((FilePacket *)packet)->reuse = new_inode->reuse;
}

/**
 * Read file from packet.
*/
void ReadFile(DataPacket *packet, int pid) {
    int inum = packet->arg1;
    int pos = packet->arg2;
    int size = packet->arg3;
    int reuse = packet->arg4;
    void *integer_buf = packet->pointer;

    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_READ_FILE;

    struct inode_cache_entry* inode_entry = GetInode(inum);
    struct inode *inode = inode_entry->inode;
    if (inode->reuse != reuse) {
        packet->arg1 = -1;
        return;
    }

    if (inode->type == INODE_FREE) {
        packet->arg1 = -2;
        return;
    }

    if (inode->size < pos + size) {
        size = inode->size - pos;
        if (size <= 0) {
            packet->arg1 = 0;
            return;
        }
    }

    int start_index = pos / BLOCKSIZE;
    int end_index = (pos + size) / BLOCKSIZE;
    if ((pos + size) % BLOCKSIZE == 0) {
        end_index = end_index - 1;
    }

    char hole_buf[BLOCKSIZE];
    memset(hole_buf, 0, BLOCKSIZE);

    int *indirect_block = NULL;

    if (inode->size >= MAX_DIRECT_SIZE) {
        struct block_cache_entry* temp_block = GetBlock(inode->indirect);
        indirect_block = temp_block->block;
    }

    int outer_index;
    int prefix = 0;
    int copied_size = 0;
    for (outer_index = start_index; outer_index <= end_index; outer_index++) {
        int block_id;
        char *block;
        if (outer_index >= NUM_DIRECT) {
            block_id = indirect_block[outer_index - NUM_DIRECT];
        } else {
            block_id = inode->direct[outer_index];
        }

        if (block_id != 0) {
            block = GetBlock(block_id)->block;
        }
        else {
            block = hole_buf;
        }

        prefix = (pos + copied_size) % BLOCKSIZE;
        int copysize = BLOCKSIZE - prefix;

        if (outer_index == end_index) {
            copysize = size - copied_size;
        }

        if (copysize > 0) {
            CopyTo(pid, integer_buf + copied_size, block + prefix, copysize);
            copied_size += copysize;
        }
    }
    packet->arg1 = copied_size;
}

/**
 * Write file from packet.
*/
void WriteFile(DataPacket *packet, int pid) {
    int inum = packet->arg1;
    int pos = packet->arg2;
    int size = packet->arg3;
    int reuse = packet->arg4;
    void *integer_buf = packet->pointer;
    char *block;

    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_WRITE_FILE;

    if (pos + size > MAX_FILE_SIZE) {
        packet->arg1 = -1;
        return;
    }

    struct inode_cache_entry *inode_entry = GetInode(inum);
    struct inode *inode;
    inode = inode_entry->inode;
    if (inode->type != INODE_REGULAR) {
        packet->arg1 = -2;
        return;
    }

    if (inode->reuse != reuse) {
        packet->arg1 = -3;
        return;
    }

    int start_index = pos / BLOCKSIZE; 
    int end_index = (pos + size) / BLOCKSIZE; 
    if ((pos + size) % BLOCKSIZE == 0) {
        end_index = end_index - 1;
    }

    struct block_cache_entry *indirect_block_entry;
    int *indirect_block = NULL;
    // Prefetch indirect block
    if (inode->size >= MAX_DIRECT_SIZE) {
        indirect_block = GetBlock(inode->indirect)->block;
    }

    int inode_block_count = (inode->size + BLOCKSIZE - 1) / BLOCKSIZE;

    int extra_blocks = 0;
    int i;
    for (i = start_index; i <= end_index; i++) {
        if (inode_block_count <= i) {
            if (i == NUM_DIRECT) {
                extra_blocks++;
            }

            if (i >= start_index) {
                extra_blocks++;
            }
        }
    }

    if (free_block_list->size <= extra_blocks) {
        ((FilePacket *)packet)->inum = -4;
        return;
    }


    int outer_index = start_index;
    if (outer_index < inode_block_count) {
        outer_index = inode_block_count;
    }
    for (; outer_index <= end_index; outer_index++) {
        if (inode_block_count <= outer_index) {
            if (outer_index == NUM_DIRECT) {
                inode->indirect = PopFromBuffer(free_block_list);
                indirect_block_entry = GetBlock(inode->indirect);
                indirect_block = indirect_block_entry->block;
                indirect_block_entry->dirty = 1;
                memset(indirect_block, 0, BLOCKSIZE);
            }

            if (outer_index > NUM_DIRECT) {
                indirect_block_entry = GetBlock(inode->indirect);
                indirect_block = indirect_block_entry->block;
                indirect_block_entry->dirty = 1;
            }

            if (outer_index >= start_index) {
                if (outer_index >= NUM_DIRECT) {
                    indirect_block[outer_index - NUM_DIRECT] = PopFromBuffer(free_block_list);
                    block = GetBlock(indirect_block[outer_index - NUM_DIRECT])->block;
                    memset(block, 0, BLOCKSIZE);
                } else {
                    inode->direct[outer_index] = PopFromBuffer(free_block_list);
                    block = GetBlock(inode->direct[outer_index])->block;
                    memset(block, 0, BLOCKSIZE);
                    inode_entry->dirty = 1;
                }
            }
        }
    }

    int copied_size = 0; 
    int new_size = start_index * BLOCKSIZE;

    for (outer_index = start_index; outer_index <= end_index; outer_index++) {
        int block_id;
        if (outer_index >= NUM_DIRECT) {
            block_id = indirect_block[outer_index - NUM_DIRECT];
        } else {
            block_id = inode->direct[outer_index];
        }

        struct block_cache_entry *block_entry = GetBlock(block_id);
        block = block_entry->block;
        int prefix = (pos + copied_size) % BLOCKSIZE;

        if (outer_index == start_index) {
            new_size += prefix;
        }

        int copysize = BLOCKSIZE - prefix;
        if (outer_index == end_index) {
            copysize = size - copied_size;
        }

        CopyFrom(pid, block + prefix, integer_buf + copied_size, copysize);
        block_entry->dirty = 1;
        copied_size += copysize;
    }
    new_size += copied_size;
    if (new_size > inode->size) {
        inode_entry->dirty = 1;
        inode->size = new_size;
    }
    packet->arg1 = copied_size;
}

/**
 * Delete directory
*/
void DeleteDir(DataPacket *packet) {
    int target_inum = packet->arg1;
    int parent_inum = packet->arg2;

    if (target_inum == ROOTINODE) {
        packet->arg1 = -1;
        return;
    }
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_DELETE_DIR;


    struct inode_cache_entry *parent_entry = GetInode(parent_inum);;
    struct inode *parent_inode = parent_entry->inode;

    if (parent_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -2;
        return;
    }

    struct inode_cache_entry *target_entry = GetInode(target_inum);
    struct inode *target_inode = target_entry->inode;

    if (target_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -3;
        return;
    }
    if (target_inode->size > DIRSIZE * 2) {
        packet->arg1 = -4;
        return;
    }

    if (UnregisterDirectory(parent_inode, target_inum) < 0) {
        packet->arg1 = -5;
        return;
    }

    target_entry->dirty = 1;
    target_inode->type = INODE_FREE;
    target_inode->size = 0;
    target_inode->nlink = 0;

    parent_entry->dirty = CleanDirectory(parent_inode);

    struct block_cache_entry *block_entry;
    struct dir_entry *block;

    block_entry = GetBlock(target_inode->direct[0]);
    block_entry->dirty = 1;
    block = block_entry->block;

    int i;
    for (i = 0; i < 2; i++) {
        if (block[i].name[1] == '.') {
            target_entry = GetInode(block[i].inum);
            target_entry->inode->nlink -= 1;
            target_entry->dirty = 1;
        }
        block[i].inum = 0;
    }
}

/**
 * Create link.
*/
void CreateLink(DataPacket *packet, int pid) {
    int target_inum = packet->arg1;
    int parent_inum = packet->arg2;

    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_LINK;
    packet->arg1 = 0;

    char dirname[DIRNAMELEN];
    void *target = packet->pointer;
    if (CopyFrom(pid, dirname, target, DIRNAMELEN) < 0) {
        packet->arg1 = -1;
        return;
    }

    struct inode_cache_entry *target_entry = GetInode(target_inum);;
    struct inode *target_inode = target_entry->inode;

    if (target_inode->type != INODE_REGULAR) {
        packet->arg1 = -2;
        return;
    }

    struct inode_cache_entry *parent_entry = GetInode(parent_inum);
    struct inode *parent_inode = parent_entry->inode;

    if (parent_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -3;
        return;
    }
    if (free_block_list->size < 2) {
        packet->arg1 = -4;
        return;
    }

    parent_entry->dirty = RegisterDirectory(parent_inode, target_inum, dirname);
    target_entry->dirty = 1;
    target_inode->nlink = target_inode->nlink + 1;
}

/**
 * Delete link.
*/
void DeleteLink(DataPacket *packet) {
    int target_inum = packet->arg1;
    int parent_inum = packet->arg2;

    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_UNLINK;
    packet->arg1 = 0;

    struct inode_cache_entry *parent_entry = GetInode(parent_inum);
    struct inode *parent_inode = parent_entry->inode;

    if (parent_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -1;
        return;
    }

    struct inode_cache_entry *target_entry = GetInode(target_inum);
    struct inode *target_inode = target_entry->inode;

    if (UnregisterDirectory(parent_inode, target_inum) < 0) {
        packet->arg1 = -2;
        return;
    }

    target_entry->dirty = 1;
    target_inode->nlink -= 1;

    if (target_inode->nlink == 0) {
        TruncateFileInode(target_inum);
        target_inode->type = INODE_FREE;
        PushToBuffer(free_inode_list, target_inum);
    }

    parent_entry->dirty = CleanDirectory(parent_inode);
}

/**
 * Sync cache.
 */
void SyncCache() {
    struct inode_cache_entry* inode;
    for (inode = cache_for_inodes->top; inode != NULL; inode = inode->next_lru) {
        if (inode->dirty) {
            struct block_cache_entry* inode_block_entry = GetBlock((inode->inum / 8) + 1);
            inode_block_entry->dirty = 1;
            void* inode_block = inode_block_entry->block;
            struct inode* overwrite = (struct inode *)inode_block + (inode->inum % 8);
            memcpy(overwrite, inode->inode, sizeof(inode));
            inode->dirty = 0;
        }
    }

    struct block_cache_entry* block = cache_for_blocks->top;
    while (block != NULL) {
        if (block->dirty) {
            WriteSector(block->block_number, block->block);
            block->dirty = 0;
        }
        block = block->next_lru;
    }
    return;
}

/**
 * Execute based on packet.
*/
int main(int argc, char **argv) {
    (void)argc;

    Register(FILE_SERVER);
    void *sector_one = malloc(SECTORSIZE);
    if (ReadSector(1, sector_one) != 0) {
        printf("Error\n");
    }
    else {
        file_system_header = (struct fs_header *)sector_one;
    }

    cache_for_inodes = CreateInodeCache(file_system_header->num_inodes);
    cache_for_blocks = CreateBlockCache(file_system_header->num_blocks);
    GetFreeInodeList();
    GetFreeBlockList();

    int pid;
  	if ((pid = Fork()) < 0) {
  	    fprintf(stderr, "Cannot Fork.\n");
  	    return -1;
  	}

    if (pid == 0) {
        Exec(argv[1], argv + 1);
        fprintf(stderr, "Cannot Exec.\n");
        return -1;
    }

    void *packet = malloc(PACKET_SIZE);
    while (1) {
        if ((pid = Receive(packet)) < 0) {
            fprintf(stderr, "Receive Error.\n");
            return -1;
        }

        if (pid == 0) {
            continue;
        }

        if (((UnknownPacket *)packet)->packet_type == MSG_GET_FILE) {
            GetFile(packet);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_SEARCH_FILE) {
            SearchFile(packet, pid);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_CREATE_FILE) {
            CreateFile(packet, pid, INODE_REGULAR);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_READ_FILE) {
            ReadFile(packet, pid);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_WRITE_FILE) {
            WriteFile(packet, pid);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_CREATE_DIR) {
            CreateFile(packet, pid, INODE_DIRECTORY);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_DELETE_DIR) {
            DeleteDir(packet);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_LINK) {
            CreateLink(packet, pid);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_UNLINK) {
            DeleteLink(packet);
        } else if (((UnknownPacket *)packet)->packet_type == MSG_SYNC) {
            SyncCache();
            if (((DataPacket *)packet)->arg1 == 1) {
                Reply(packet, pid);
                printf("Shutdown by pid: %d.\n", pid);
                Exit(0);
            }
        }

        if (Reply(packet, pid) < 0) {
            fprintf(stderr, "Reply Error.\n");
            return -1;
        }
    }

    return 0;
};