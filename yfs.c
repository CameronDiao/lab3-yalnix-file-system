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

struct buffer {
    /**
     * Size to go to until the buffer loops
     */
    int size;
    /**
     * Pointer to character array
     */
    int *b;
    /**
     * First filled space, next to be removed
     */
    int out;

    /**
     * First empty space, next to be filled
     */
    int in;
    /**
     * Condition on if the buffer is empty
     */
    int empty;
    /**
     * Condition on if the buffer is full
     */
    int full;
};

struct fs_header *header; /* Pointer to File System Header */

struct block_cache* block_stack; /* Cache for recently accessed blocks */
struct inode_cache* inode_stack; /* Cache for recently accessed inodes */

struct buffer* free_inode_list; /* List of Inodes available to assign to files */
struct buffer* free_block_list; /* List of blocks ready to allocate for file data */


/**
 * Constructor message for a new buffer
 * @param size How much space to allocate for the buffer
 */
struct buffer *GetBuffer(int size);

/**
 * Adds a value to the buffers queue
 * @param buf Buffer to add to
 * @param c Character that is being added
 */
void PushToBuffer(struct buffer *buf, int i);

/**
 * Pops the next character from the buffer's queue
 * @param buf Buffer requesting character from
 * @return the character that was popped
 */
int PopFromBuffer(struct buffer *buf);
/**
 * Prints out the contents of the buffer
 * @param buf Buffer to print
 */
void PrintBuffer(struct buffer *buf);

struct buffer *GetBuffer(int size) {
    struct buffer* newBuf = malloc(sizeof(struct buffer));
    newBuf->size = size;
    newBuf->b = malloc(sizeof(int) * size);
    newBuf->in = 0;
    newBuf->out = 0;
    /**
     * New buffer should ALWAYS be empty
     */
    newBuf->empty = 1;
    newBuf->full = 0;
    return newBuf;
}

void PushToBuffer(struct buffer *buf, int i) {
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

int PopFromBuffer(struct buffer *buf) {
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

void PrintBuffer(struct buffer *buf) {
    // int i;
    if (buf->out < buf->in) {
        int i;
        printf("[");
        for (i = buf->out; i < buf->in-1; i++) {
            printf("%d, ", buf->b[i]);
        }
        printf("%d]\n", buf->b[buf->in-1]);
    } else {
        int i;
        printf("[");
        for (i = buf->out; i < buf->size; i++) {
            printf("%d, ", buf->b[i]);
        }
        for (i = 0; i < buf->in-1; i++) {
            printf("%d, ", buf->b[i]);
        }
        printf("%d]\n", buf->b[buf->in-1]);
    }

}

/*****************
 * PATH HANDLING *
 *****************/

typedef struct PathIterator {
    struct PathIterator *head;
    struct PathIterator *next;
    char data[DIRNAMELEN];
} PathIterator;

/*
 * Fills target buffer path + empty strings.
 */
void SetDirectoryName(char *target, char *path, int start, int end);

/*
 * Given pathname as Yalnix argument, parse it
 * into a linked list of component.
 */
PathIterator *ParsePath(char *pathname);

/*
 * Free pathIterator linked list after finished using it.
 * You must provided the head of the linked list.
 */
int DeletePathIterator(PathIterator *it);


/*
 * Private helper method which allocate PathIterator.
 */
PathIterator *CreatePathIterator(PathIterator *head) {
    PathIterator *it = malloc(sizeof(PathIterator));
    it->head = head;
    it->next = NULL;
    return it;
}


/*
 * Fills target buffer path + empty strings.
 */
 void SetDirectoryName(char *target, char *path, int start, int end) {
     int i;
     for (i = 0; i < DIRNAMELEN; i++) {
        if (i < end - start) {
            target[i] = path[start + i];
        } else {
            target[i] = '\0';
        }
        //  if (i < end - start) target[i] = path[start + i];
        //  else target[i] = '\0';
     }
 }

/*
 * Given pathname as Yalnix argument, parse it
 * into a linked list of component.
 */
PathIterator *ParsePath(char *pathname) {
    PathIterator *it = CreatePathIterator(NULL);
    PathIterator *head = it;

    int i = 0;
    int start = 0;
    int found = 0;
    char next;

    // Backslash is found in the root position
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
            // Backslash is just found
            SetDirectoryName(it->data, pathname, start, i);
            it->next = CreatePathIterator(head);
            it = it->next;
            found = 1;
        } else if (found == 1 && next != '/') {
            // Backslash is just finished.
            found = 0;
            start = i;
        }
        i++;
    }

    if (found) {
        // If iterator is finished after backslash is found,
        // last component is current directory
        SetDirectoryName(it->data, ".", 0, 1);
        it->next = CreatePathIterator(head);
    } else {
        // Last remaining path
        SetDirectoryName(it->data, pathname, start, i);
        it->next = CreatePathIterator(head);
    }

    return head;
}

/*
 * Free pathIterator linked list after finished using it.
 * You must provided the head of the linked list.
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

// void TestPathIterator() {
//     PathIterator *it;

//     printf("Testing /abc/def/ghi.c\n");
//     it = ParsePath("/abc/def/ghi.c");
//     assert(strcmp(it->data, "/") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "abc") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "def") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "ghi.c") == 0);
//     it = it->next;
//     assert(it->next == NULL);
//     DeletePathIterator(it->head);

//     printf("Testing xyz/123-456\n");
//     it = ParsePath("xyz/123-456");
//     assert(strcmp(it->data, "xyz") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "123-456") == 0);
//     it = it->next;
//     printf("it->data: %s\n", it->data);
//     assert(it->next == NULL);
//     DeletePathIterator(it->head);

//     printf("Testing ../../foo/bar/baz/example.txt\n");
//     it = ParsePath("../../foo/bar/baz/example.txt");
//     assert(strcmp(it->data, "..") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "..") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "foo") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "bar") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "baz") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "example.txt") == 0);
//     it = it->next;
//     assert(it->next == NULL);
//     DeletePathIterator(it->head);

//     printf("Testing silly/./././././././example.txt\n");
//     it = ParsePath("silly/./././././././example.txt");
//     assert(strcmp(it->data, "silly") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "example.txt") == 0);
//     it = it->next;
//     assert(it->next == NULL);
//     DeletePathIterator(it->head);

//     printf("Testing //////dirname//subdirname///file\n");
//     it = ParsePath("//////dirname//subdirname///file");
//     assert(strcmp(it->data, "/") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "dirname") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "subdirname") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "file") == 0);
//     it = it->next;
//     assert(it->next == NULL);
//     DeletePathIterator(it->head);

//     printf("Testing /a/b/c/\n");
//     it = ParsePath("/a/b/c/");
//     assert(strcmp(it->data, "/") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "a") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "b") == 0);
//     it = it->next;
//     assert(strcmp(it->data, "c") == 0);
//     it = it->next;
//     assert(strcmp(it->data, ".") == 0);
//     it = it->next;
//     assert(it->next == NULL);
//     DeletePathIterator(it->head);
// }

/*************************
 * Block and INode Cache *
 *************************/

struct block_cache {
    struct block_cache_entry* top; //Top of the cache stack
    struct block_cache_entry* base; //Bottom of the cache stack
    struct block_cache_entry** hash_set;
    int stack_size; //Number of entries in the cache stack
    int hash_size;
};

struct block_cache_entry {
    void* block;
    int block_number; //Item that this entry represents. Can either be a block or inode
    struct block_cache_entry* prev_lru; //Previous Block in Stack
    struct block_cache_entry* next_lru; //Next Block in Stack
    struct block_cache_entry* prev_hash;
    struct block_cache_entry* next_hash;
    int dirty; //Whether or not this
};

struct inode_cache {
    struct inode_cache_entry* top; //Top of the cache stack
    struct inode_cache_entry* base; //Bottom of the cache stack
    struct inode_cache_entry** hash_set;
    int stack_size; //Number of entries in the cache stack
    int hash_size; //Number of hash collisions
};

struct inode_cache_entry {
    struct inode* inode; //Item that this entry represents. Can either be a block or inode
    int inum; //Inode/Block number of the cache entry.
    struct inode_cache_entry* prev_lru; //Previous Inode in Stack
    struct inode_cache_entry* next_lru; //Next Inode in Stack
    struct inode_cache_entry* prev_hash;
    struct inode_cache_entry* next_hash;
    int dirty; //Whether or not this
};

int inode_count;
int block_count;
struct block_cache* block_stack; /* Cache for recently accessed blocks */
struct inode_cache* inode_stack; /* Cache for recently accessed inodes */

struct inode_cache *CreateInodeCache();

void AddToInodeCache(struct inode_cache *stack, struct inode *in, int inumber);

struct inode_cache_entry* LookUpInode(struct inode_cache *stack, int inumber);

void RaiseInodeCachePosition(struct inode_cache* stack, struct inode_cache_entry* recent_access);

void WriteBackInode(struct inode_cache_entry* out);

struct inode_cache_entry* GetInode(int inode_num);

void PrintInodeCacheHashSet(struct inode_cache* stack);

void PrintInodeCacheStack(struct inode_cache* stack);

/*********************
 * Block Cache Code *
 ********************/

struct block_cache *CreateBlockCache(int num_blocks);

void AddToBlockCache(struct block_cache *stack, void* block, int block_number);

struct block_cache_entry* LookUpBlock(struct block_cache *stack, int block_number);

void RaiseBlockCachePosition(struct block_cache *stack, struct block_cache_entry* recent_access);

void WriteBackBlock(struct block_cache_entry* out);

struct block_cache_entry* GetBlock(int block_num);

void PrintBlockCacheHashSet(struct block_cache* stack);

void PrintBlockCacheStack(struct block_cache* stack);

int HashIndex(int key_value);

void TestInodeCache(int num_inodes);

void TestBlockCache(int num_blocks);

/*********************
 * Inode Cache Code *
 ********************/
/**
 * Creates a new Cache for Inodes
 * @return Newly created cache
 */
struct inode_cache *CreateInodeCache(int num_inodes) {
    inode_count = num_inodes;

    struct inode_cache *new_cache = malloc(sizeof(struct inode_cache));
    new_cache->stack_size = 0;
    new_cache->hash_set = calloc((num_inodes/8) + 1, sizeof(struct inode_cache_entry));
    new_cache->hash_size = (num_inodes/8) + 1;
    inode_stack = new_cache;

    struct inode* dummy_inode = malloc(sizeof(struct inode));
    int i;
    for (i = 1; i <= INODE_CACHESIZE; i++) {
        AddToInodeCache(new_cache, dummy_inode, -1 * i);
    }
    // for (i = -1; i >= -1 * INODE_CACHESIZE; i--) {
    //     AddToInodeCache(new_cache, dummy_inode, i);
    // }
    return new_cache;
}

/**
 * Adds a new inode to the top of the cache entry
 */
void AddToInodeCache(struct inode_cache *stack, struct inode *inode, int inum) {
    if (stack->stack_size == INODE_CACHESIZE) {
        /**If the stack is full, the base is de-allocated and the pointers to it are nullified */
        struct inode_cache_entry *entry = stack->base;
        int old_index = HashIndex(entry->inum);
        int new_index = HashIndex(inum);

        /**Reassign Base*/
        stack->base = stack->base->prev_lru;
        stack->base->next_lru = NULL;

        /** Write Back the Inode if it is dirty to avoid losing data*/
        if (entry->dirty && entry->inum > 0) WriteBackInode(entry);
        if (entry->prev_hash != NULL && entry->next_hash != NULL) {
            /**Both Neighbors aren't Null*/
            entry->next_hash->prev_hash = entry->prev_hash;
            entry->prev_hash->next_hash = entry->next_hash;
        } else if (entry->prev_hash == NULL && entry->next_hash != NULL) {
            /**Tail End of Hash Table Array*/
            stack->hash_set[old_index] = entry->next_hash;
            entry->next_hash->prev_hash = NULL;
        } else if (entry->prev_hash != NULL && entry->next_hash == NULL) {
            /**Head of Hash Table Array*/
            entry->prev_hash->next_hash = NULL;
        } else {
            /**No Neighbors in Hash Table Array*/
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
        /** Create a Cache Entry for the Inode*/
        struct inode_cache_entry* item = malloc(sizeof(struct inode_cache_entry));
        int index = HashIndex(inum);
        item->inum = inum;
        item->inode = inode;
        item->prev_hash = NULL;
        item->prev_lru = NULL;

        /** If hash already exists, enqueue item in the hash linked list */
        if (stack->hash_set[index] != NULL) {
            stack->hash_set[index]->prev_hash = item;
        }
        item->next_hash = stack->hash_set[index];
        stack->hash_set[index] = item;

        /** Place entry into the LRU Stack*/
        if (stack->stack_size == 0) {
            /** If the stack is empty the entry becomes both the top and base*/
            stack->base = item;
            stack->top = item;
        } else {
            /** If the stack isn't empty the entry is added to the top*/
            item->next_lru = stack->top;
            stack->top->prev_lru = item;
            stack->top = item;
        }

        /**If the stack isn't full increase the size*/
        stack->stack_size++;
    }
}

/**
 * Searches up an inode within a cache
 * @param stack Cache to look for the inode in
 * @param inum Number of the inode being looked up
 * @return The requested inode
 */
struct inode_cache_entry* LookUpInode(struct inode_cache *stack, int inum) {
    struct inode_cache_entry* ice;
    /**For loop iterating into the hash array index the inode number points to*/
    for (ice = stack->hash_set[HashIndex(inum)]; ice != NULL; ice = ice->next_hash) {
        if (ice->inum == inum) {
            RaiseInodeCachePosition(stack, ice);
            return ice;
        }
    }
    /**Returns null if Inode is not found*/
    return NULL;
}

/**
 *
 * @param out Inode that was pushed out of the cache
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
 * When ever an inode is accessed, it is pulled from it's position in the stack
 * and placed at the top
 * @param stack Stack to rearrange
 * @param recent_access Entry to move
 */
void RaiseInodeCachePosition(struct inode_cache* stack, struct inode_cache_entry* recent_access) {
    bool compare_inums = recent_access->inum == stack->top->inum;
    bool compare_lrus = recent_access->prev_lru == NULL && recent_access->next_lru == NULL;
    if (compare_inums || compare_lrus) {
        return;
    }

    if (recent_access->inum == stack->base->inum) {
        /**Reassign Base*/
        recent_access->prev_lru->next_lru = NULL;
        stack->base = recent_access->prev_lru;

        /**Reassign Top*/
        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    } else {
        /**Reassign Neighbors*/
        recent_access->next_lru->prev_lru = recent_access->prev_lru;
        recent_access->prev_lru->next_lru = recent_access->next_lru;

        /**Reassign Top*/
        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    }
}

/**
 * Searches for and returns the inode based on the number given
 * @param inode_num The inode being requested
 * @return A pointer to where the inode is
 */
struct inode_cache_entry* GetInode(int inum) {
    /** Inode number must be in valid range*/
    // assert(inum >= 1 && inum <= inode_count);

    /** First Check the Inode Cache */
    struct inode_cache_entry* current = LookUpInode(inode_stack, inum);
    if (current != NULL) {
        return current;
    }
    // if (current != NULL) return current;

    /** If it's not in the Inode Cache, check the Block */
    struct block_cache_entry* block_entry = GetBlock((inum / 8) + 1);
    void* inode_block = block_entry->block;

    /** Add inode to cache, when accessed*/
    AddToInodeCache(inode_stack, (struct inode *)inode_block + (inum % 8), inum);
    return inode_stack->top;
}

void PrintInodeCacheHashSet(struct inode_cache* stack) {
    int index;
    // struct inode_cache_entry* entry;

    for (index = 0; index < stack->hash_size; index++) {
        printf("[");
        struct inode_cache_entry* entry = stack->hash_set[index];
        while (entry != NULL) {
            printf(" %d ",entry->inum);
            entry = entry->next_hash;
        }

        // for (entry = stack->hash_set[index]; entry != NULL; entry = entry->next_hash) {
        //     printf(" %d ",entry->inum);
        // }
        printf("]\n");
    }
}

void PrintInodeCacheStack(struct inode_cache* stack) {
    struct inode_cache_entry* position = stack->top;
    while (position != NULL) {
        printf("| %d |\n", position->inum);
        position = position->next_lru;
    }
}

/*********************
 * Block Cache Code *
 ********************/
/**
 * Creates new LIFO Cache for blocks
 */
struct block_cache *CreateBlockCache(int num_blocks) {
    block_count = num_blocks;
    struct block_cache *new_cache = malloc(sizeof(struct block_cache));
    new_cache->stack_size = 0;
    new_cache->hash_set = calloc((num_blocks/8) + 1, sizeof(struct block_cache_entry));
    new_cache->hash_size = (num_blocks/8) + 1;
    block_stack = new_cache;
    return new_cache;
}

/**
 * Adds a new inode to the top of the cache entry
 */
void AddToBlockCache(struct block_cache *stack, void* block, int block_number) {
    if (stack->stack_size == BLOCK_CACHESIZE) {
        /**If the stack is full, the base is de-allocated and the pointers to it are nullified */
        struct block_cache_entry *entry = stack->base;

        /**Reassign Base*/
        stack->base = stack->base->prev_lru;
        stack->base->next_lru = NULL;

        int old_index = HashIndex(entry->block_number);
        int new_index = HashIndex(block_number);

        /** Write Back the Block if it is dirty to avoid losing data*/
        if (entry->dirty && entry->block_number > 0) {
            WriteSector(entry->block_number, entry->block);
        }
        if (entry->next_hash != NULL && entry->prev_hash != NULL) {
            /**Both Neighbors aren't Null*/
            entry->prev_hash->next_hash = entry->next_hash;
            entry->next_hash->prev_hash = entry->prev_hash;
        } else if(entry->next_hash == NULL && entry->prev_hash != NULL) {
            /**Head of Hash Table Array*/
            entry->prev_hash->next_hash = NULL;
        } else if(entry->next_hash != NULL && entry->prev_hash == NULL) {
            /**Tail End of Hash Table Array*/
            stack->hash_set[old_index] = entry->next_hash;
            entry->next_hash->prev_hash = NULL;
        } else {
            /**No Neighbors in Hash Table Array*/
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
        /** Create a Cache Entry for the Inode*/
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
        /** Place entry into the LRU Stack*/
        if (stack->stack_size == 0) {
            /** If the stack is empty the entry becomes both the top and base*/
            stack->top = item;
            stack->base = item;
        } else {
            /** If the stack isn't empty the entry is added to the top*/
            item->next_lru = stack->top;
            stack->top->prev_lru = item;
            stack->top = item;
        }
        /**If the stack isn't full increase the size*/
        stack->stack_size = stack->stack_size + 1;
    }
}

/**
 * Searches for a Block in the Cache
 * @param stack Stack to search for the Block in
 * @param block_number Number of the block to search for
 * @return The requested Block or a pointer to Null
 */
struct block_cache_entry* LookUpBlock(struct block_cache *stack, int block_number) {
    struct block_cache_entry* block;
    /**For loop iterating into the hash array index the inode number points to*/
    block = stack->hash_set[HashIndex(block_number)];
    while (block != NULL) {
        if (block->block_number == block_number) {
            RaiseBlockCachePosition(stack, block);
            return block;
        }
        block = block->next_hash;
    }

    // for (block = stack->hash_set[HashIndex(block_number)]; block != NULL; block = block->next_hash) {
    //     if (block->block_number == block_number) {
    //         RaiseBlockCachePosition(stack,block);
    //         return block;
    //     }
    // }
    /**Returns null if Inode is not found*/
    return NULL;
}

/**
 * Repositions block at top of stack whenever used
 */
void RaiseBlockCachePosition(struct block_cache *stack, struct block_cache_entry* recent_access) {
    bool compare_bns = recent_access->block_number == stack->top->block_number;
    bool compare_lrus = recent_access->prev_lru == NULL && recent_access->next_lru == NULL;
    if (compare_bns || compare_lrus) {
        return;
    }
    if (recent_access->block_number == stack->base->block_number) {
        /**Reassign Base*/
        recent_access->prev_lru->next_lru = NULL;
        stack->base = recent_access->prev_lru;

        /**Reassign Top*/
        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    } else {
        /**Reassign Neighbors*/
        recent_access->next_lru->prev_lru = recent_access->prev_lru;
        recent_access->prev_lru->next_lru = recent_access->next_lru;

        /**Reassign Top*/
        recent_access->next_lru = stack->top;
        stack->top->prev_lru = recent_access;
        stack->top = recent_access;
    }
}


void PrintBlockCacheHashSet(struct block_cache* stack) {
    int index;
    // struct block_cache_entry* entry;

    for (index = 0; index < stack->hash_size; index++) {
        if (stack->hash_set[index] == NULL) continue;
        printf("[");
        struct block_cache_entry* entry = stack->hash_set[index];
        while (entry != NULL) {
            printf(" %d ",entry->block_number);
            entry = entry->next_hash;
        }
        // for (entry = stack->hash_set[index]; entry != NULL; entry = entry->next_hash) {
        //     printf(" %d ",entry->block_number);
        // }
        printf("]\n");
    }
}

/**
 * Returns a block, either by searching the cache or reading its sector
 * @param block_num The number of the block being requested
 * @return Pointer to the data that the block encapsulates
 */
struct block_cache_entry* GetBlock(int block_num) {
    /**Must be a valid block number */
    // assert(block_num >= 1 && block_num <= block_count);

    /** First Check Block Cache*/
    struct block_cache_entry *current = LookUpBlock(block_stack,block_num);
    if (current != NULL) {
        return current;
    }

   /** If not found in cache, read directly from disk */
    void *block_buffer = malloc(SECTORSIZE);
    ReadSector(block_num, block_buffer);
    AddToBlockCache(block_stack, block_buffer, block_num);
    return block_stack->top;
}


/**
 * Prints out the Block Cache as a stack
 */
 void PrintBlockCacheStack(struct block_cache* stack) {
     struct block_cache_entry* position = stack->top;
    for (position = stack->top; position != NULL; position = position->next_lru) {
        printf("| %d |\n", position->block_number);
    }

    //  while (position != NULL) {
    //      printf("| %d |\n", position->block_number);
    //      position = position->next_lru;
    //  }
 }

int HashIndex(int key_value) {
    if (key_value > 0) {
        return key_value/8;
    } else {
        return key_value/(-8);
    }
    // return key_value > 0 ? key_value/8 : key_value/(-8) ;
}

// void TestInodeCache(int num_inodes) {
//     int i;
//     int inode_number;
//     struct inode* inode;
//     for(i = 1; i <= 64; i++) {
//         inode_number = (rand() % num_inodes)+1;
//         printf("Call %d: Calling Inode %d\n",i,inode_number);
//         inode = GetInode(inode_number)->inode;
//         printf("Inode Type: %d\n",inode->type);
//         printf("Cache Hash Table: \n");
//         PrintInodeCacheHashSet(inode_stack);
//         printf("Cache Stack\n");
//         PrintInodeCacheStack(inode_stack);
//         printf("Cache Size: %d\n",inode_stack->stack_size);
//     }
//     int j;
//     for(i = 1; i <= num_inodes; i++) {
//         printf("Calling Inode %d\n",i);
//         for(j = 0; j < 64; j++) {
//             GetInode(i);
//         }
//         printf("%d Calls to Inode %d Successful\n",j,i);
//         printf("Cache Hash Table: \n");
//         PrintInodeCacheHashSet(inode_stack);
//         printf("Cache Stack\n");
//         PrintInodeCacheStack(inode_stack);
//     }

// }

// void TestBlockCache(int num_blocks) {
//     int i;
//     int block_number;
//     for(i = 1; i <= 64; i++) {
//         block_number = (rand() % num_blocks)+1;
//         printf("Call %d: Calling Block %d\n",i,block_number);
//         GetBlock(block_number);
//         printf("Cache Hash Table: \n");
//         PrintBlockCacheHashSet(block_stack);
//         printf("Cache Stack\n");
//         PrintBlockCacheStack(block_stack);
//         printf("Cache Size: %d\n",block_stack->stack_size);
//     }
//     int j;
//     for(i = 1; i <= num_blocks; i++) {
//         printf("Calling Block %d\n",i);
//         for(j = 0; j < 64; j++) {
//             GetBlock(i);
//         }
//         printf("%d Calls to Inode %d Successful\n",j,i);
//         printf("Cache Hash Table: \n");
//         PrintBlockCacheHashSet(block_stack);
//         printf("Cache Stack\n");
//         PrintBlockCacheStack(block_stack);
//     }
// }

/******************
 * Directory Name *
 ******************/
/*
 * Compare dirname. Return 0 if equal, -1 otherwise.
 */
int CompareDirname(char *dirname, char *other);

/*
 * Compare dirname. Return 0 if equal, -1 otherwise.
 */
 int CompareDirname(char *dirname, char *other) {
     int i;
     for (i = 0; i < DIRNAMELEN; i++) {
         /* If characters do not match, it is not equal */
        //  if (dirname[i] != other[i]) return -1;
         if (dirname[i] != other[i]) {
            return -1;
         }

         /* If dirname is null string, break */
        //  if (dirname[i] == '\0') break;
        if (dirname[i] == '\0') {
            break;
        }
     }

     return 0;
 }

/*******************
 * File Descriptor *
 *******************/

typedef struct FileDescriptor {
    int id; /* FD id */
    int used; /* Only valid if used = 1 */
    int inum; /* Inode number */
    int reuse; /* Same inode with different reuse means file is changed */
    int pos; /* Current position */
} FileDescriptor;

/*
 * Prepare new file descriptor using lowest available fd using file data.
 * Return NULL if open file table is all filled up.
 */
FileDescriptor *CreateFileDescriptor();

/*
 * Close file descriptor. Return -1 if fd is invalid.
 */
int CloseFileDescriptor(int fd);

/*
 * Get file descriptor. Return NULL if fd is invalid.
 */
FileDescriptor *GetFileDescriptor(int fd);

FileDescriptor open_file_table[MAX_OPEN_FILES];
int initialized = 0;

/*
 * Private helper for initializing open file table
 */
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
 * Prepare new file descriptor using lowest available fd using file data.
 * Return -1 if open file table is all filled up.
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
 * Close file descriptor. Return -1 if fd is invalid.
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
 * Get file descriptor. Return NULL if fd is invalid.
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



/*
 * Simple helper for getting block count with inode->size
 */
int GetBlockCount(int size) {
    int block_count = (size + BLOCKSIZE - 1) / BLOCKSIZE;
    // if ((size % BLOCKSIZE) > 0) {
    //     block_count = block_count + 1; // Round up
    // }
    return block_count;
}


/**
 * Searches for a value in an array, if it's found swaps it with the value
 * at index. Helper function for GetFreeBlockList
 * @param arr Array that is to be swapped
 * @param size Size of the array
 * @param value Value that will be swapped
 * @param index Index to swap to
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
 * Iterates through Inodes and pushes each free one to the buffer
 */
void GetFreeInodeList() {
    free_inode_list = GetBuffer(header->num_inodes);
    int i;
    for (i = 1; i <= header->num_inodes; i++) {
        struct inode_cache_entry* curr_inode = GetInode(i);
        struct inode *next = curr_inode->inode;
        if (next->type == INODE_FREE) {
            PushToBuffer(free_inode_list, i);
        }
    }
}

/**
 * Function initializes the list numbers for blocks that have not been allocated yet
 */
void GetFreeBlockList() {
    /* Compute number of blocks occupited by inodes */
    int inode_count = header->num_inodes + 1;
    int inode_block_count = (inode_count + INODE_PER_BLOCK - 1) / INODE_PER_BLOCK;
    // if (inode_count % INODE_PER_BLOCK > 0) {
    //     inode_block_count = inode_block_count + 1;
    // }

    /* Compute number of blocks with inodes and boot block excluded */
    int block_count = header->num_blocks - inode_block_count - 1;
    // int first_data_block = 1 + inode_block_count;
    int *buffer = malloc(block_count * sizeof(int));
    int i;
    /* Block 0 is the boot block and not used by the file system */
    for (i = 0; i < block_count; i++) {
        // buffer[i] = i + first_data_block;
        buffer[i] = inode_block_count + i + 1;
    }


    /* Iterate through inodes to check which ones are using blocks */
    // int j;
    // int pos;
    int busy_blocks = 0;
    // struct inode *scan;
    for (i = 1; i <= header->num_inodes; i ++) {
        struct inode_cache_entry* inode_entry = GetInode(i);
        // scan = inode_entry->inode;

        /* Iterate through direct array to find first blocks */
        int pos = 0;
        int j = 0;
        while (pos < inode_entry->inode->size && j < NUM_DIRECT) {
            SearchAndSwap(buffer, block_count, inode_entry->inode->direct[j], busy_blocks);
            busy_blocks = busy_blocks + 1;
            j = j + 1;
            pos += BLOCKSIZE;
        }
        /*
         * If indirect array is a non-zero value add it to the allocated block
         * and the array that's contained
         */
        if (pos < inode_entry->inode->size) {
            SearchAndSwap(buffer, header->num_blocks, inode_entry->inode->indirect, busy_blocks);
            busy_blocks = busy_blocks + 1;
            struct block_cache_entry* curr_block = GetBlock(inode_entry->inode->indirect);
            int *indirect_blocks = curr_block->block;
            j = 0;
            while (j < 128 && pos < inode_entry->inode->size) {
                SearchAndSwap(buffer, header->num_blocks, indirect_blocks[j], busy_blocks);
                busy_blocks = busy_blocks + 1;
                j = j + 1;
                pos += BLOCKSIZE;
            }
        }
    }

    /* Initialize a special buffer */
    free_block_list = malloc(sizeof(struct buffer));
    free_block_list->size = block_count;
    free_block_list->b = buffer;
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
 * Create a new file inode using provided arguments
 */
struct inode* CreateFileInode(int new_inum, int parent_inum, short type) {
    struct inode_cache_entry *inode_entry = GetInode(new_inum);
    struct inode *inode = inode_entry->inode;
    // struct block_cache_entry *block_entry;
    // struct dir_entry *block;

    /* New inode is created and it is dirty */
    inode_entry->dirty = 1;
    inode->type = type;
    inode->size = 0;
    inode->nlink = 0;
    inode->reuse++;

    /* Create . and .. by default */
    if (type == INODE_DIRECTORY) {
        inode->nlink = 1; /* Link to itself */
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
 * Truncate file inode
 */
struct inode* TruncateFileInode(int target_inum) {
    struct inode_cache_entry *entry = GetInode(target_inum);
    struct inode *inode = entry->inode;
    // struct block_cache_entry *indirect_block_entry;
    // int *indirect_block = NULL;

    entry = GetInode(target_inum);
    entry->dirty = 1;
    inode = entry->inode;

    int block_count = GetBlockCount(inode->size);
    int i;

    /* Need to free indirect blocks */
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
 * Register provided inum and dirname to directory inode.
 * Return 1 if parent inode becomes dirty for this action.
 */
int RegisterDirectory(struct inode* parent_inode, int new_inum, char *dirname) {
    /* Verify against number of new blocks required */
    struct block_cache_entry *block_entry;
    struct dir_entry *block;
    int *indirect_block = NULL;
    int dir_index;
    int prev_index = -1;
    int outer_index; /* index of direct or indirect */
    int inner_index; /* index of dir_entry array */

    /* Similar process as SearchDirectory to find available inum */
    for (dir_index = 0; dir_index < GET_DIR_COUNT(parent_inode->size); dir_index++) {
        outer_index = dir_index / DIR_PER_BLOCK;
        inner_index = dir_index % DIR_PER_BLOCK;

        /* Get block if outer_index is incremented */
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

        /* Found empty inum. Finished! */
        if (block[inner_index].inum == 0) {
            block[inner_index].inum = new_inum;
            SetDirectoryName(block[inner_index].name, dirname, 0, DIRNAMELEN);
            block_entry->dirty = 1;
            return 0;
        }
    }

    struct block_cache_entry *indirect_block_entry;
    /* No inum = 0 is found. Need to append and increase size */
    if (parent_inode->size >= MAX_DIRECT_SIZE) {
        /* If it just reached MAX_DIRECT_SIZE, need extra block for indirect */
        if (parent_inode->size == MAX_DIRECT_SIZE) {
            parent_inode->indirect = PopFromBuffer(free_block_list);
        }

        indirect_block_entry = GetBlock(parent_inode->indirect);
        indirect_block = indirect_block_entry->block;

        outer_index = (parent_inode->size - MAX_DIRECT_SIZE) / BLOCKSIZE;
        inner_index = GET_DIR_COUNT(parent_inode->size) % DIR_PER_BLOCK;

        /*
         * If size is perfectly divisible by DIR_PER_BLOCK,
         * that means it is time to allocate new block.
         */
        if (inner_index == 0) {
            indirect_block[outer_index] = PopFromBuffer(free_block_list);
            indirect_block_entry->dirty = 1;
        }

        block_entry = GetBlock(indirect_block[outer_index]);
        block = block_entry->block;
    } else {
        outer_index = parent_inode->size / BLOCKSIZE;
        inner_index = GET_DIR_COUNT(parent_inode->size) % DIR_PER_BLOCK;
        /*
         * If size is perfectly divisible by DIR_PER_BLOCK,
         * that means it is time to allocate new block.
         */
        if (inner_index == 0) {
            parent_inode->direct[outer_index] = PopFromBuffer(free_block_list);
        }

        block_entry = GetBlock(parent_inode->direct[outer_index]);
        block = block_entry->block;
    }

    /* Register inum and dirname in the dir_entry */
    block[inner_index].inum = new_inum;
    SetDirectoryName(block[inner_index].name, dirname, 0, DIRNAMELEN);
    block_entry->dirty = 1;
    parent_inode->size += DIRSIZE;
    return 1;
}

/*
 * Remove provided inum in the parent directory
 * Return -1 if target inum is not found.
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

        /* Get block if outer_index is incremented */
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

        /* Found it! */
        if (block[inner_index].inum == target_inum) {
            block[inner_index].inum = 0;
            block_entry->dirty = 1;
            return 0;
        }
    }

    return -1;
}

/*
 * Given directory inode and dirname,
 * find inode number which matches with dirname.
 */
int SearchDirectory(struct inode *inode, char *dirname) {
    int *indirect_block = NULL;
    struct dir_entry *block;
    int dir_index;
    int prev_index = -1;
    int outer_index; /* index of direct or indirect */
    int inner_index; /* index of dir_entry array */

    for (dir_index = 0; dir_index < GET_DIR_COUNT(inode->size); dir_index++) {
        outer_index = dir_index / DIR_PER_BLOCK;
        inner_index = dir_index % DIR_PER_BLOCK;

        /* Get block if outer_index is incremented */
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
 * Given directory inode with a link deleted,
 * update size and free blocks if necessary.
 */
int CleanDirectory(struct inode *inode) {
    int dirty = 0;
    struct block_cache_entry *indirect_block_entry;
    struct dir_entry *block;
    int *indirect_block = NULL;
    // int dir_index = GET_DIR_COUNT(inode->size) - 1;
    int prev_index = -1;
    // int outer_index; /* index of direct or indirect */
    // int inner_index; /* index of dir_entry array */

    /* Iterate from backward */
    int dir_index;
    for (dir_index = GET_DIR_COUNT(inode->size) - 1; dir_index > 0; dir_index--) {
        int outer_index = dir_index / DIR_PER_BLOCK;
        int inner_index = dir_index % DIR_PER_BLOCK;

        /* Get block if outer_index is incremented */
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

            /* If block is switched 2nd+ time, that block needs to be freed */
            if (prev_index > 0) {
                if (prev_index >= NUM_DIRECT) {
                    if (indirect_block[outer_index - NUM_DIRECT] != 0) {
                        PushToBuffer(free_block_list, indirect_block[prev_index - NUM_DIRECT]);
                    }

                    /* Need to free indirect block as well */
                    if (prev_index == NUM_DIRECT && inode->indirect != 0) {
                        PushToBuffer(free_block_list, inode->indirect);
                    }
                } else if (inode->direct[prev_index] != 0) {
                    PushToBuffer(free_block_list, inode->direct[prev_index]);
                }
            }
            prev_index = outer_index;
        }

        /* Found valid directory */
        if (block[inner_index].inum != 0) {
            break;
        }

        /* Decrease parent size */
        dirty = 1;
        inode->size -= DIRSIZE;
    }

    return dirty;
}

/*************************
 * File Request Hanlders *
 *************************/
void GetFile(FilePacket *packet) {
    int inum = packet->inum;
    struct inode_cache_entry* inode_entry = GetInode(inum);
    // struct inode *inode = inode_entry->inode;

    /* Bleach packet for reuse */
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_SEARCH_FILE;
    packet->inum = inum;
    packet->type = inode_entry->inode->type;
    packet->size = inode_entry->inode->size;
    packet->nlink = inode_entry->inode->nlink;
    packet->reuse = inode_entry->inode->reuse;
}

void SearchFile(void *packet, int pid) {
    int inum = ((DataPacket *) packet)->arg1;
    void *target = ((DataPacket *) packet)->pointer;

    /* Bleach packet for reuse */
    memset(packet, 0, PACKET_SIZE);
    ((FilePacket *) packet)->packet_type = MSG_SEARCH_FILE;
    ((FilePacket *) packet)->inum = 0;

    char dirname[DIRNAMELEN];
    if (CopyFrom(pid, dirname, target, DIRNAMELEN) < 0) {
        return;
    }
    struct inode *parent_inode = GetInode(inum)->inode;

    /* Cannot search inside non-directory. */
    if (parent_inode->type != INODE_DIRECTORY) {
        return;
    }
    int target_inum = SearchDirectory(parent_inode, dirname);

    /* Not found */
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

void CreateFile(void *packet, int pid, short type) {
    int parent_inum = ((DataPacket *)packet)->arg1;
    void *target = ((DataPacket *)packet)->pointer;

    /* Bleach packet for reuse */
    memset(packet, 0, PACKET_SIZE);
    ((FilePacket *)packet)->packet_type = MSG_CREATE_FILE;

    char dirname[DIRNAMELEN];
    if (CopyFrom(pid, dirname, target, DIRNAMELEN) < 0) {
        ((FilePacket *)packet)->inum = 0;
        return;
    }

    struct inode_cache_entry *parent_entry = GetInode(parent_inum);
    struct inode *parent_inode = parent_entry->inode;

    /* Cannot create inside non-directory. */
    if (parent_inode->type != INODE_DIRECTORY) {
        ((FilePacket *)packet)->inum = -1;
        return;
    }

    /* Maximum file size reached. */
    if (parent_inode->size >= MAX_FILE_SIZE) {
        ((FilePacket *)packet)->inum = -2;
        return;
    }

    int target_inum = SearchDirectory(parent_inode, dirname);
    struct inode *new_inode;
    if (target_inum > 0) {
        new_inode = TruncateFileInode(target_inum);
    } else {
        /* If no free inode to spare, error */
        if (free_inode_list->size == 0) {
            ((FilePacket *)packet)->inum = -3;
            return;
        }

        /*
         * Creating a directory will require 1 block.
         * Adding it to parent inode may require 2 blocks.
         */
        if (free_block_list->size < 3) {
            ((FilePacket *)packet)->inum = -4;
            return;
        }

        // Create new file if not found
        target_inum = PopFromBuffer(free_inode_list);
        new_inode = CreateFileInode(target_inum, parent_inum, type);

        /* Child directory refers to parent via .. */
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

void ReadFile(DataPacket *packet, int pid) {
    int inum = packet->arg1;
    int pos = packet->arg2;
    int size = packet->arg3;
    int reuse = packet->arg4;
    void *buffer = packet->pointer;
    // struct inode *inode;

    /* Bleach packet for reuse */
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

    /* If trying to read more than size, adjust size */
    if (inode->size < pos + size) {
    // if (pos + size > inode->size) {
        size = inode->size - pos;

        /* If pos is already beyond size, read 0 */
        if (size <= 0) {
            packet->arg1 = 0;
            return;
        }
    }

    // int inode_block_count = GetBlockCount(inode->size);
    int start_index = pos / BLOCKSIZE; /* Block index where writing starts */
    int end_index = (pos + size) / BLOCKSIZE; /* Block index where writing ends */
    /* ex) if pos = 0 and size = 512, it should only iterate 0 ~ 0 */
    if ((pos + size) % BLOCKSIZE == 0) {
        end_index = end_index - 1;
    }

    /* Just in case there is a hole */
    char hole_buffer[BLOCKSIZE];
    memset(hole_buffer, 0, BLOCKSIZE);

    /* Start reading from the blocks */
    int *indirect_block = NULL;
    // int copysize;

    /* Prefetch indirect block */
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

        /* Use hole block if block does not exist */
        if (block_id != 0) {
            block = GetBlock(block_id)->block;
        }
        else {
            block = hole_buffer;
        }

        /*
         * If current_pos is not divisible by BLOCKSIZE,
         * this does not start from the beginning of the block.
         */
        prefix = (pos + copied_size) % BLOCKSIZE;
        int copysize = BLOCKSIZE - prefix;

        /* Adjust size at last index */
        if (outer_index == end_index) {
            copysize = size - copied_size;
        }

        if (copysize > 0) {
            CopyTo(pid, buffer + copied_size, block + prefix, copysize);
            copied_size += copysize;
        }
    }
    packet->arg1 = copied_size;
}

void WriteFile(DataPacket *packet, int pid) {
    int inum = packet->arg1;
    int pos = packet->arg2;
    int size = packet->arg3;
    int reuse = packet->arg4;
    void *buffer = packet->pointer;
    // struct inode *inode;
    char *block;

    /* Bleach packet for reuse */
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_WRITE_FILE;

    /* Attempting to write beyond max file size */
    if (pos + size > MAX_FILE_SIZE) {
        packet->arg1 = -1;
        return;
    }

    /* Cannot write to non-regular file */
    struct inode_cache_entry *inode_entry = GetInode(inum);
    struct inode *inode;
    // inode_entry = GetInode(inum);
    inode = inode_entry->inode;
    if (inode->type != INODE_REGULAR) {
        packet->arg1 = -2;
        return;
    }

    if (inode->reuse != reuse) {
        packet->arg1 = -3;
        return;
    }

    int start_index = pos / BLOCKSIZE; /* Block index where writing starts */
    int end_index = (pos + size) / BLOCKSIZE; /* Block index where writing ends */
    /* ex) if pos = 0 and size = 512, it should only iterate 0 ~ 0 */
    if ((pos + size) % BLOCKSIZE == 0) {
        end_index = end_index - 1;
    }

    struct block_cache_entry *indirect_block_entry;
    int *indirect_block = NULL;
    /* Prefetch indirect block */
    if (inode->size >= MAX_DIRECT_SIZE) {
        indirect_block = GetBlock(inode->indirect)->block;
    }

    /*
     * Start iterating from whichever the lowest between start and block count.
     * - Increase size if end_index is greater than block_count.
     * - Assign new block only if size is increased and within writing range.
     * - Note: @634 if block is entirely hole, it is 0.
     */
    int inode_block_count = GetBlockCount(inode->size);

    /* Test compute number of additional blocks needed */
    int extra_blocks = 0;
    int i;
    for (i = start_index; i <= end_index; i++) {
        /* Increase the size if current index is less than or equal to block count */
        if (inode_block_count <= i) {
            /* If i is at NUM_DIRECT, it is about to create indirect */
            if (i == NUM_DIRECT) {
                extra_blocks++;
            }

            /* Assign new block if size increased AND is part of writing range */
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
        /* Increase the size if current index is less than or equal to block count */
        if (inode_block_count <= outer_index) {
            /* If i is at NUM_DIRECT, it is about to create indirect */
            if (outer_index == NUM_DIRECT) {
                inode->indirect = PopFromBuffer(free_block_list);
                indirect_block_entry = GetBlock(inode->indirect);
                indirect_block = indirect_block_entry->block;
                indirect_block_entry->dirty = 1;
                memset(indirect_block, 0, BLOCKSIZE);
            }

            /* Constantly get indirect block since it gets dirty many times */
            if (outer_index > NUM_DIRECT) {
                indirect_block_entry = GetBlock(inode->indirect);
                indirect_block = indirect_block_entry->block;
                indirect_block_entry->dirty = 1;
            }

            /* Assign new block if size increased AND is part of writing range */
            if (outer_index >= start_index) {
                if (outer_index >= NUM_DIRECT) {
                    /* Create new block in indirect block. */
                    indirect_block[outer_index - NUM_DIRECT] = PopFromBuffer(free_block_list);
                    block = GetBlock(indirect_block[outer_index - NUM_DIRECT])->block;
                    memset(block, 0, BLOCKSIZE);
                } else {
                    /* Create new block at direct */
                    inode->direct[outer_index] = PopFromBuffer(free_block_list);
                    block = GetBlock(inode->direct[outer_index])->block;
                    memset(block, 0, BLOCKSIZE);
                    inode_entry->dirty = 1;
                }
            }
        }
    }

    /* Start writing in the block */
    // int block_id;
    // int prefix = 0;
    int copied_size = 0; /* Total copied size */
    // int copysize; /* size used for CopyFrom */
    // struct block_cache_entry *block_entry;

    /*
     * New file size based on the write operation.
     * If start writing from index 1, then block size is 512+.
     */
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
        /*
         * If current_pos is not divisible by BLOCKSIZE,
         * this does not start from the beginning of the block.
         */
        int prefix = (pos + copied_size) % BLOCKSIZE;

        /* Prefix should impact new size */
        if (outer_index == start_index) {
            new_size += prefix;
        }

        int copysize = BLOCKSIZE - prefix;
        /* Adjust size at last index */
        if (outer_index == end_index) {
            copysize = size - copied_size;
        }

        CopyFrom(pid, block + prefix, buffer + copied_size, copysize);
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

void DeleteDir(DataPacket *packet) {
    int target_inum = packet->arg1;
    int parent_inum = packet->arg2;

    if (target_inum == ROOTINODE) {
        packet->arg1 = -1;
        return;
    }

    /* Bleach packet for reuse */
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_DELETE_DIR;


    struct inode_cache_entry *parent_entry = GetInode(parent_inum);;
    struct inode *parent_inode = parent_entry->inode;

    /* Parent must be directory */
    if (parent_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -2;
        return;
    }

    struct inode_cache_entry *target_entry = GetInode(target_inum);
    struct inode *target_inode = target_entry->inode;

    /* Cannot call deleteDir on non-directory */
    if (target_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -3;
        return;
    }

    /* There should be . and .. left only */
    if (target_inode->size > DIRSIZE * 2) {
        packet->arg1 = -4;
        return;
    }

    /* Remove inum from parent inode */
    if (UnregisterDirectory(parent_inode, target_inum) < 0) {
        packet->arg1 = -5;
        return;
    }

    target_entry->dirty = 1;
    target_inode->type = INODE_FREE;
    target_inode->size = 0;
    target_inode->nlink = 0;

    /* Clean parent directory */
    parent_entry->dirty = CleanDirectory(parent_inode);

    struct block_cache_entry *block_entry;
    struct dir_entry *block;

    block_entry = GetBlock(target_inode->direct[0]);
    block_entry->dirty = 1;
    block = block_entry->block;

    int i;
    /* Need to decrement nlink of the parent */
    for (i = 0; i < 2; i++) {
        if (block[i].name[1] == '.') {
            target_entry = GetInode(block[i].inum);
            target_entry->inode->nlink -= 1;
            target_entry->dirty = 1;
        }
        block[i].inum = 0;
    }
}

void CreateLink(DataPacket *packet, int pid) {
    int target_inum = packet->arg1;
    int parent_inum = packet->arg2;

    /* Bleach packet for reuse */
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

    /* Already checked by client but just to be sure */
    if (target_inode->type != INODE_REGULAR) {
        packet->arg1 = -2;
        return;
    }

    struct inode_cache_entry *parent_entry = GetInode(parent_inum);
    struct inode *parent_inode = parent_entry->inode;

    /* Already checked by client but just to be sure */
    if (parent_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -3;
        return;
    }

    /*
     * Creating a directory will require 1 block.
     * Adding it to parent inode may require 2 blocks.
     */
    if (free_block_list->size < 2) {
        packet->arg1 = -4;
        return;
    }

    parent_entry->dirty = RegisterDirectory(parent_inode, target_inum, dirname);
    target_entry->dirty = 1;
    target_inode->nlink = target_inode->nlink + 1;
}

void DeleteLink(DataPacket *packet) {
    int target_inum = packet->arg1;
    int parent_inum = packet->arg2;

    /* Bleach packet for reuse */
    memset(packet, 0, PACKET_SIZE);
    packet->packet_type = MSG_UNLINK;
    packet->arg1 = 0;

    struct inode_cache_entry *parent_entry = GetInode(parent_inum);
    struct inode *parent_inode = parent_entry->inode;

    /* Parent must be directory */
    if (parent_inode->type != INODE_DIRECTORY) {
        packet->arg1 = -1;
        return;
    }

    struct inode_cache_entry *target_entry = GetInode(target_inum);
    struct inode *target_inode = target_entry->inode;

    /* Remove inum from parent inode */
    if (UnregisterDirectory(parent_inode, target_inum) < 0) {
        packet->arg1 = -2;
        return;
    }

    target_entry->dirty = 1;
    target_inode->nlink -= 1;

    /* Target Inode is linked no more */
    if (target_inode->nlink == 0) {
        TruncateFileInode(target_inum);
        target_inode->type = INODE_FREE;
        PushToBuffer(free_inode_list, target_inum);
    }

    /* Clean parent directory */
    parent_entry->dirty = CleanDirectory(parent_inode);
}

/**
 * Writes all Dirty Inodes
 */
void SyncCache() {
    /**
     * Synchronize Inodes in Cache to Blocks in Cache
     */
    struct inode_cache_entry* inode;
    //PrintInodeCacheHashSet(inode_stack);
    //PrintInodeCacheStack(inode_stack);
    for (inode = inode_stack->top; inode != NULL; inode = inode->next_lru) {
        if (inode->dirty) {
            struct block_cache_entry* inode_block_entry = GetBlock((inode->inum / 8) + 1);
            inode_block_entry->dirty = 1;
            void* inode_block = inode_block_entry->block;
            struct inode* overwrite = (struct inode *)inode_block + (inode->inum % 8);
            memcpy(overwrite, inode->inode, sizeof(inode));
            inode->dirty = 0;
        }
    }

    /**
     * Synchronize Blocks in Cache to Disk
     */
    struct block_cache_entry* block = block_stack->top;
    while (block != NULL) {
        if (block->dirty) {
            WriteSector(block->block_number, block->block);
            block->dirty = 0;
        }
        block = block->next_lru;
    }
    // for (block = block_stack->top; block != NULL; block = block->next_lru) {
    //     if (block->dirty) {
    //         WriteSector(block->block_number,block->block);
    //         block->dirty = 0;
    //     }
    // }
    return;
}

int main(int argc, char **argv) {
    (void)argc;

    Register(FILE_SERVER);
    /* Obtain File System Header */
    void *sector_one = malloc(SECTORSIZE);
    if (ReadSector(1, sector_one) != 0) {
        printf("Error\n");
    }
    else {
        header = (struct fs_header *)sector_one;
    }
    // if (ReadSector(1, sector_one) == 0) {
    //     header = (struct fs_header *)sector_one;
    // } else {
    //     printf("Error\n");
    // }

    inode_stack = CreateInodeCache(header->num_inodes);
    block_stack = CreateBlockCache(header->num_blocks);
    GetFreeInodeList();
    GetFreeBlockList();

    int pid;
  	if ((pid = Fork()) < 0) {
  	    fprintf(stderr, "Cannot Fork.\n");
  	    return -1;
  	}

    /* Child process exec program */
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

        // switch (((UnknownPacket *)packet)->packet_type) {
        //     case MSG_GET_FILE:
        //         GetFile(packet);
        //         break;
        //     case MSG_SEARCH_FILE:
        //         SearchFile(packet, pid);
        //         break;
        //     case MSG_CREATE_FILE:
        //         CreateFile(packet, pid, INODE_REGULAR);
        //         break;
        //     case MSG_READ_FILE:
        //         ReadFile(packet, pid);
        //         break;
        //     case MSG_WRITE_FILE:
        //         WriteFile(packet, pid);
        //         break;
        //     case MSG_CREATE_DIR:
        //         CreateFile(packet, pid, INODE_DIRECTORY);
        //         break;
        //     case MSG_DELETE_DIR:
        //         DeleteDir(packet);
        //         break;
        //     case MSG_LINK:
        //         CreateLink(packet, pid);
        //         break;
        //     case MSG_UNLINK:
        //         DeleteLink(packet);
        //         break;
        //     case MSG_SYNC:
        //         SyncCache();
        //         if (((DataPacket *)packet)->arg1 == 1) {
        //             Reply(packet, pid);
        //             printf("Shutdown by pid: %d. Bye bye!\n", pid);
        //             Exit(0);
        //         }
        //         break;
        //     default:
        //         continue;
        // }

        if (Reply(packet, pid) < 0) {
            fprintf(stderr, "Reply Error.\n");
            return -1;
        }
    }

    return 0;
};