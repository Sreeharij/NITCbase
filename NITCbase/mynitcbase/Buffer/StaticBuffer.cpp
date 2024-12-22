#include "StaticBuffer.h"
#include <limits.h>
#include <string.h>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

//ALERT: MAKE SURE OF THE VALIDITY OF USING MEMCPY IN THE BELOW IMPLEMENTATION (IS IT CORRECT FULLY?)
StaticBuffer::StaticBuffer(){
    unsigned char buffer[BLOCK_SIZE];
    for(int blockNum = 0; blockNum < 4; blockNum ++){
        Disk::readBlock(buffer,blockNum);
        memcpy(blockAllocMap + BLOCK_SIZE*blockNum,buffer,BLOCK_SIZE);
    }
    
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY;bufferIndex++){
        metainfo[bufferIndex].free = true;
        metainfo[bufferIndex].dirty = false;
        metainfo[bufferIndex].timeStamp = -1;
        metainfo[bufferIndex].blockNum = -1;
    }

}

StaticBuffer::~StaticBuffer(){
    unsigned char buffer[BLOCK_SIZE];
    for(int blockNum = 0; blockNum < 4;blockNum++){
        memcpy(buffer,blockAllocMap + BLOCK_SIZE*blockNum, BLOCK_SIZE);
        Disk::writeBlock(buffer,blockNum);
    }

    for(int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++){
        if(metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty == true){
            Disk::writeBlock(blocks[bufferIndex],metainfo[bufferIndex].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum){
    if(blockNum < 0 || blockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }
    for(int bufferIndex=0;bufferIndex < BUFFER_CAPACITY; bufferIndex++){
        if(!metainfo[bufferIndex].free){
            metainfo[bufferIndex].timeStamp++;
        }
    }
    int bufferNum = BUFFER_CAPACITY;
    
    for(int i=0;i<BUFFER_CAPACITY;i++){
        if(metainfo[i].free){
            bufferNum = i;
            break;
        }
    }
    if(bufferNum == BUFFER_CAPACITY){
        int largestTimeStamp = INT_MIN;
        for(int bufferIndex = 0;bufferIndex < BUFFER_CAPACITY;bufferIndex++){
            if(metainfo[bufferIndex].timeStamp > largestTimeStamp){
                largestTimeStamp = metainfo[bufferIndex].timeStamp;
                bufferNum = bufferIndex;
            }
        }
        if(metainfo[bufferNum].dirty){
            Disk::writeBlock(blocks[bufferNum],metainfo[bufferNum].blockNum);
        }
    }

    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].blockNum = blockNum;
    metainfo[bufferNum].timeStamp = 0;

    return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum){
    if(blockNum < 0 || blockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    for(int i=0;i<BUFFER_CAPACITY;i++){
        if(metainfo[i].blockNum == blockNum){
            return i;
        }
    }

    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    int bufferNum = getBufferNum(blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER){
        return E_BLOCKNOTINBUFFER;
    }
    if(bufferNum == E_OUTOFBOUND){
        return E_OUTOFBOUND;
    }
    metainfo[bufferNum].dirty = true;
    return SUCCESS;
}
