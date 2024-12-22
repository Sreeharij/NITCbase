#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdio.h>
//ALERT: FAILURE CODE IS NOT RETURNED IN CODE(anywhere) check

BlockBuffer::BlockBuffer(int blockNum){
    // initialise this.blockNum with the argument
    if(blockNum < 0 || blockNum >= DISK_BLOCKS){
        this->blockNum = blockNum;    
    }
    else{
        this->blockNum = blockNum;
    }

}

BlockBuffer::BlockBuffer(char blockType){
    int blockTypeInt = -1;
    if(blockType == 'R')blockTypeInt = REC;
    else if(blockType == 'I')blockTypeInt = IND_INTERNAL;
    else if(blockType == 'L')blockTypeInt = IND_LEAF;
    else{
        printf("Error From BlockBuffer Constructor");
    }
    int freeBlock = BlockBuffer::getFreeBlock(blockTypeInt);

    if(freeBlock < 0 || freeBlock >= DISK_BLOCKS){
        printf("Error occured from BlockBuffer 2 Constructor");
    }
    //IF SOMETHING ERROR OCCURS, the this->blockNum field would contain the error code.
    this->blockNum = freeBlock;

}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R') {}
int BlockBuffer::getHeader(struct HeadInfo *head){
    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    memcpy(&head->numSlots,bufferPtr + 24,4);
    memcpy(&head->numEntries,bufferPtr + 16,4);
    memcpy(&head->numAttrs,bufferPtr + 20,4);
    memcpy(&head->rblock,bufferPtr + 12,4);
    memcpy(&head->lblock,bufferPtr + 8,4);

    return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum){
    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;
    this->getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    int recordSize = attrCount * ATTR_SIZE;
    unsigned char *slotPointer = bufferPtr + HEADER_SIZE + slotCount + (recordSize * slotNum);

    memcpy(rec,slotPointer,recordSize);

    return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr){
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER){
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
        
        if(bufferNum == E_OUTOFBOUND){
            return E_OUTOFBOUND;
        }

        Disk::readBlock(StaticBuffer::blocks[bufferNum],this->blockNum);
    }
    else{
        for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++){
            StaticBuffer::metainfo[bufferIndex].timeStamp++;
        }
        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }
    *buffPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char* slotMap){
    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;
    this->getHeader(&head);
    int slotCount = head.numSlots;

    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
    
    for(int slot = 0;slot < slotCount; slot++){
        *(slotMap + slot) = *(slotMapInBuffer + slot);
    }

    return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType){
    if(attrType == NUMBER){
        return attr1.nVal > attr2.nVal ? 1 : (attr1.nVal < attr2.nVal ? -1 : 0);
    }
    return strcmp(attr1.sVal,attr2.sVal);
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum){
    unsigned char* bufferPtr;
    int retVal = loadBlockAndGetBufferPtr(&bufferPtr);
    if(retVal != SUCCESS) return retVal;
    HeadInfo head;
    this->getHeader(&head);
    int numAttributes = head.numAttrs;
    int numSlots = head.numSlots;

    if(slotNum < 0 || slotNum >= numSlots) return E_OUTOFBOUND;
    int recordSize = ATTR_SIZE * numAttributes;
    unsigned char* offset = bufferPtr + HEADER_SIZE + numSlots + recordSize*slotNum;
    memcpy(offset,rec,recordSize);

    int ret = StaticBuffer::setDirtyBit(this->blockNum);
    if(ret != SUCCESS){
        printf("Error in code, arised from RecBuffer::setRecord function");
        return ret;
    }

    return SUCCESS;
}   

int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    int retVal = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(retVal != SUCCESS) return retVal;

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
    bufferHeader->blockType = head->blockType;
    bufferHeader->pblock = head->pblock;
    bufferHeader->lblock = head->lblock;
    bufferHeader->rblock = head->rblock;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->numSlots = head->numSlots;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code
    return StaticBuffer::setDirtyBit(this->blockNum);
    
    // return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int retVal = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(retVal != SUCCESS) return retVal;

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;
    *((int32_t *)bufferPtr) = blockType;

    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call
    return StaticBuffer::setDirtyBit(this->blockNum);
    // return SUCCESS
}

int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
    int freeBlock = -1;
    for(int blockNum = 0; blockNum < DISK_BLOCKS; blockNum++){
        if(StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK){
            freeBlock = blockNum;
            break;
        }
    }

    // if no block is free, return E_DISKFULL.
    if(freeBlock == -1) return E_DISKFULL;
    // set the object's blockNum to the block number of the free block.
    this->blockNum = freeBlock;
    // find a free buffer using StaticBuffer::getFreeBuffer() .
    int bufferIndex = StaticBuffer::getFreeBuffer(blockNum);

	if (bufferIndex < 0 && bufferIndex >= BUFFER_CAPACITY) {
		printf ("Error from getFreeBlock function\n");
		return bufferIndex;
	}
    HeadInfo head;
    head.pblock = -1;
    head.lblock = -1;
    head.rblock = -1;
    head.numEntries = 0;
    head.numAttrs = 0;
    head.numSlots = 0;
    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.
        
    // update the block type of the block to the input block type using setBlockType().
    setHeader(&head);
    setBlockType(blockType);

    // return block number of the free block.
    return blockNum;
}
int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
    int retVal = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(retVal != SUCCESS) return retVal;

    // get the header of the block using the getHeader() function
    HeadInfo head;
    getHeader(&head);
    int numSlots = head.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the exis`ting slotmap.
    // Note that size of slotmap is `numSlots`
    memcpy(bufferPtr + HEADER_SIZE,slotMap,numSlots);

    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call
    // return SUCCESS
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getBlockNum(){
    return this->blockNum;
}