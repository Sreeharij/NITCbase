#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdio.h>
//ALERT: FAILURE CODE IS NOT RETURNED IN CODE(anywhere) check

BlockBuffer::BlockBuffer(int blockNum){
    // initialise this.blockNum with the argument
    this->blockNum = blockNum;    

}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

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