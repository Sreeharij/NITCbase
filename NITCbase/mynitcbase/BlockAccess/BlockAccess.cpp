#include "BlockAccess.h"
#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op){
    
    RecId lastRecId;
    RelCacheTable::getSearchIndex(relId,&lastRecId);
    int block = -1;
    int slot = -1;

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);

    if(lastRecId.block == -1 && lastRecId.slot == -1){
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else{
        block = lastRecId.block;
        slot = lastRecId.slot + 1;
    }
//ALERT    
//I AM MAKING A MODIFICATION SO THAT IT CHECKS IF slot >= numSlots
    while(block != -1){
        RecBuffer recordBlock(block);
        HeadInfo head;

        recordBlock.getHeader(&head);

        unsigned char slotMap[head.numSlots];
        recordBlock.getSlotMap(slotMap);

        // if(slot >= relCatEntry.numSlotsPerBlk){
        if(slot >= head.numSlots){
            block = head.rblock;
            slot = 0;
            continue;
        }
        
        if(slotMap[slot] == SLOT_UNOCCUPIED){
            slot += 1;
            continue;
        }

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
        int attrOffset = attrCatEntry.offset;

        Attribute record[head.numAttrs];
        recordBlock.getRecord(record,slot);

        int cmpVal = compareAttrs(record[attrOffset],attrVal,attrCatEntry.attrType);
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ){
            RecId currentRecord{block,slot};
            RelCacheTable::setSearchIndex(relId,&currentRecord);

            return currentRecord;
        }
        slot++;
    }
    RelCacheTable::resetSearchIndex(relId);
    return RecId{-1,-1};
}