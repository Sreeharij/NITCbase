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

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newRelationName;
    strcpy(newRelationName.sVal,newName);

    char relcatAttrRelname[] = RELCAT_ATTR_RELNAME;
    char attrcatAttrRelname[] = ATTRCAT_ATTR_RELNAME;
    RecId searchIndex = BlockAccess::linearSearch(RELCAT_RELID,relcatAttrRelname,newRelationName,EQ);
        
    if(!(searchIndex.block == -1 && searchIndex.slot == -1)){
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    
    Attribute oldRelationName;
    strcpy(oldRelationName.sVal,oldName);
    
    searchIndex = BlockAccess::linearSearch(RELCAT_RELID,relcatAttrRelname,oldRelationName,EQ);
    
    if(searchIndex.block == -1 && searchIndex.slot == -1){
        return E_RELNOTEXIST;
    }
    
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(relCatRecord,searchIndex.slot);
    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,newName);
    relCatBuffer.setRecord(relCatRecord,searchIndex.slot);
    
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    for(int attributeNumber = 0;attributeNumber < relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; attributeNumber++){
        searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID,attrcatAttrRelname,oldRelationName,EQ);
        RecBuffer attrCatBuffer(searchIndex.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord,searchIndex.slot);
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,newName);
        attrCatBuffer.setRecord(attrCatRecord,searchIndex.slot);
    }
    
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;    // set relNameAttr to relName
    strcpy(relNameAttr.sVal,relName);
    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    char relcatAttrRelname[] = RELCAT_ATTR_RELNAME;
    char attrcatAttrRelname[] = ATTRCAT_ATTR_RELNAME;
    RecId searchIndex = BlockAccess::linearSearch(RELCAT_RELID,relcatAttrRelname,relNameAttr,EQ);
    if(searchIndex.block == -1 && searchIndex.slot == -1){
        return E_RELNOTEXIST;
    }

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr
        searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID,attrcatAttrRelname,relNameAttr,EQ);
        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
        if(searchIndex.block == -1 && searchIndex.slot == -1){
            break;
        }

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        RecBuffer attrCatBuffer(searchIndex.block);
        attrCatBuffer.getRecord(attrCatEntryRecord,searchIndex.slot);

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldName) == 0){
            attrToRenameRecId = searchIndex;
            //ALERT: NO BREAKING condition, check if this works correctly
        }
        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName) == 0){
            return E_ATTREXIST;
        }
    }

    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1){
        return E_ATTRNOTEXIST;
    }
    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord

    RecBuffer attrToRenameBlockBuffer(attrToRenameRecId.block);
    attrToRenameBlockBuffer.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);
    attrToRenameBlockBuffer.setRecord(attrCatEntryRecord,attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId,&relCatBuf);
    
    int blockNum = relCatBuf.firstBlk;

    // rec_id will be used to store where the new record will be inserted
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatBuf.numSlotsPerBlk;
    int numOfAttributes = relCatBuf.numAttrs;

    int prevBlockNum = -1;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    while (blockNum != -1) {
        // create a RecBuffer object for blockNum (using appropriate constructor!)
        RecBuffer blockBuffer(blockNum);
        // get header of block(blockNum) using RecBuffer::getHeader() function
        HeadInfo head;
        blockBuffer.getHeader(&head);
        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char slotMap[numOfSlots];
        blockBuffer.getSlotMap(slotMap);
        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */
        int freeSlot = -1;
        for(int slotNum = 0;slotNum < numOfSlots; slotNum++){
            if(slotMap[slotNum] == SLOT_UNOCCUPIED){
                freeSlot = slotNum;
                break;
            }
        }
        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */
        if(freeSlot != -1){
            rec_id = {blockNum,freeSlot};
            break;
        }

        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
        prevBlockNum = blockNum;
        blockNum = head.rblock;
    }
    if(rec_id.block == -1 && rec_id.slot == -1){
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;
        if(relId == RELCAT_RELID) return E_MAXRELATIONS;
        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call
        RecBuffer blockBuffer;
        int ret = blockBuffer.getBlockNum();
        if(ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
        rec_id.block = ret;
        rec_id.slot = 0;
        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */
       HeadInfo head;
       head.blockType = REC;
       head.rblock = head.pblock = -1;
       head.numEntries = 0;
       head.numAttrs = numOfAttributes;
       head.numSlots = numOfSlots;
       head.lblock = prevBlockNum;
       blockBuffer.setHeader(&head);

        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */
       unsigned char slotMap[numOfSlots];
       for(int i=0;i<numOfSlots;i++){
        slotMap[i] = SLOT_UNOCCUPIED;
       }
       blockBuffer.setSlotMap(slotMap);
        if(prevBlockNum != -1)
        {
            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)
            RecBuffer prevBlockNumBuffer(prevBlockNum);

            HeadInfo prevBlockHead;
            prevBlockNumBuffer.getHeader(&prevBlockHead);
            prevBlockHead.rblock = ret;
            prevBlockNumBuffer.setHeader(&prevBlockHead);
        }
        else{
            relCatBuf.firstBlk = ret;
            //ALERT: I DIDNT DO RelCacheTable::setRelCatEntry, check if any error arises
            RelCacheTable::setRelCatEntry(relId,&relCatBuf);
        }
        relCatBuf.lastBlk = ret;
        RelCacheTable::setRelCatEntry(relId,&relCatBuf);
        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
    }

    // create a RecBuffer object for rec_id.block
    // insert the record into rec_id'th slot using RecBuffer.setRecord())
    RecBuffer blockBuffer(rec_id.block);
    blockBuffer.setRecord(record,rec_id.slot);

    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
    unsigned char slotMap[numOfSlots];
    blockBuffer.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    blockBuffer.setSlotMap(slotMap);
    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    HeadInfo head;
    blockBuffer.getHeader(&head);
    head.numEntries++;
    blockBuffer.setHeader(&head);
    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)
    relCatBuf.numRecs++;
    RelCacheTable::setRelCatEntry(relId,&relCatBuf);
    return SUCCESS;
}
