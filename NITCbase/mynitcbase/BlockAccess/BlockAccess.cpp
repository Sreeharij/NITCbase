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

    int flag = SUCCESS;
    for(int attrOffset=0; attrOffset < numOfAttributes;attrOffset++){
        AttrCatEntry attrCatEntryBuffer;
        AttrCacheTable::getAttrCatEntry(relId,attrOffset,&attrCatEntryBuffer);
        int rootBlock = attrCatEntryBuffer.rootBlock;
        if(rootBlock != -1){
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntryBuffer.attrName,record[attrOffset], rec_id);
            if(retVal == E_DISKFULL){
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }
    return SUCCESS; 
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* get the attribute catalog entry from the attribute cache corresponding
    to the relation with Id=relid and with attribute_name=attrName  */
    AttrCatEntry attrCatEntry;
    int ret=AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
    // if this call returns an error, return the appropriate error code
    if(ret!=SUCCESS){
		return ret;
	}
    // get rootBlock from the attribute catalog entry
    int rootBlock=attrCatEntry.rootBlock;
    if(rootBlock == -1){
        recId=BlockAccess::linearSearch(relId,attrName,attrVal,op);
    }
    else{
        recId = BPlusTree::bPlusSearch(relId,attrName,attrVal,op);
    }

    if(recId.block == -1 && recId.slot == -1){
        return E_NOTFOUND;
    }

    RecBuffer recordBlock(recId.block);
    recordBlock.getRecord(record,recId.slot);
    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0)return E_NOTPERMITTED;
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
    strcpy(relNameAttr.sVal,relName);

    //  linearSearch on the relation catalog for RelName = relNameAttr
    char relcatAttrRelname[] = RELCAT_ATTR_RELNAME;
    RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID,relcatAttrRelname,relNameAttr,EQ);
    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
    if(relCatRecId.block == -1 || relCatRecId.slot == -1) return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer relCatBlockBuffer(relCatRecId.block);
    relCatBlockBuffer.getRecord(relCatEntryRecord,relCatRecId.slot);

    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */
    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttributes = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    
    /*
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    //     get block header using BlockBuffer.getHeader
    //     get the next block from the header (rblock)
    //     release the block using BlockBuffer.releaseBlock
    //
    //     Hint: to know if we reached the end, check if nextBlock = -1

    int currentBlock = firstBlock;
    while(currentBlock != -1){
        RecBuffer blockBuffer(currentBlock);
        HeadInfo head;
        blockBuffer.getHeader(&head);
        int nextBlock = head.rblock;
        blockBuffer.releaseBlock();
        currentBlock = nextBlock;
    }


    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);    

    int numberOfAttributesDeleted = 0;

    while(true) {
        // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
        RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,relcatAttrRelname,relNameAttr,EQ);
        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;
        if(attrCatRecId.block != -1 || attrCatRecId.slot != -1){
            break;
        }

        numberOfAttributesDeleted++;

        // create a RecBuffer for attrCatRecId.block
        // get the header of the block
        // get the record corresponding to attrCatRecId.slot
        RecBuffer attrCatBlockBuffer(attrCatRecId.block);
        HeadInfo head;
        attrCatBlockBuffer.getHeader(&head);

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlockBuffer.getRecord(attrCatRecord,attrCatRecId.slot);

        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.
        int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        // (This will be used later to delete any indexes if it exists)

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
        unsigned char slotmap[head.numSlots];
        attrCatBlockBuffer.getSlotMap(slotmap);

        slotmap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        attrCatBlockBuffer.setSlotMap(slotmap);
        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */
        head.numEntries--;
        attrCatBlockBuffer.setHeader(&head);

        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (head.numEntries == 0) {
            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */
            // create a RecBuffer for lblock and call appropriate methods
            RecBuffer prevBlock(head.lblock);
            HeadInfo leftHeader;
            prevBlock.getHeader(&leftHeader);
            leftHeader.rblock = head.rblock;
            prevBlock.setHeader(&leftHeader);


            if (head.rblock != -1) {
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods
                RecBuffer nextBlock(head.rblock);
                HeadInfo rightHeader;
                nextBlock.getHeader(&rightHeader);
                rightHeader.lblock = head.lblock;
                nextBlock.setHeader(&rightHeader);

            } 
            else {
                // (the block being released is the "Last Block" of the relation.)
                /* update the Relation Catalog entry's LastBlock field for this
                   relation with the block number of the previous block. */
                RelCatEntry relCatEntryBuffer;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&relCatEntryBuffer);
                relCatEntryBuffer.lastBlk = head.lblock;
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            // call releaseBlock()
            attrCatBlockBuffer.releaseBlock();
        }

        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        if (rootBlock != -1) {
            BPlusTree::bPlusDestroy(rootBlock);
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
        }
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block

    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */
    HeadInfo relCatHeader;
    relCatBlockBuffer.getHeader(&relCatHeader);

    relCatHeader.numEntries--;
    relCatBlockBuffer.setHeader(&relCatHeader);

    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */
    unsigned char slotmap[relCatHeader.numSlots];
    relCatBlockBuffer.getSlotMap(slotmap);

    slotmap[relCatRecId.slot] = SLOT_UNOCCUPIED;
    relCatBlockBuffer.setSlotMap(slotmap);

    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    RelCatEntry relCatEntryBuffer;
    RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCatEntryBuffer);
    relCatEntryBuffer.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID,&relCatEntryBuffer);

    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted

    RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&relCatEntryBuffer);
    relCatEntryBuffer.numRecs -= numberOfAttributesDeleted;    
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&relCatEntryBuffer);
    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId lastRecId;
    int retVal = RelCacheTable::getSearchIndex(relId,&lastRecId);
    if(retVal != SUCCESS)return retVal;
    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block=lastRecId.block, slot=lastRecId.slot;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (lastRecId.block == -1 && lastRecId.slot == -1){
        // (new project operation. start from beginning)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId,&relCatEntry);
        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)

        // block = first record block of the relation
        // slot = 0
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else{
        // (a project/search operation is already in progress)

        // block = previous search index's block
        // slot = previous search index's slot + 1
        block = lastRecId.block;
        slot = lastRecId.slot + 1;
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1){
        // create a RecBuffer object for block (using appropriate constructor!)
        RecBuffer recordBlock(block);
        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function
        HeadInfo head;
        recordBlock.getHeader(&head);
        unsigned char slotMap[head.numSlots];
        recordBlock.getSlotMap(slotMap);

        if(slot >= head.numSlots){
            // (no more slots in this block)
            // update block = right block of block
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
            block = head.rblock;
            slot = 0;
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED){ 
            // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)
            // increment slot
            slot++;
        }
        else {
            // (the next occupied slot / record has been found)
            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};
    // set the search index to nextRecId using RelCacheTable::setSearchIndex
    RelCacheTable::setSearchIndex(relId,&nextRecId);

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recordBlock(block);
    recordBlock.getRecord(record,slot);

    return SUCCESS;
}