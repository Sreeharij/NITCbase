#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>    

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // declare searchIndex which will be used to store search index for attrName.
    IndexId searchIndex;

    /* get the search index corresponding to attribute with name attrName
       using AttrCacheTable::getSearchIndex(). */
    int retVal = AttrCacheTable::getSearchIndex(relId,attrName,&searchIndex);
    if(retVal != SUCCESS){
        printf("Search Index Not Available");
    }
    AttrCatEntry attrCatEntry;
    /* load the attribute cache entry into attrCatEntry using
     AttrCacheTable::getAttrCatEntry(). */
    // declare variables block and index which will be used during search
    retVal = AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
    if(retVal != SUCCESS){
        printf("getAttrCatEntry failed from bplustree");
    }
    int block = -1, index = -1;

    if (searchIndex.block == -1 && searchIndex.index == -1){
        // (search is done for the first time)

        // start the search from the first entry of root.
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1) {
            return RecId{-1, -1};
        }

    } else {
        /*a valid searchIndex points to an entry in the leaf index of the attribute's
        B+ Tree which had previously satisfied the op for the given attrVal.*/

        block = searchIndex.block;
        index = searchIndex.index + 1;  // search is resumed from the next index.

        // load block into leaf using IndLeaf::IndLeaf().
        IndLeaf leaf(block);

        // declare leafHead which will be used to hold the header of leaf.
        HeadInfo leafHead;

        // load header into leafHead using BlockBuffer::getHeader().
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            /* (all the entries in the block has been searched; search from the
            beginning of the next leaf index block. */

            // update block to rblock of current block and index to 0.
            block = leafHead.rblock;
            index = 0;

            if (block == -1) {
                // (end of linked list reached - the search is done.)
                return RecId{-1, -1};
            }
        }
    }

    /******  Traverse through all the internal nodes according to value
             of attrVal and the operator op                             ******/

    /* (This section is only needed when
        - search restarts from the root block (when searchIndex is reset by caller)
        - root is not a leaf
        If there was a valid search index, then we are already at a leaf block
        and the test condition in the following loop will fail)
    */

    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL){  //use StaticBuffer::getStaticBlockType()

        // load the block into internalBlk using IndInternal::IndInternal().
        IndInternal internalBlk(block);

        HeadInfo intHead;

        // load the header of internalBlk into intHead using BlockBuffer::getHeader()
        internalBlk.getHeader(&intHead);
        // declare intEntry which will be used to store an entry of internalBlk.
        InternalEntry intEntry;

        if (op == NE or op == LT or op == LE) {
            /*
            - NE: need to search the entire linked list of leaf indices of the B+ Tree,
            starting from the leftmost leaf index. Thus, always move to the left.

            - LT and LE: the attribute values are arranged in ascending order in the
            leaf indices of the B+ Tree. Values that satisfy these conditions, if
            any exist, will always be found in the left-most leaf index. Thus,
            always move to the left.
            */

            // load entry in the first slot of the block into intEntry
            // using IndInternal::getEntry().
            internalBlk.getEntry(&intEntry,0);
            block = intEntry.lChild;

        } else {
            /*
            - EQ, GT and GE: move to the left child of the first entry that is
            greater than (or equal to) attrVal
            (we are trying to find the first entry that satisfies the condition.
            since the values are in ascending order we move to the left child which
            might contain more entries that satisfy the condition)
            */

            /*
             traverse through all entries of internalBlk and find an entry that
             satisfies the condition.
             if op == EQ or GE, then intEntry.attrVal >= attrVal
             if op == GT, then intEntry.attrVal > attrVal
             Hint: the helper function compareAttrs() can be used for comparing
            */
            bool found_flag = false;
            for(int i=0;i<intHead.numEntries;i++){
                internalBlk.getEntry(&intEntry, index);
                int compare = compareAttrs(intEntry.attrVal,attrVal,attrCatEntry.attrType);
                found_flag = (op == EQ && compare == 0) || (op == GE && compare >= 0) || (op == GT || compare > 0);
                if(found_flag)break;
            }
            if (found_flag) {
                // move to the left child of that entry
                block =  intEntry.lChild;

            } else {
                // move to the right child of the last entry of the block
                // i.e numEntries - 1 th entry of the block

                block =  intEntry.rChild;
            }
        }
    }

    // NOTE: `block` now has the block number of a leaf index block.

    /******  Identify the first leaf index entry from the current position
                that satisfies our condition (moving right)             ******/

    while (block != -1) {
        // load the block into leafBlk using IndLeaf::IndLeaf().
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        // load the header to leafHead using BlockBuffer::getHeader().
        leafBlk.getHeader(&leafHead);

        // declare leafEntry which will be used to store an entry from leafBlk
        Index leafEntry;

        while (index < leafHead.numEntries){

            // load entry corresponding to block and index into leafEntry
            // using IndLeaf::getEntry().
            leafBlk.getEntry(&leafEntry,index);
            int cmpVal = compareAttrs(leafEntry.attrVal,attrVal,NUMBER);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                // (entry satisfying the condition found)

                // set search index to {block, index}

                // return the recId {leafEntry.block, leafEntry.slot}.
                searchIndex.block = block;
                searchIndex.index = index;
                AttrCacheTable::setSearchIndex(relId,attrName,&searchIndex);

                return RecId{leafEntry.block,leafEntry.slot};

            } else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                /*future entries will not satisfy EQ, LE, LT since the values
                    are arranged in ascending order in the leaves */

                return RecId {-1, -1};
            }

            // search next index.
            ++index;
        }

        /*only for NE operation do we have to check the entire linked list;
        for all the other op it is guaranteed that the block being searched
        will have an entry, if it exists, satisying that op. */
        if(op != NE) {
            break;
        }

        // block = next block in the linked list, i.e., the rblock in leafHead.
        // update index to 0.
        block = leafHead.rblock;
        index = 0;
    }
    return RecId{-1,-1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {

    // if relId is either RELCAT_RELID or ATTRCAT_RELID:
    //     return E_NOTPERMITTED;
    if(relId == RELCAT_RELID || relId == ATTRCAT_RELID) return E_NOTPERMITTED;

    // get the attribute catalog entry of attribute `attrName`
    // using AttrCacheTable::getAttrCatEntry()
    AttrCatEntry attrCatEntryBuffer;
    int ret = AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntryBuffer);

    // if getAttrCatEntry fails
    //     return the error code from getAttrCatEntry
    if(ret != SUCCESS) return ret;
    if (attrCatEntryBuffer.rootBlock != -1) {
        return SUCCESS;
    }

    /******Creating a new B+ Tree ******/

    // get a free leaf block using constructor 1 to allocate a new block
    IndLeaf rootBlockBuf;

    // (if the block could not be allocated, the appropriate error code
    //  will be stored in the blockNum member field of the object)

    // declare rootBlock to store the blockNumber of the new leaf block
    int rootBlock = rootBlockBuf.getBlockNum();

    // if there is no more disk space for creating an index
    if (rootBlock == E_DISKFULL) {
        return E_DISKFULL;
    }

    attrCatEntryBuffer.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    RelCatEntry relCatEntry;

    // load the relation catalog entry into relCatEntry
    // using RelCacheTable::getRelCatEntry().
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    int block = relCatEntry.firstBlk;

    /***** Traverse all the blocks in the relation and insert them one
           by one into the B+ Tree *****/
    while(block != -1) {

        // declare a RecBuffer object for `block` (using appropriate constructor)
        RecBuffer blockBuffer(block);
        unsigned char slotMap[relCatEntry.numSlotsPerBlk];

        // load the slot map into slotMap using RecBuffer::getSlotMap().
        blockBuffer.getSlotMap(slotMap);

        for(int slot=0; slot < relCatEntry.numSlotsPerBlk; slot++){
            if(slotMap[slot] == SLOT_OCCUPIED){
                Attribute record[relCatEntry.numAttrs];
                // load the record corresponding to the slot into `record`
                // using RecBuffer::getRecord().
                blockBuffer.getRecord(record,slot);
                // declare recId and store the rec-id of this record in it
                RecId recId{block, slot};
                
                // insert the attribute value corresponding to attrName from the record
                // into the B+ tree using bPlusInsert.
                // (note that bPlusInsert will destroy any existing bplus tree if
                // insert fails i.e when disk is full)
                int retVal = bPlusInsert(relId, attrName, record[attrCatEntryBuffer.offset], recId);
                if (retVal == E_DISKFULL) {
                //     // (unable to get enough blocks to build the B+ Tree.)
                    return E_DISKFULL;
                }
            }
        }

        HeadInfo head;
        // get the header of the block using BlockBuffer::getHeader()
        blockBuffer.getHeader(&head);
        block = head.rblock;
        // set block = rblock of current block (from the header)
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF) {
        // declare an instance of IndLeaf for rootBlockNum using appropriate
        // constructor
        IndLeaf leafBlock(rootBlockNum);
        leafBlock.releaseBlock();

        return SUCCESS;

    } else if (type == IND_INTERNAL) {
        // declare an instance of IndInternal for rootBlockNum using appropriate
        // constructor
        IndInternal internalBlock(rootBlockNum);

        // load the header of the block using BlockBuffer::getHeader().
        HeadInfo head;
        internalBlock.getHeader(&head);

        /*iterate through all the entries of the internalBlk and destroy the lChild
        of the first entry and rChild of all entries using BPlusTree::bPlusDestroy().
        (the rchild of an entry is the same as the lchild of the next entry.
         take care not to delete overlapping children more than once ) */

        // release the block using BlockBuffer::releaseBlock().
        InternalEntry internalEntry;
        internalBlock.getEntry(&internalEntry,0);
        BPlusTree::bPlusDestroy(internalEntry.lChild);

        for(int entry = 0;entry < head.numEntries; entry++){
            internalBlock.getEntry(&internalEntry,entry);
            BPlusTree::bPlusDestroy(internalEntry.rChild);
        }

        internalBlock.releaseBlock();
        return SUCCESS;

    } else {
        // (block is not an index block.)
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatBuf;
    int retVal = AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatBuf);
    // if getAttrCatEntry() failed
    //     return the error code
    if(retVal != SUCCESS)return retVal;

    int rootBlockNum = attrCatBuf.rootBlock;

    if (rootBlockNum == -1) {
        return E_NOINDEX;
    }

    int leafBlkNum = findLeafToInsert(rootBlockNum, attrVal, attrCatBuf.attrType);

    // insert the attrVal and recId to the leaf block at blockNum using the
    // insertIntoLeaf() function.

    // declare a struct Index with attrVal = attrVal, block = recId.block and
    // slot = recId.slot to pass as argument to the function.
    // insertIntoLeaf(relId, attrName, leafBlkNum, Index entry)
    // NOTE: the insertIntoLeaf() function will propagate the insertion to the
    //       required internal nodes by calling the required helper functions
    //       like insertIntoInternal() or createNewRoot()

    Index indexEntry;
    indexEntry.attrVal = attrVal;
    indexEntry.block = recId.block;
    indexEntry.slot = recId.slot;
    
    if(insertIntoLeaf(relId,attrName,leafBlkNum,indexEntry) == E_DISKFULL){
        // destroy the existing B+ tree by passing the rootBlock to bPlusDestroy().
        BPlusTree::bPlusDestroy(rootBlockNum);
        // update the rootBlock of attribute catalog cache entry to -1 using
        // AttrCacheTable::setAttrCatEntry().
        attrCatBuf.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId,attrName,&attrCatBuf);
        return E_DISKFULL;
    }
    return SUCCESS;
}


int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;

    while(StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF){  // use StaticBuffer::getStaticBlockType()

        // declare an IndInternal object for block using appropriate constructor
        IndInternal internalIndexBlock(blockNum);
        // get header of the block using BlockBuffer::getHeader()
        HeadInfo head;
        internalIndexBlock.getHeader(&head);
        /* iterate through all the entries, to find the first entry whose
             attribute value >= value to be inserted.
             NOTE: the helper function compareAttrs() declared in BlockBuffer.h
                   can be used to compare two Attribute values. */
        int index = 0;
        while(index < head.numEntries){
            InternalEntry internalIndexEntry;
            internalIndexBlock.getEntry(&internalIndexEntry,index);
            if(compareAttrs(internalIndexEntry.attrVal, attrVal, attrType) >= 0){
                break;
            }
            index++;
        }
        if(index == head.numEntries){
            // set blockNum = rChild of (nEntries-1)'th entry of the block
            // (i.e. rightmost child of the block)
            InternalEntry internalIndexEntry;
            internalIndexBlock.getEntry(&internalIndexEntry,head.numEntries-1);
            blockNum = internalIndexEntry.rChild;
        } 
        else{
            InternalEntry internalIndexEntry;
            internalIndexBlock.getEntry(&internalIndexEntry,index);
            blockNum = internalIndexEntry.lChild;
        }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
    // declare an IndLeaf instance for the block using appropriate constructor
    IndLeaf leafBlock(blockNum);
    HeadInfo blockHeader;
    // store the header of the leaf index block into blockHeader
    // using BlockBuffer::getHeader()
    leafBlock.getHeader(&blockHeader);
    // the following variable will be used to store a list of index entries with
    // existing indices + the new index to insert
    Index indices[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array indices.
    Also insert `indexEntry` at appropriate position in the indices array maintaining
    the ascending order.
    - use IndLeaf::getEntry() to get the entry
    - use compareAttrs() declared in BlockBuffer.h to compare two Attribute structs
    */
   bool inserted = false;
   for(int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++){
       Index entry;
       leafBlock.getEntry(&entry, entryindex);

       if (compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatEntry.attrType) <= 0)
       {
           // the current entry element is lesser than the one to be inserted
           indices[entryindex] = entry;
       }
       else
       {
           // the indexEntry is to be placed here
           indices[entryindex] = indexEntry;
           // insertmarker++;
           inserted = true;

            for(entryindex++; entryindex <= blockHeader.numEntries; entryindex++){
                leafBlock.getEntry(&entry, entryindex-1);
                indices[entryindex] = entry;
            }
            break;
        }
    }

    if (!inserted) indices[blockHeader.numEntries] = indexEntry;

    if (blockHeader.numEntries != MAX_KEYS_LEAF) {
        // (leaf block has not reached max limit)

        // increment blockHeader.numEntries and update the header of block
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);

        for(int i=0;i<blockHeader.numEntries;i++){
            leafBlock.setEntry(&indices[i],i);
        }
        // iterate through all the entries of the array `indices` and populate the
        // entries of block with them using IndLeaf::setEntry().

        return SUCCESS;
    }

    // If we reached here, the `indices` array has more than entries than can fit
    // in a single leaf index block. Therefore, we will need to split the entries
    // in `indices` between two leaf blocks. We do this using the splitLeaf() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitLeaf(blockNum, indices);

    // if splitLeaf() returned E_DISKFULL
    //     return E_DISKFULL
    if(newRightBlk == E_DISKFULL)return E_DISKFULL;

    int retVal = SUCCESS;
    if (blockHeader.pblock != -1){  // check pblock in header
        // insert the middle value from `indices` into the parent block using the
        // insertIntoInternal() function. (i.e the last value of the left block)

        // the middle value will be at index 31 (given by constant MIDDLE_INDEX_LEAF)

        // create a struct InternalEntry with attrVal = indices[MIDDLE_INDEX_LEAF].attrVal,
        // lChild = currentBlock, rChild = newRightBlk and pass it as argument to
        // the insertIntoInternalFunction as follows
        InternalEntry middleEntry;
        middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        middleEntry.lChild = blockNum;
        middleEntry.rChild = newRightBlk;

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
        retVal = insertIntoInternal(relId,attrName,blockHeader.pblock,middleEntry);

    } else {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments
        int retVal = createNewRoot(relId,attrName,indices[MIDDLE_INDEX_LEAF].attrVal,blockNum,newRightBlk);
        // createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal,
        //               current block, new right block)
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
    if(retVal != SUCCESS) return retVal;
    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting
    IndLeaf rightBlock;

    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block
    IndLeaf leftBlock(leafBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = leftBlock.getBlockNum();

    if (rightBlockNum == E_DISKFULL){
        //(failed to obtain a new leaf index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlock.getHeader(&leftBlkHeader);
    rightBlock.getHeader(&rightBlkHeader);
    // get the headers of left block and right block using BlockBuffer::getHeader()

    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32,
    // - pblock = pblock of leftBlk
    // - lblock = leftBlkNum
    // - rblock = rblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()
    
    rightBlkHeader.blockType = leftBlkHeader.blockType, 
    rightBlkHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    
    rightBlkHeader.pblock = leftBlkHeader.pblock, 
    rightBlkHeader.lblock = leftBlockNum, 
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    
    rightBlock.setHeader(&rightBlkHeader);

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - rblock = rightBlkNum
    // and update the header of leftBlk using BlockBuffer::setHeader() */

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF+1)/2;
    leftBlkHeader.rblock = rightBlockNum;

    leftBlock.setHeader(&leftBlkHeader);

    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry().

    for(int i = 0; i < MIDDLE_INDEX_LEAF; i++){
        leftBlock.setEntry(&indices[i], i);
        rightBlock.setEntry(&indices[i + MIDDLE_INDEX_LEAF+1], i);
    }

    return rightBlockNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    // declare intBlk, an instance of IndInternal using constructor 2 for the block
    // corresponding to intBlockNum
    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);
    IndInternal internalBlock(intBlockNum);
    HeadInfo blockHeader;
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().
    internalBlock.getHeader(&blockHeader);
    
    // declare internalEntries to store all existing entries + the new entry
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array
    `internalEntries`. Insert `indexEntry` at appropriate position in the
    array maintaining the ascending order.
        - use IndInternal::getEntry() to get the entry
        - use compareAttrs() to compare two structs of type Attribute

    Update the lChild of the internalEntry immediately following the newly added
    entry to the rChild of the newly added entry.
    */
    int insertedIndex = -1;
    for(int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++){
        InternalEntry internalBlockEntry;
        internalBlock.getEntry(&internalBlockEntry, entryindex);

        if (compareAttrs(internalBlockEntry.attrVal, intEntry.attrVal, attrCatEntryBuffer.attrType) <= 0){
            // the new value is lesser (or equal to), hence does not go here
            internalEntries[entryindex] = internalBlockEntry;
        }
        else{
            internalEntries[entryindex] = intEntry;
            insertedIndex = entryindex;
            // internalBlock.getEntry(&internalBlockEntry, entryindex+1);
            // internalBlockEntry.lChild = intEntry.rChild;

            for(entryindex++; entryindex <= blockHeader.numEntries; entryindex++){
                internalBlock.getEntry(&internalBlockEntry, entryindex-1);
                internalEntries[entryindex] = internalBlockEntry;
            }

            break;
        }
    }
    if(insertedIndex == -1){
        internalEntries[blockHeader.numEntries] = intEntry;
        insertedIndex = blockHeader.numEntries;
    }

    if(insertedIndex > 0){
        internalEntries[insertedIndex-1].rChild = intEntry.lChild;
    }

    if(insertedIndex < blockHeader.numEntries){
        internalEntries[insertedIndex+1].lChild = intEntry.rChild;
    }

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL) {
        // (internal index block has not reached max limit)

        // increment blockheader.numEntries and update the header of intBlk
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        internalBlock.setHeader(&blockHeader);
        // iterate through all entries in internalEntries array and populate the
        // entries of intBlk with them using IndInternal::setEntry().
        for(int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++){
            internalBlock.setEntry(&internalEntries[entryindex], entryindex);
        }
        return SUCCESS;
    }

    // If we reached here, the `internalEntries` array has more than entries than
    // can fit in a single internal index block. Therefore, we will need to split
    // the entries in `internalEntries` between two internal index blocks. We do
    // this using the splitInternal() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL){

        // Using bPlusDestroy(), destroy the right subtree, rooted at intEntry.rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree
        BPlusTree::bPlusDestroy(intEntry.rChild);

        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1) {  // (check pblock in header)
        // insert the middle value from `internalEntries` into the parent block
        // using the insertIntoInternal() function (recursively).

        // the middle value will be at index 50 (given by constant MIDDLE_INDEX_INTERNAL)
        InternalEntry middleEntry;
        middleEntry.lChild = intBlockNum;
        middleEntry.rChild = newRightBlk;
        middleEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        // create a struct InternalEntry with lChild = current block, rChild = newRightBlk
        // and attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal
        // and pass it as argument to the insertIntoInternalFunction as follows
        return insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)

    } 
    else{
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName,
        //               internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,
        //               current block, new right block)
        return createNewRoot(relId,attrName,internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,intBlockNum,newRightBlk);
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    // declare rightBlk, an instance of IndInternal using constructor 1 to obtain new
    // internal index block that will be used as the right block in the splitting

    // declare leftBlk, an instance of IndInternal using constructor 2 to read from
    // the existing internal index block
    IndInternal rightBlock;
    IndInternal leftBlock(intBlockNum);
    
    int rightBlkNum = rightBlock.getBlockNum();
    int leftBlkNum = leftBlock.getBlockNum();

    if (rightBlkNum == E_DISKFULL) {
        //(failed to obtain a new internal index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()
    leftBlock.getHeader(&leftBlkHeader);
    rightBlock.getHeader(&rightBlkHeader);
    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // - pblock = pblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()
    rightBlkHeader.numEntries = MAX_KEYS_INTERNAL/2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlock.setHeader(&rightBlkHeader);
    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // and update the header using BlockBuffer::setHeader()
    leftBlkHeader.numEntries = MAX_KEYS_INTERNAL/2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlock.setHeader(&leftBlkHeader);
    /*
    - set the first 50 entries of leftBlk = index 0 to 49 of internalEntries
      array
    - set the first 50 entries of newRightBlk = entries from index 51 to 100
      of internalEntries array using IndInternal::setEntry().
      (index 50 will be moving to the parent internal index block)
    */
    for(int entryindex = 0; entryindex < MIDDLE_INDEX_INTERNAL; entryindex++){
        leftBlock.setEntry(&internalEntries[entryindex], entryindex);
        rightBlock.setEntry(&internalEntries[entryindex + MIDDLE_INDEX_INTERNAL + 1], entryindex);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);
    //            (use StaticBuffer::getStaticBlockType())

    BlockBuffer blockbuffer(internalEntries[MIDDLE_INDEX_INTERNAL+1].lChild);

    HeadInfo blockHeader;
    blockbuffer.getHeader(&blockHeader);

    blockHeader.pblock = rightBlkNum;
    blockbuffer.setHeader(&blockHeader);

    for(int entryindex = 0; entryindex < MIDDLE_INDEX_INTERNAL; entryindex++){
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2
        BlockBuffer blockbuffer(internalEntries[entryindex + MIDDLE_INDEX_INTERNAL + 1].rChild);
        blockbuffer.getHeader(&blockHeader);
        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
        blockHeader.pblock = rightBlkNum;
        blockbuffer.setHeader(&blockHeader);
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    // to allocate a new internal index block on the disk
    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);
    IndInternal newRootBlock;
    int newRootBlkNum = newRootBlock.getBlockNum();

    if (newRootBlkNum == E_DISKFULL) {
        // (failed to obtain an empty internal index block because the disk is full)

        // Using bPlusDestroy(), destroy the right subtree, rooted at rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree

        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    // update the header of the new block with numEntries = 1 using
    // BlockBuffer::getHeader() and BlockBuffer::setHeader()
    HeadInfo blockHeader;
    newRootBlock.getHeader(&blockHeader);

    blockHeader.numEntries = 1;
    newRootBlock.setHeader(&blockHeader);
    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()

    InternalEntry internalentry;
    internalentry.lChild = lChild;
    internalentry.rChild = rChild;
    internalentry.attrVal = attrVal;
    newRootBlock.setEntry(&internalentry, 0);

    // declare BlockBuffer instances for the `lChild` and `rChild` blocks using
    // appropriate constructor and update the pblock of those blocks to `newRootBlkNum`
    // using BlockBuffer::getHeader() and BlockBuffer::setHeader()
    BlockBuffer leftChildBlock(lChild);
    BlockBuffer rightChildBlock(rChild);

    HeadInfo leftChildHeader, rightChildHeader;
    leftChildBlock.getHeader(&leftChildHeader);
    rightChildBlock.getHeader(&rightChildHeader);

    leftChildHeader.pblock = newRootBlkNum;
    rightChildHeader.pblock = newRootBlkNum;

    leftChildBlock.setHeader(&leftChildHeader);
    rightChildBlock.setHeader(&rightChildHeader);
    // update rootBlock = newRootBlkNum for the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().
    attrCatEntryBuffer.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName,  &attrCatEntryBuffer);
    return SUCCESS;
}