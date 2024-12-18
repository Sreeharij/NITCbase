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