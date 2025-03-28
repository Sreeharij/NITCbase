#include "RelCacheTable.h"
#include <stdio.h>
#include <cstring>

RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];

int RelCacheTable::getSearchIndex(int relId, RecId* searchIndex){
    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }
    if(relCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }
    *searchIndex = relCache[relId]->searchIndex;
    return SUCCESS;
}

int RelCacheTable::setSearchIndex(int relId, RecId* searchIndex){
    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }
    if(relCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }
    relCache[relId]->searchIndex = *searchIndex;
    return SUCCESS;
}

int RelCacheTable::resetSearchIndex(int relId){
    RecId dummy{-1,-1};
    return setSearchIndex(relId,&dummy);    
}

int RelCacheTable::getRelCatEntry(int relId, RelCatEntry* relCatBuf){
    if(relId < 0 || relId > MAX_OPEN){
        return E_OUTOFBOUND;
    }

    if(relCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }
    
    *relCatBuf = relCache[relId]->relCatEntry;

    return SUCCESS;
}

void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],RelCatEntry* relCatEntry){
    strcpy(relCatEntry->relName,record[RELCAT_REL_NAME_INDEX].sVal);
    relCatEntry->numAttrs = record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    relCatEntry->numRecs = record[RELCAT_NO_RECORDS_INDEX].nVal;
    relCatEntry->firstBlk = record[RELCAT_FIRST_BLOCK_INDEX].nVal;
    relCatEntry->lastBlk = record[RELCAT_LAST_BLOCK_INDEX].nVal;
    relCatEntry->numSlotsPerBlk = record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}

int RelCacheTable::setRelCatEntry(int relId, RelCatEntry *relCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN)return E_OUTOFBOUND;

    if(RelCacheTable::relCache[relId] == nullptr)return E_RELNOTOPEN;

    // copy the relCatBuf to the corresponding Relation Catalog entry in
    // the Relation Cache Table.
    RelCacheTable::relCache[relId]->relCatEntry = *relCatBuf;

    // set the dirty flag of the corresponding Relation Cache entry in
    // the Relation Cache Table.
    RelCacheTable::relCache[relId]->dirty = true;

    return SUCCESS;
}

void RelCacheTable::relCatEntryToRecord(RelCatEntry *relCatEntry, union Attribute record[RELCAT_NO_ATTRS]){
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal,relCatEntry->relName);
    record[RELCAT_FIRST_BLOCK_INDEX].nVal = relCatEntry->firstBlk;
    record[RELCAT_LAST_BLOCK_INDEX].nVal = relCatEntry->lastBlk;
    record[RELCAT_NO_ATTRIBUTES_INDEX].nVal = relCatEntry->numAttrs;
    record[RELCAT_NO_RECORDS_INDEX].nVal = relCatEntry->numRecs;
    record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = relCatEntry->numSlotsPerBlk;    
}