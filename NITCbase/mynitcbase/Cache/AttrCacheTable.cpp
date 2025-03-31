#include "AttrCacheTable.h"

#include <cstring>
AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf){
    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next){
        if(entry->attrCatEntry.offset == attrOffset){
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf){
    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }
    for(AttrCacheEntry* entry = attrCache[relId];entry != nullptr; entry = entry->next){
        if(strcmp(entry->attrCatEntry.attrName,attrName) == 0){
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],AttrCatEntry* attrCatEntry){
    strcpy(attrCatEntry->relName,record[ATTRCAT_REL_NAME_INDEX].sVal);
    strcpy(attrCatEntry->attrName,record[ATTRCAT_ATTR_NAME_INDEX].sVal);
    attrCatEntry->attrType = record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
    attrCatEntry->primaryFlag = record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
    attrCatEntry->rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    attrCatEntry->offset = record[ATTRCAT_OFFSET_INDEX].nVal;
}

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {

    if(relId < 0 or relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }

    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *attrCacheEntry = AttrCacheTable::attrCache[relId];

    while (attrCacheEntry) {
        if (strcmp(attrCacheEntry->attrCatEntry.attrName, attrName) == 0){
            //copy the searchIndex field of the corresponding Attribute Cache entry
            //in the Attribute Cache Table to input searchIndex variable.
            *searchIndex = attrCacheEntry->searchIndex;
            return SUCCESS;
        }
        attrCacheEntry = attrCacheEntry->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex){
    if (relId < 0 or relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *attrCacheEntry = AttrCacheTable::attrCache[relId];
    while(attrCacheEntry){
        if(attrOffset == attrCacheEntry->attrCatEntry.offset){
            *searchIndex = attrCacheEntry->searchIndex;
            return SUCCESS;
        }
        attrCacheEntry = attrCacheEntry->next;
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {
    if (relId < 0 or relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *attrCacheEntry = AttrCacheTable::attrCache[relId];
    while (attrCacheEntry) {
        if (strcmp(attrCacheEntry->attrCatEntry.attrName, attrName) == 0) {
            attrCacheEntry->searchIndex = *searchIndex;
            return SUCCESS;
        }
        attrCacheEntry = attrCacheEntry->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId,int attrOffset, IndexId *searchIndex){
    if (relId < 0 or relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }
    AttrCacheEntry *attrCacheEntry = AttrCacheTable::attrCache[relId];
    while(attrCacheEntry){
        if(attrOffset == attrCacheEntry->attrCatEntry.offset){
            attrCacheEntry->searchIndex = *searchIndex;
            return SUCCESS;
        }
        attrCacheEntry = attrCacheEntry->next;
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {
  IndexId index = {-1, -1};
  return AttrCacheTable::setSearchIndex(relId, attrName, &index);
}

int AttrCacheTable::resetSearchIndex(int relId,int attrOffset) {
  IndexId index = {-1, -1};
  return AttrCacheTable::setSearchIndex(relId,attrOffset,&index);
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN){
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr){
    return E_RELNOTOPEN;
  }

  for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next){
    if(entry->attrCatEntry.offset == attrOffset){
        // copy the attrCatBuf to the corresponding Attribute Catalog entry in
        // the Attribute Cache Table.
        strcpy(entry->attrCatEntry.relName,attrCatBuf->relName);
        strcpy(entry->attrCatEntry.attrName,attrCatBuf->attrName);
        entry->attrCatEntry.attrType = attrCatBuf->attrType;
        entry->attrCatEntry.offset = attrCatBuf->offset;
        entry->attrCatEntry.primaryFlag = attrCatBuf->primaryFlag;
        entry->attrCatEntry.rootBlock = attrCatBuf->rootBlock;
        // set the dirty flag of the corresponding Attribute Cache entry in the
        // Attribute Cache Table.
        entry->dirty = true;
        return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN){
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr){
    return E_RELNOTOPEN;
  }

  for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next){
    if(strcmp(entry->attrCatEntry.attrName, attrName) == 0){
        // copy the attrCatBuf to the corresponding Attribute Catalog entry in
        // the Attribute Cache Table.
        strcpy(entry->attrCatEntry.relName,attrCatBuf->relName);
        strcpy(entry->attrCatEntry.attrName,attrCatBuf->attrName);
        entry->attrCatEntry.attrType = attrCatBuf->attrType;
        entry->attrCatEntry.offset = attrCatBuf->offset;
        entry->attrCatEntry.primaryFlag = attrCatBuf->primaryFlag;
        entry->attrCatEntry.rootBlock = attrCatBuf->rootBlock;
        // set the dirty flag of the corresponding Attribute Cache entry in the
        // Attribute Cache Table.
        entry->dirty = true;
        return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, Attribute record[ATTRCAT_NO_ATTRS]){
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal,attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal,attrCatEntry->attrName);

    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;
}