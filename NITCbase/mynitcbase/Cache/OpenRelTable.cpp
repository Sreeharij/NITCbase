#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>

//THROUGHOUT THIS FILE MIND THE HANDLING OF STRUCT POINTERS AND OBJECTS

OpenRelTable::OpenRelTable(){
    for(int i=0;i<MAX_OPEN;i++){
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }
    RecBuffer relCatBlock(RELCAT_BLOCK);

    Attribute relCatRecord[RELCAT_NO_ATTRS];

    struct RelCacheEntry relCacheEntry;
    
    relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

    RelCacheTable::recordToRelCatEntry(relCatRecord,&relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

    
    relCatBlock.getRecord(relCatRecord,RELCAT_SLOTNUM_FOR_ATTRCAT);
    RelCacheTable::recordToRelCatEntry(relCatRecord,&relCacheEntry.relCatEntry);

    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

    RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;

    /************ Setting up Attribute cache entries ************/
   
    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    
    struct AttrCacheEntry *attrCacheEntry = nullptr;
    struct AttrCacheEntry *ll_pointer = nullptr;

    int attribute_catalog_record_id = 0;
    for(int relId = RELCAT_RELID; relId <= ATTRCAT_RELID; relId++){
        int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
        ll_pointer = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        attrCacheEntry = ll_pointer;

        for(int node_no = 1; node_no < numberOfAttributes; node_no++){
            ll_pointer->next = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
            ll_pointer = ll_pointer->next;
        }
        ll_pointer->next = nullptr;
        ll_pointer = attrCacheEntry; //ll_pointer points to the head now
        
        while(numberOfAttributes--){
            attrCatBlock.getRecord(attrCatRecord,attribute_catalog_record_id);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&(ll_pointer->attrCatEntry));
            ll_pointer->recId.slot = attribute_catalog_record_id++;
            ll_pointer->recId.block = ATTRCAT_BLOCK;
            ll_pointer = ll_pointer->next;
        }
        AttrCacheTable::attrCache[relId] = attrCacheEntry;
    }
}

OpenRelTable::~OpenRelTable(){}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]){
    if(strcmp(relName,RELCAT_RELNAME) == 0) return RELCAT_RELID;
    if(strcmp(relName,ATTRCAT_RELNAME) == 0) return ATTRCAT_RELID;
    
    return E_RELNOTOPEN;
}
