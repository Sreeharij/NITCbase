#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>

//THROUGHOUT THIS FILE MIND THE HANDLING OF STRUCT POINTERS AND OBJECTS
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable(){
    for(int i=0;i<MAX_OPEN;i++){
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        //ALERT: im doing .free = true instead of .free = FREE. keep in mind
        tableMetaInfo[i].free = true;
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
        ll_pointer = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
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

    /************ Setting up tableMetaInfo entries ************/
    tableMetaInfo[RELCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);

    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);

}

OpenRelTable::~OpenRelTable(){
    for(int i=2;i<MAX_OPEN;i++){
        if(!tableMetaInfo[i].free){
            OpenRelTable::closeRel(i);
        }
    }

    for(int relId=RELCAT_RELID;relId<+ATTRCAT_RELID;relId++){
        free(RelCacheTable::relCache[relId]);

        AttrCacheEntry *ptr = AttrCacheTable::attrCache[relId];
        AttrCacheEntry *trail = nullptr;

        while(ptr){
            trail = ptr;
            ptr = ptr->next;
            free(trail);
        }

        RelCacheTable::relCache[relId] = nullptr;
        AttrCacheTable::attrCache[relId] = nullptr;
        tableMetaInfo[relId].free = true;
        strcpy(tableMetaInfo[relId].relName,"");

    }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]){
    for(int i=0;i<MAX_OPEN;i++){
        if(!tableMetaInfo[i].free){
            if(strcmp(tableMetaInfo[i].relName,relName) == 0){
                return i;
            }
        }
    }
    
    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
  // if found return the relation id, else return E_CACHEFULL.
  for(int i=0;i<MAX_OPEN;i++){
    if(tableMetaInfo[i].free){
        return i;
    }
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]){
    int relId = OpenRelTable::getRelId(relName);
    if(relId != E_RELNOTOPEN){
        return relId;
    }
    
    relId = OpenRelTable::getFreeOpenRelTableEntry();
    if(relId == E_CACHEFULL){
        return E_CACHEFULL;
    }

     /****** Setting up Relation Cache entry for the relation ******/
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal,relName);
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    char relcatAttrRelname[] = RELCAT_ATTR_RELNAME;
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID,relcatAttrRelname,relNameAttr,EQ);
    if(relcatRecId.block == -1 && relcatRecId.slot == -1){
        return E_RELNOTEXIST;
    }

    RecBuffer relCatBlock(relcatRecId.block);
    HeadInfo head;
    relCatBlock.getHeader(&head);
    
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord,relcatRecId.slot);

    RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord,&relCacheEntry.relCatEntry);
    relCacheEntry.recId = relcatRecId;
    RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = relCacheEntry;

    /****** Setting up Attribute Cache entry for the relation ******/
    int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
    struct AttrCacheEntry *attrCacheEntry = nullptr;
    struct AttrCacheEntry *ll_pointer = nullptr;
    attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    ll_pointer = attrCacheEntry;
    for(int node_no=1;node_no<numberOfAttributes;node_no++){
        ll_pointer->next = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        ll_pointer = ll_pointer->next;
    }
    ll_pointer->next = nullptr;
    ll_pointer = attrCacheEntry;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    //reusing relNameAttr from above
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    char attrcatAttrRelname[] = ATTRCAT_ATTR_RELNAME;
    while(numberOfAttributes--){
        RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,attrcatAttrRelname,relNameAttr,EQ);
        RecBuffer attrCatBlock(attrcatRecId.block);
        attrCatBlock.getRecord(attrCatRecord,attrcatRecId.slot);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&ll_pointer->attrCatEntry);
        ll_pointer->recId.block = attrcatRecId.block;
        ll_pointer->recId.slot = attrcatRecId.slot;

        ll_pointer = ll_pointer->next;
    }
    AttrCacheTable::attrCache[relId] = attrCacheEntry;
    /****** Setting up metadata in the Open Relation Table for the relation******/
    tableMetaInfo[relId].free = false;
    strcpy(tableMetaInfo[relId].relName,relName);
    return relId;
}

int OpenRelTable::closeRel(int relId){
    if(relId == RELCAT_RELID || relId == ATTRCAT_RELID) return E_NOTPERMITTED;
    if(relId < 0 || relId >= MAX_OPEN) return E_OUTOFBOUND;  
    if(tableMetaInfo[relId].free) return E_RELNOTOPEN;

    if(RelCacheTable::relCache[relId]->dirty){
        Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry),record);
    
        RecId recId = RelCacheTable::relCache[relId]->recId;
        RecBuffer relCatBlock(recId.block);

        relCatBlock.setRecord(record,recId.slot);
    }

    free(RelCacheTable::relCache[relId]);
    
    AttrCacheEntry *ptr = AttrCacheTable::attrCache[relId];
    AttrCacheEntry *trail = nullptr;
    while(ptr){
        trail = ptr;
        ptr = ptr->next;
        free(trail);
    }
    AttrCacheTable::attrCache[relId] = nullptr;
    RelCacheTable::relCache[relId] = nullptr;
    tableMetaInfo[relId].free = true;
    strcpy(tableMetaInfo[relId].relName,"");

    return SUCCESS;
}