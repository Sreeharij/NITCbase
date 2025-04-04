#include "Buffer/StaticBuffer.h"
#include "Buffer/BlockBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
using std::cout;
using std::endl;

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  return FrontendInterface::handleFrontend(argc,argv);
  
  /*
  RelCatEntry relCatEntry;
  AttrCatEntry attrCatEntry;
  for(int i=RELCAT_RELID; i<= ATTRCAT_RELID; i++){
    RelCacheTable::getRelCatEntry(i,&relCatEntry);
    printf("Relation: %s\n",relCatEntry.relName);

    for(int j=0;j<relCatEntry.numAttrs;j++){
      AttrCacheTable::getAttrCatEntry(i,j,&attrCatEntry);
      printf(" %s: %s\n",attrCatEntry.attrName,attrCatEntry.attrType == 0 ? "NUM" : "STR");
    }
    printf("\n");
  }

  */

  /*
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;  

  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  for(int i=0;i<relCatHeader.numEntries;i++){
    Attribute relCatRecord[RELCAT_NO_ATTRS]; //RELCAT_NO_ATTRS = 6 is defined since 6 attributes for a record in record catalog
    relCatBuffer.getRecord(relCatRecord,i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal); //RELCAT_REL_NAME_INDEX = 0 defined for relation name
    for(int j=0;j<attrCatHeader.numEntries;j++){
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord,j);

      if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0){
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf(" %s: %s\n",attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,attrType);
      }
    }
    printf("\n");
  }
  return 0;
  */
}
