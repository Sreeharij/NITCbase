//ALERT: understand where we need to call the reset searchindex

#include "Algebra.h"

#include <cstring>
#include <stdlib.h>
#include <stdio.h>

bool isNumber(char *str);

int Algebra::select(char srcRel[ATTR_SIZE],char targetRel[ATTR_SIZE],char attr[ATTR_SIZE],int op, char strVal[ATTR_SIZE]){
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId,attr,&attrCatEntry);
    if(ret == E_ATTRNOTEXIST){
        return E_ATTRNOTEXIST;
    }

    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if(type == NUMBER){
        if(isNumber(strVal)){
            attrVal.nVal = atof(strVal);
        }
        else{
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if(type == STRING){
        strcpy(attrVal.sVal,strVal);
    }

    RelCacheTable::resetSearchIndex(srcRelId);
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);

    printf("|");
    for(int i=0;i<relCatEntry.numAttrs;i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
        printf(" %s |", attrCatEntry.attrName);
    }
    printf("\n");
    while(true){
        RecId searchRes = BlockAccess::linearSearch(srcRelId,attr,attrVal,op);

        if(searchRes.block != -1 && searchRes.slot != -1){
            AttrCatEntry attrCatEntry;

            RecBuffer recordEntry(searchRes.block);
            
            HeadInfo head;
            recordEntry.getHeader(&head);

            Attribute rowVals[relCatEntry.numAttrs];
            recordEntry.getRecord(rowVals,searchRes.slot);
            
            printf("|");

            for(int i=0;i<relCatEntry.numAttrs;i++){
                AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
                if(attrCatEntry.attrType == NUMBER){
                    printf(" %d |", (int)rowVals[i].nVal);
                }
                else{
                    printf(" %s |", rowVals[i].sVal);
                }
            }
            printf("\n");
        }
        else{
            break;
        }
    }
    return SUCCESS;
}   

bool isNumber(char *str){
    int len;
    float ignore;
    /*
        sscanf returns the number of elements read, so if there is no float matching
        the first %f, ret will be 0, else it'll be 1

        %n gets the number of characters read. this scanf sequence will read the
        first float ignoring all the whitespace before and after. and the number of
        characters read that far will be stored in len. if len == strlen(str), then
        the string only contains a float with/without whitespace. else, there's other
        characters.
    */

    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
    // return E_NOTPERMITTED;
    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0){
        return E_NOTPERMITTED;
    }
    // get the relation's rel-id using OpenRelTable::getRelId() method
    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN)return E_RELNOTOPEN;
    
    
    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);

    /* if relCatEntry.numAttrs != numberOfAttributes in relation,
       return E_NATTRMISMATCH */
    if(relCatEntry.numAttrs != nAttrs) return E_NATTRMISMATCH;

    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];
    /*
        Converting 2D char array of record values to Attribute array recordValues
     */
    // iterate through 0 to nAttrs-1: (let i be the iterator)
    for(int recordValueIdx = 0; recordValueIdx < nAttrs; recordValueIdx++){
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId,recordValueIdx,&attrCatEntry);

        int type = attrCatEntry.attrType;
        if (type == NUMBER){
            if(isNumber(record[recordValueIdx])){
                recordValues[recordValueIdx].nVal = atof(record[recordValueIdx]);
            }
            else{
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING){
            strcpy(recordValues[recordValueIdx].sVal,record[recordValueIdx]);
            // copy record[i] to recordValues[i].sVal
        }
    }

    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call
    int retVal = BlockAccess::insert(relId,recordValues);
    return retVal;
}
