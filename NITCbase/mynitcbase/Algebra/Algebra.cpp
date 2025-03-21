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

    /*** Creating and opening the target relation ***/
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&srcRelCatEntry);
    int src_nAttrs = srcRelCatEntry.numAttrs;

    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];

    for(int i=0;i<src_nAttrs;i++){
        AttrCatEntry srcAttrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId,i,&srcAttrCatEntry);
        strcpy(attr_names[i],srcAttrCatEntry.attrName);
        attr_types[i] = srcAttrCatEntry.attrType;
    }

    ret = Schema::createRel(targetRel,src_nAttrs,attr_names,attr_types);
    if(ret != SUCCESS) return ret;

    int targetRelId = OpenRelTable::openRel(targetRel);    
    if(targetRelId < 0 || targetRelId >= MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    Attribute record[src_nAttrs];

    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);

    while(BlockAccess::search(srcRelId,record,attr,attrVal,op) == SUCCESS){
        int ret = BlockAccess::insert(targetRelId,record);
        if(ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    Schema::closeRel(targetRel);

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

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    int srcRelId = OpenRelTable::getRelId(srcRel);

    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    if(srcRelId < 0 || srcRelId >= MAX_OPEN) return E_RELNOTOPEN;
    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry srcRelCatEntry;
    int ret = RelCacheTable::getRelCatEntry(srcRelId,&srcRelCatEntry);
    if(ret != SUCCESS)return ret;
    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int numAttrs = srcRelCatEntry.numAttrs;
    // attrNames and attrTypes will be used to store the attribute names
    // and types of the source relation respectively
    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    /*iterate through every attribute of the source relation :
        - get the AttributeCat entry of the attribute with offset.
          (using AttrCacheTable::getAttrCatEntry())
        - fill the arrays `attrNames` and `attrTypes` that we declared earlier
          with the data about each attribute
    */
    for(int i=0;i<numAttrs;i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
        strcpy(attrNames[i],attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
        
    }

    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()

    // if the createRel returns an error code, then return that value.
    ret = Schema::createRel(targetRel,numAttrs,attrNames,attrTypes);
    if(ret != SUCCESS) return ret;

    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target relid

    // If opening fails, delete the target relation by calling Schema::deleteRel() of
    // return the error value returned from openRel().

    int targetRelId = OpenRelTable::openRel(targetRel);
    if(targetRelId < 0 || targetRelId >= MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[numAttrs];


    while (BlockAccess::project(srcRelId, record) == SUCCESS){
        // record will contain the next record

        ret = BlockAccess::insert(targetRelId, record);

        if(ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {

    int srcRelId = OpenRelTable::getRelId(srcRel);

    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    if(srcRelId < 0 || srcRelId >= MAX_OPEN)return E_RELNOTOPEN;
    // get RelCatEntry of srcRel using 
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&srcRelCatEntry);
    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int srcNoAttrs = srcRelCatEntry.numAttrs;
    
    // declare attr_offset[tar_nAttrs] an array of type int.
    // where i-th entry will store the offset in a record of srcRel for the
    // i-th attribute in the target relation.
    int attr_offset[tar_nAttrs];

    // let attr_types[tar_nAttrs] be an array of type int.
    // where i-th entry will store the type of the i-th attribute in the
    // target relation.
    int attr_types[tar_nAttrs];


    /*** Checking if attributes of target are present in the source relation
         and storing its offsets and types ***/

    /*iterate through 0 to tar_nAttrs-1 :
        - get the attribute catalog entry of the attribute with name tar_attrs[i].
        - if the attribute is not found return E_ATTRNOTEXIST
        - fill the attr_offset, attr_types arrays of target relation from the
          corresponding attribute catalog entries of source relation
    */
    for(int i=0;i<tar_nAttrs;i++){
        AttrCatEntry attrCatEntry;
        if(AttrCacheTable::getAttrCatEntry(srcRelId,tar_Attrs[i],&attrCatEntry) == E_ATTRNOTEXIST){
            return E_ATTRNOTEXIST;
        }
        attr_offset[i] = attrCatEntry.offset;
        attr_types[i] = attrCatEntry.attrType;
    }


    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()
    int ret = Schema::createRel(targetRel,tar_nAttrs,tar_Attrs,attr_types);
    // if the createRel returns an error code, then return that value.
    if(ret != SUCCESS) return ret;

    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target relid
    // If opening fails, delete the target relation by calling Schema::deleteRel()
    // and return the error value from openRel()

    int targetRelId = OpenRelTable::openRel(targetRel);
    if(targetRelId < 0 || targetRelId >= MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[srcNoAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS) {
        // the variable `record` will contain the next record
        Attribute proj_record[tar_nAttrs];
        //iterate through 0 to tar_attrs-1:
        //    proj_record[attr_iter] = record[attr_offset[attr_iter]]

        for(int i=0;i<tar_nAttrs;i++){
            proj_record[i] = record[attr_offset[i]];
        }

        ret = BlockAccess::insert(targetRelId, proj_record);

        if (ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);
    return SUCCESS;
}