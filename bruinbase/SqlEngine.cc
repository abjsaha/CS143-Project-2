/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string.h>
#include <strings.h>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
using namespace std;

// external functions and variables for load file and sql command parsing
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
    fprintf(stdout, "Bruinbase> ");
    
    // set the command line input and start parsing user input
    sqlin = commandline;
    sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
    // SqlParser.y by bison (bison is GNU equivalent of yacc)
    
    return 0;
}

typedef struct
{
    int set;
    int value;
    int canEqual;
} rangeSet;

void initRangeSet(rangeSet &t)
{
    t.set = 0;
    t.value = 0;
    t.canEqual = 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
    RecordFile rf;   // RecordFile containing the table
    RecordId   rid;  // record cursor for table scanning
    
    BTreeIndex bpt; //B+tree
    
    RC     rc;
    int keyFound = 0;//Check if key already found;
    int hasIndex = 1;//Check if table has index;
    int noresult = 0;//Search on key yields no result
    int    key;
    string value;
    int    count;
    int    diff;
    
    //Variables for index
    IndexCursor cursor; //Cursor for B+Tree
    RecordId min_rid, max_rid;
    int is_key_comparison = 0;
    int keyRangeSet = 0;
    
    //flags to see if min or max was set on key
    rangeSet max_key;
    rangeSet min_key;
    int maxkeyint;
    initRangeSet(max_key);
    initRangeSet(min_key);
    
    // open the index file
    if ((hasIndex = bpt.open(table + ".idx", 'r')) < 0) {
        hasIndex = 0;
        bpt.close();
    }
    else
        hasIndex = 1;
    
    // open the table file
    if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
        fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
        return rc;
    }
    
    //Initialize structures to optimize query
    if (hasIndex)
    {
        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
            int value = atoi(cond[i].value); //obtain value to be compared
            
            //check key comparisons
            if (cond[i].attr == 1){
                is_key_comparison = 1;
                switch (cond[i].comp) {
                    case SelCond::EQ: //If key is set to equal, it either exists or doesn't and then query must end
                        if (bpt.locate(value, cursor) == 0)
                        {
                            //Handle case with more than one equals on a key
                            if (keyFound == 1 && value != key)
                                goto no_result;
                            
                            key = value;
                            keyFound = 1;
                        }
                        else
                            goto no_result; //If doesn't exist, then return none
                        break;
                    case SelCond::NE:
                        break;
                    case SelCond::GT: //Max key is set to iterate through recordfile
                        if (value >= min_key.value || min_key.set==0){
                            min_key.value = value;
                            min_key.set=1;
                            min_key.canEqual = 0;
                        }
                        break;
                    case SelCond::GE: //Min key is set to iterate through recordfile
                        if (value > min_key.value || min_key.set==0){
                            min_key.value = value;
                            min_key.set=1;
                            min_key.canEqual = 1;
                        }
                        break;
                    case SelCond::LT: //Max key is set to iterate through recordfile
                        if (value <= max_key.value || max_key.set==0){
                            max_key.value = value;
                            max_key.set=1;
                            max_key.canEqual = 0;
                        }
                        break;
                    case SelCond::LE: //Min key is set to iterate through recordfile
                        if (value < max_key.value || max_key.set==0){
                            max_key.value = value;
                            max_key.set=1;
                            max_key.canEqual = 1;
                        }
                        break;
                }
            }
        }
        
        //Check that key ranges are valid
        if (min_key.set && max_key.set)
        {
            keyRangeSet = 1;
            if (max_key.value < min_key.value)
                goto no_result;
            
            if ((!max_key.canEqual || !min_key.canEqual) && min_key.value == max_key.value)
                goto no_result;
        }
    }
    
    //Set upper and lower bounds
    if (keyRangeSet)
    {
        if (max_key.canEqual)
            maxkeyint = max_key.value + 1;
        else
            maxkeyint = max_key.value;
    }
    
    // scan the table file from the beginning if no index exists
    //If index exists, check if value range was specified
    if (!keyFound && !keyRangeSet)
        rid.pid = rid.sid = 0;
    else if (!keyFound && keyRangeSet)//Set starting rid
    {
        key = min_key.value;
        if (!min_key.canEqual)
            key++;
        
        //find first available tuple with min key
        while((bpt.locate(key, cursor) != 0) && key<maxkeyint)
            key++;
        
        //read key
        bpt.readForward(cursor, key, rid);
    }
    else//If key found, read that specific key
        bpt.readForward(cursor, key, rid);
    count = 0;
    while (rid < rf.endRid() && (keyRangeSet ? key<maxkeyint : 1)) {
        // read the tuple
        if ((rc = rf.read(rid, key, value)) < 0) {
            fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
            goto exit_select;
        }
        
        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
            // compute the difference between the tuple value and the condition value
            switch (cond[i].attr) {
                case 1:
                    diff = key - atoi(cond[i].value);
                    break;
                case 2:
                    diff = strcmp(value.c_str(), cond[i].value);
                    break;
            }
            
            // skip the tuple if any condition is not met
            switch (cond[i].comp) {
                case SelCond::EQ:
                    if (diff != 0) goto next_tuple;
                    break;
                case SelCond::NE:
                    if (diff == 0) goto next_tuple;
                    break;
                case SelCond::GT:
                    if (diff <= 0) goto next_tuple;
                    break;
                case SelCond::LT:
                    if (diff >= 0) goto next_tuple;
                    break;
                case SelCond::GE:
                    if (diff < 0) goto next_tuple;
                    break;
                case SelCond::LE:
                    if (diff > 0) goto next_tuple;
                    break;
            }
        }
        
        // the condition is met for the tuple.
        // increase matching tuple counter
        count++;
        
        // print the tuple
        switch (attr) {
            case 1:  // SELECT key
                fprintf(stdout, "%d\n", key);
                break;
            case 2:  // SELECT value
                fprintf(stdout, "%s\n", value.c_str());
                break;
            case 3:  // SELECT *
                fprintf(stdout, "%d '%s'\n", key, value.c_str());
                break;
        }
        
        // move to the next tuple
    next_tuple:
        if (keyFound)
            break;
        if (!keyRangeSet)
            ++rid;
        else //condition if keyRange is set
        {
            //Iterate until next element is found or max key is reached
            while((bpt.locate(++key, cursor) != 0) && key<maxkeyint);
            bpt.readForward(cursor, key, rid);
        }
        
    }
    // print matching tuple count if "select count(*)"
    if (attr == 4) {
        fprintf(stdout, "%d\n", count);
    }
    rc = 0;
    
    // close the table file and return
exit_select:
    if (attr == 4 && noresult)
        fprintf(stdout, "0\n");
    rf.close();
    return rc;
    
no_result:
    noresult=1;
    goto exit_select;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
    int debug = 0;
    RecordFile record_file;
    RecordId record_id;
    RC rc;
    BTreeIndex bpt;
    //open file stream
    ifstream file(loadfile.c_str());
    //open loadfile
    if(!file.is_open()) {
        if (debug)
            fprintf(stderr, "Could Not Open File Error: %s\n", loadfile.c_str());
    }
    //open table file
    if((rc=record_file.open(table+".tbl", 'w'))<0) {
        if (debug)
            fprintf(stderr, "Could Not Create or Write to Table: %s\n", table.c_str());
    }
    //check index option
    if(index) {
        if((rc=bpt.open(table+".idx", 'w'))<0) {
            if (debug)
                fprintf(stderr, "Could Not Create or Write to Index for Table: %s\n", table.c_str());
        }
    }
    //iterate through tuples
    while(!file.eof()) {
        int key;
        string line, value;
        getline(file, line);
        
        if((rc=parseLoadLine(line, key, value))<0) {
            if (debug)
                fprintf(stderr, "Could not parse line from File: %s\n", loadfile.c_str());
            break;
        }
        //ignore empty lines
        if(key!=0||strcmp(value.c_str(), "")!=0) {
            //write to table
            if((rc=record_file.append(key, value, record_id))<0) {
                if (debug)
                    fprintf(stderr, "Could not insert key: %s\n", key);
                break;
            }
            if(index) {
                if((rc=bpt.insert(key, record_id))<0) {
                    if (debug)
                        fprintf(stderr, "Could not write to Index for Table\n");
                    break;
                }
                if (debug)
                    printf("Inserting: %d\n", key);
            }
        }
    }
    record_file.close();
    file.close();
    if(index) {
        bpt.close();
    }
    return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }
    
    // get the integer key value
    key = atoi(s);
    
    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }
    
    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) {
        value.erase();
        return 0;
    }
    
    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }
    
    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }
    
    return 0;
}
