/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    if(pf.open(indexname, mode)!=0)
    	return DEFAULT_ERROR_CODE;
    if(pf.endPid()>0) {
    	char buffer[DEFAULT_SIZE];
    	pf.read(TREE_PAGE, buffer);
    	bTreeInfo* info=(bTreeInfo*) buffer;
    	treeHeight=info->totalHeight;
    	rootPid=info->rootPid;
    }
    return 0;

}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    char buffer[DEFAULT_SIZE];
    pf.read(TREE_PAGE, buffer);
    bTreeInfo* info=(bTreeInfo*) buffer;
    info->totalHeight=treeHeight;
    info->rootPid=rootPid;
    pf.write(TREE_PAGE, buffer);
    return pf.close();
}

// /*
//  * Insert (key, RecordId) pair to the index.
//  * @param key[IN] the key for the value inserted into the index
//  * @param rid[IN] the RecordId for the record being inserted into the index
//  * @return error code. 0 if no error
//  */
// RC BTreeIndex::insert(int key, const RecordId& rid)
// {
//     IndexCursor cursor;
    
//     //SET CURSOR TO IMMEDIATELY AFTER LARGEST INDEX KEY
//     locate(key, cursor);
    
//     //INSERT INTO KEY, RECORD INTO LEFTNODE CURSOR POINTS TO
//     BTLeafNode* node = new BTLeafNode();
    
//     //LOAD NODE FROM .idx FILE
//     node->read(cursor.pid, pf);
    
//     //IF SPACE AVAILABLE ON LEAF NODE, SIMPLE ADD CASE
//     if (node->getKeyCount() < node->getMaxKeyCount())
//     {
//         node->insert(key, rid);
//         return 0;
//     }
    
//     //SPLITTING CASE
//     else
//     {
//         BTLeafNode* sibling = new BTLeafNode();
//         int siblingKey = 0;
//         node->insertAndSplit(key, rid, sibling, siblingKey);
        
//         //NEED TO OBTAIN PARENT PID
//         PageId parentPid;
//         getParentPid(node->getNextNodePtr(), key, parentPid);
        
//         //WE INSERT OUR THE MIDDLE KEY INTO OUR NON LEAF NODE WITH THE PID OF THE CREATED SIBLING
//         insertIntoNonLeaf(siblingKey, parentPid, node->getNextNodePtr());
//     }
    
//     return 0;
// }

// RC BTreeIndex::getParentPid(PageId childPid, int key, PageId &parentPid)
// {
//     //START FROM ROOT
//     int level=0;
//     BTNonLeafNode* node=new BTNonLeafNode();
//     node->initializeRoot(0,-1,-1);
//     node->read(rootPid, pf);
    
//     PageId matchPid = rootPid;
//     while (matchPid != childPid || level>treeHeight)
//     {
//         parentPid = matchPid;
//         node->locateChildPtr(key, matchPid);
//         delete node;
//         node = new BTNonLeafNode();
//         node->read(matchPid, pf);
        
        
//         level++;
//     }
    
//     delete node;
    
//     if (matchPid != childPid)
//         matchPid = -1;
// }

// //RECURSIVE FUNCTION USED TO RECURSIVELY SPLIT UP NON LEAF NODES ON OVERFLOW
// //Key: Key we want to insert into non leaf node
// //pid: Pid of the current nonleaf node that needs to be loaded
// //siblingPid: The pid of the previously created right sibling that we want to add into our non leaf ode along with key
// RC BTreeIndex::insertIntoNonLeaf(int key, PageId pid, PageId siblingPid)
// {
//     BTNonLeafNode *curNode = new BTNonLeafNode();
//     curNode->read(pid, pf);
    
//     if (curNode->getKeyCount() < curNode->getMaxKeyCount())
//     {
//         curNode->insert(key, siblingPid);
//         return 0;
//     }
    
//     else if (curNode->getKeyCount() == curNode->getMaxKeyCount())
//     {
//         int midkey = 0;
//         BTNonLeafNode *sibling = new BTNonLeafNode();
//         curNode->insertAndSplit(key, siblingPid, sibling, midkey);
//         PageId parentPid;
//         getParentPid(pid, key, parentPid);
//         insertIntoNonLeaf(midkey, parentPid, curNode->getNextNodePtr());
        
//         return 0;
//     }
// }
/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	//if tree is empty
	if(treeHeight==0) {
		//make the root a leaf node
		BTLeafNode lnode;
		lnode.insert(key, rid);
		rootPid=1;//update rootpid to 1 as 0 holds tree info
		lnode.write(rootPid, pf);
		treeHeight=1;//set tree height to 1
		return 0;
	}
	//if tree is not empty
	else {
		int nkey;
		PageId npid;
		//if the root node needs to be split then create a new root non-leaf node
		if(insertRecursively(key, rid, 1, rootPid, nkey, npid)==RC_NODE_FULL) {
			BTNonLeafNode node;
			RC rval=node.initializeRoot(rootPid, nkey, npid);
			rootPid=pf.endPid();
			node.write(rootPid, pf);
			treeHeight++;
			return rval;
		}

	}
}
/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @param height[IN] used to store current height of tree
 * @param curNodePid[IN] used to store current pid of node
 * @param nkey[OUT] used to get key value that is pushed up
 * @param npid[OUT] used to get pid value that is pushed up 
 * @return error code. 0 if no error
 */
RC BTreeIndex::insertRecursively(int key, const RecordId& rid, int height, PageId curNodePid, int& nkey, PageId& npid) {
	//if we are at leaf level
	if(height==treeHeight) {
		BTLeafNode leaf;
		leaf.read(curNodePid, pf); //read current node's info
		//try inserting the key into leaf node
		if(leaf.insert(key, rid)==0) {
			//if insertion succeeds wrtie the node aka no need to split and recursively call
			//leaf.print();
			leaf.write(curNodePid, pf);
			return 0;
		}
		//if insertion fails aka splitting is required
		BTLeafNode sibling;
		int siblingKey;
		leaf.insertAndSplit(key, rid, sibling, siblingKey);
		//set sibling pid to be the end pid as it is being newly created
		PageId siblingPid=pf.endPid();
		//write the sibling node into memory
		sibling.write(siblingPid, pf);
		//set current node's next node pointer to be the sibling node
		leaf.setNextNodePtr(siblingPid);
		//overwrite the old node
		leaf.write(curNodePid, pf);
		//set pushup key and pid
		nkey=siblingKey;
		npid=siblingPid;
		//return RC_NODE_FULL to initiate upwards recursive splitting
		return RC_NODE_FULL;
	}
	//get info of current non-leaf node as we are not at leaf level
	BTNonLeafNode node;
	node.read(curNodePid, pf);
	PageId cpid=-1;
	//get child ptr to insert to
	node.locateChildPtr(key, cpid);
	//printf("Pid of child: %d\n", cpid);
	int gkey;
	PageId gpid;
	//if node needs to be split and added to
	if(insertRecursively(key, rid, height+1, cpid, gkey, gpid)==RC_NODE_FULL) {
		//try inserting
		if(node.insert(gkey, gpid)==0) {
			//insertion succeeds then write node to disk
			//node.print();
			node.write(curNodePid, pf);
			return 0;
		}
		//if insertion fails
		else {
			//create sibling node and middle key
			BTNonLeafNode sibling;
			int middleKey=0;
			//split the node
			node.insertAndSplit(gkey, gpid, sibling, middleKey);
			//write the original node to disk
			node.write(curNodePid, pf);
			//get new position for sibling node
			PageId siblingPid=pf.endPid();
			//write sibling node to disk
			sibling.write(siblingPid, pf);
			//set pushup key and pid
			nkey=middleKey;
			npid=siblingPid;
			return RC_NODE_FULL;
		}
	}
	return 0;
}
/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	//if no nodes in tree then return RC_NO_SUCH_RECORD
    if(treeHeight==0)
    	return RC_NO_SUCH_RECORD;
    //set pid to root intitially
    PageId pid=rootPid;
    //create new non-leaf node and intitalize it as the root
    BTNonLeafNode* node=new BTNonLeafNode();
    node->initializeRoot(0,-1,-1);
    //if tree has more than a root node
    if(treeHeight>1) {
    	//reach correct leafnode for searchkey
    	for(int currHeight=1; currHeight<treeHeight; currHeight++) {
    		//read contents of NonLeafNode
    		if(node->read(pid, pf)<0)
    			return DEFAULT_ERROR_CODE;
    		//find next node to lead up to correct leaf node, here pid will point us to the right node
    		if(node->locateChildPtr(searchKey, pid)<0)
    			return DEFAULT_ERROR_CODE;
    	}
    }
    //create a leafe node
    BTLeafNode* leaf=new BTLeafNode();
    //read contents of NonLeafNode
    if(leaf->read(pid, pf)<0)
    	return DEFAULT_ERROR_CODE;
    //find key in leaf node, if key is found set cursor's eid to eid of the found key
    if(leaf->locate(searchKey, cursor.eid)<0)
    	return RC_NO_SUCH_RECORD;
    //if key is found then set the cursor's pid to the pid of the leaf-node
    cursor.pid=pid;
    return 0;
}
/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    BTLeafNode* leaf=new BTLeafNode();
    //read contents of NonLeafNode
    if(leaf->read(cursor.pid, pf)<0)
    	return DEFAULT_ERROR_CODE;
    //get contents of cursor eid
    if(leaf->readEntry(cursor.eid, key, rid)<0)
    	return DEFAULT_ERROR_CODE;
    //move forward cursor by incrementing cursor.eid
    cursor.eid++;
    return 0;
}


void BTreeIndex::printTree()
{
	printRecurse(rootPid, 1);
}

void BTreeIndex::printRecurse(PageId pid, int level)
{
	cout << "\n========================================\n";
	cout << "Printing out level: " << level << endl;
	cout << "Printing out page pid: " << pid << endl;
	if (level > treeHeight)
		return;
	// Leaf node
	else if (level == treeHeight)
	{
		BTLeafNode node;
		node.read(pid, pf);
		node.print();
	}
	else
	{
		BTNonLeafNode node;
		
		node.read(pid, pf);
		node.print();
		PageId p;
		node.locateChildPtr(-1, p);
		printRecurse(p, level+1);
		for (int i = 0; i < node.getKeyCount(); i++)
		{
			p=node.getPID(i);
			printRecurse(p, level+1);
		}

	}
	cout << "\n========================================\n";
}