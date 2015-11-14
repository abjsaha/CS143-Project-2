#include "BTreeNode.h"

using namespace std;
//leaf node constructor
BTLeafNode::BTLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE);
	maxKeyCount=(PageFile::PAGE_SIZE-sizeof(PageId)/sizeof(entry);
}
//leaf node destructor
BTLeafNode::~BTLeafNode() {
	delete[] buffer;
}
//get method for maxKeyCount
int BTLeafNode::getMaxKeyCount() {
	return maxKeyCount;
}
//set method for currKeyCount
void BTLeafNode::setCurrKeyCount(int x) {
	currKeyCount=x;
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	return currKeyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	//check if key count is greater than equal to max
	if(getCurrKeyCount()>=getMaxKeyCount())
		return RC_NODE_FULL;
	int eid=-1;
	//find the entry id
	locate(key, eid);
	//point to last entry
	entry* tmp=(entry*) buffer+eid;
	if(getCurrKeyCount()>0&&eid!=getCurrKeyCount())
		memmove(tmp+1, tmp, (getCurrKeyCount()-eid)*sizeof(entry));//copy entry from tmp to tmp+1
	//assign key and rid
	tmp->key=key;
	tmp->rid=rid;
	setCurrKeyCount(getCurrKeyCount()+1);//increment currKeyCount
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{
	if(sibling.getCurrKeyCount()>0)
		return -1;
	int split=(getCurrKeyCount()+1)/2;
	int eid=0;
	locate(key,eid);
	siblingKey=(eid==split)?key:((entry*)buffer+split)->key;
	for(int i=split;i<getCurrKeyCount();i++) {
		entry* cur=(entry*)buffer+i;
		sibling.insert(cur->key, cur->rid);
		cur->key=0;
		cur->rid.sid=0;
		cur->rid.pid=0;
	}
	setCurrKeyCount(split);
	if(eid<split) {
		insert(key, rid);
	}
	else
		sibling.insert(key, rid);
	return 0;

}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	entry* tmp=(entry*)buffer; //set pointer to starting entry
	for(int i=0;i<getCurrKeyCount();i++) {
		if(tmp->key==searchKey) { //if searchkey is the key of an entry
			eid=i;
			return 0;
		}
		else if(tmp->key>searchKey) { //immediately behind the largest key smaller than searchKey
			eid=i;
			return RC_NO_SUCH_RECORD;
		}
		tmp++;
	}
	eid=getCurrKeyCount();//reached end of node without reaching searchKey smaller than existing key or the key itself
	return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	//checking if buffer is null
	if(!buffer) {
		return -1;
	}
	//if invalid eid
	if(eid>-getKeyCount()) {
		return RC_INVALID_EID;
	}
	//go to specific entry
	entry* tmp=(entry*)buffer+eid;
	//read entry
	key=tmp->key;
	rid=tmp->rid;
	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	return (PageId*)(buffer+getMaxKeyCount*sizeof(entry)); //increase the pointer by the size of the maximum number of nodes
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	if(pid<0) //check invalid pid
		return RC_INVALID_PID;
	PageId* next=(PageId*)(buffer+getMaxKeyCount*sizeof(entry)); //find next node similarly to get next node
	*next=pid;//set it
	return 0;
}








////////////////////////////////////////////////////////////////////////////////
//NON-leaf node stuff
////////////////////////////////////////////////////////////////////////////////
//non-leaf node constructor
BTNonLeafNode::BTNonLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE);
	maxKeyCount=(PageFile::PAGE_SIZE-sizeof(PageId)/sizeof(entry);
}
//leaf node destructor
BTNonLeafNode::~BTNonLeafNode() {
	delete[] buffer;
}
//get method for maxKeyCount
int BTNonLeafNode::getMaxKeyCount() {
	return maxKeyCount;
}
//set method for currKeyCount
void BTNonLeafNode::setCurrKeyCount(int x) {
	currKeyCount=x;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	return currKeyCount;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
