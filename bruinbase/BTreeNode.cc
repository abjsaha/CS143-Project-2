#include "BTreeNode.h"

using namespace std;
//leaf node constructor
BTLeafNode::BTLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE);
	maxKeyCount=(PageFile::PAGE_SIZE-sizeof(PageId))/sizeof(entry);
	currKeyCount=0;
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
	int n=0;
	for(entry* curr=(entry*)(buffer);(curr->key)!=0&&n<getMaxKeyCount();curr++, n++);
	return n;
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
	if(getKeyCount()>=getMaxKeyCount())
		return RC_NODE_FULL;
	int eid=-1;
	//find the entry id
	locate(key, eid);
	//point to last entry
	entry* tmp=(entry*) buffer+eid;
	if(getKeyCount()>0&&eid!=getKeyCount())
		memmove(tmp+1, tmp, (getKeyCount()-eid)*sizeof(entry));//copy entry from tmp to tmp+1
	//assign key and rid
	tmp->key=key;
	tmp->rid=rid;
	setCurrKeyCount(getKeyCount()+1);//increment currKeyCount
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
	if(sibling.getKeyCount()>0)
		return -1;
	int split=(getKeyCount()+1)/2;
	int eid=0;
	locate(key,eid);
	siblingKey=(eid==split)?key:((entry*)buffer+split)->key;
	for(int i=split;i<getKeyCount();i++) {
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
	for(int i=0;i<getKeyCount();i++) {
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
	eid=getKeyCount();//reached end of node without reaching searchKey smaller than existing key or the key itself
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
		return -1;
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
	PageId* next=(PageId*)(buffer+getMaxKeyCount()*sizeof(entry));
	return *next; //increase the pointer by the size of the maximum number of nodes
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
	PageId* next=(PageId*)(buffer+getMaxKeyCount()*sizeof(entry)); //find next node similarly to get next node
	*next=pid;//set it
	return 0;
}








////////////////////////////////////////////////////////////////////////////////
//NON-leaf node stuff
////////////////////////////////////////////////////////////////////////////////
//non-leaf node constructor
BTNonLeafNode::BTNonLeafNode() {
	memset(buffer, 0, PageFile::PAGE_SIZE);
	maxKeyCount=(PageFile::PAGE_SIZE-sizeof(PageId))/sizeof(entry);
	currKeyCount=0;
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
{
	return pf.read(pid, buffer);
}
    
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
	int n=0;
	for(entry* curr=(entry*)(buffer+sizeof(PageId));(curr->key)!=0&&n<getMaxKeyCount();curr++, n++);
	return n;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	//check for node full error
	if(getKeyCount()>=getMaxKeyCount())
		return RC_NODE_FULL;
	entry* tmp=(entry*)(buffer+sizeof(PageId));//find first entry
	int prev_key=tmp->key;//set first entry key
	tmp++;//go to second entry
	for(int i=1;i<getKeyCount();i++) {
		if(prev_key<key&&tmp->key>key) { //if key is between two entrys then move over all the entries on its right and insert in the middle
			memmove(tmp, tmp-1, (getKeyCount()-i)*sizeof(entry));
			tmp--;
			tmp->key=key;
			tmp->pid=pid;
			setCurrKeyCount(getKeyCount()+1);
			return 0;
		}
		else {
			prev_key=tmp->key;
			tmp++;
		}
	}
	//if entry is inserted at the end
	tmp=(entry*)(buffer+getKeyCount()+sizeof(PageId));
	tmp->key=key;
	tmp->pid=pid;
	setCurrKeyCount(getKeyCount()+1);
	return 0;
}

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
{
	int split=(getKeyCount()-1)/2;
	entry* first=(entry*)(buffer+sizeof(PageId));
	entry* mid=first+split;
	entry* last=first+getKeyCount();
	bool flg=false;
	if(key>(mid+1)->key) {
		split++;
		mid++;
		flg=true;
	}
	midKey=mid->key;
	sibling.initializeRoot(mid->pid, (mid+1)->key, (mid+1)->pid);
	for(int i=split+2;i<getKeyCount();i++) {
		sibling.insert((first+i)->key,(first+i)->pid);
	}
	memset(mid, 0, (getKeyCount()-split-1)*sizeof(entry));
	setCurrKeyCount(split);
	if(flg) {
		sibling.insert(key, pid);
	}
	else {
		insert(key,pid);
	}
	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	int offset=sizeof(PageId); //to offset initial pageId
	entry* tmp=(entry*)(buffer+offset);
	if(searchKey<tmp->key) { //if pid to be returned is the initial pid
		PageId* p=(PageId*)buffer;
		pid=*p;
		return 0;
	}
	for(int i=1;i<getKeyCount();i++) {
		if((tmp+i)->key>searchKey) { //if searchkey is less than a key then return the pid before that entry
			pid=(tmp+i-1)->pid;
			return 0;
		}
	}
	pid=(tmp+getKeyCount())->pid;//if searchkey is greater than all keys then return last pid
	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	int offset=sizeof(PageId);//get space for first pid
	memset(buffer, 0, PageFile::PAGE_SIZE);//reset buffer
	PageId* firstPid=(PageId*) buffer;//the first pid will be at the start of the buffer
	*firstPid=pid1;//set the first pid 
	entry* tmp=(entry*)(buffer+offset); //create an entry after the first pid
	tmp->pid=pid2;
	tmp->key=key;
	return 0;
}
