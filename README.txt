Project Description
Developed and designed a buffer-pool-management system using C, implemented FIFO,LFU, LRU, CLOCK page replacement strategies.

There are 3 new data structures defined in buffer_mgm.c. One is BM_PageFrame, which stores infomation about a pageframe in buffer: a page handler, the number of the page, a dirty flag to mark a dirty page, a fix-count number for FIFO, a timestamp for LRU, a reference bit for CLOCK, used time for LFU.

The second one is MgmtData, which stores the bookkeeping infomation for buffer pool. The last one is Mapping, standing for a hashtable which maps the pagenum tothe buffer frame of the in-buffer page.

Method:

initBufferPool() initiates an empty buffer, set all the frames to unused state.

shutdownBufferPool() first forces buffer pool flushing then check if there is still in-use frame then free the mgmtData.

forceFlushPool() writes the dirty and not-in-use page back to disk.

FIFO() replaces a page using FIFO. We need to find the first not-in-use page when the buffer is full andreplaces it if the replaced page is dirty, write it back first. 

LRU() replaces a page using LRU. We find the most rent and not-in-used frame andreplace it and assign a new timestamp to the newly inserted frame. 

CLOCk() replaces a page using CLOCK. We first check refer bit of each frame starting from the clock pointer, if the referbit is 1, set it to 0, if the referbit is 0 and as well as the fixcount, means this frame is not-in-use, then replaces it with new page.

LFU() replaces a page using LFU. we need to find the least freaquently used frame and then replaces it.

markDirty() marks the specific page into dirty page.

unpinPage() unpins the specific page one time if it is in-use.

forcePage() forces the given page, writing it into disk.

pinPage() pins the given pageNum page. We first find it in the hashtable and getthe corresponding frame position. if the page is not found, then we read it fromdisk and set it pinned, else if the buffer is full, then take a replacement plicy.

getFrameContents() gets all the pageNum of the frames in buffer pool.

getDirtyFlags() gets all the dirty page in the buffer pool.

getFixCounts() gets all the fixcount of every frame.

getNumReadIO() gets the number pages that have been read from disk. This attribute is stored in MgmData.

 
getNumWriteIO() gets the number pages that have been written to disk. This attribute is stored in MgmData.

