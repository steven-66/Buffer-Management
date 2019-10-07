#include "buffer_mgr.h"
#include "stdio.h"
#include "stdlib.h"
#include "storage_mgr.h"
#include "uthash.h"

typedef struct BM_PageFrame {
	SM_PageHandle content;
	PageNumber pageNum;
	int dirtyFlag;
	int fixCount;
	int timeStamp;
	int referBit;
	int usedTime;
} BM_PageFrame;

typedef struct Mapping {
	int key;                    /* key */
	int value;
	UT_hash_handle hh;         /* makes this structure hashable */
} Mapping;

typedef struct MgmtData
{
	int curIdx;
	int bufferSize;
	int timeNo;
	int readNum;
	int writeNum;
	int clockPointer;
	BM_PageFrame* pageFrames;
	Mapping* pageNum2Frame;
}MgmtData;


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		const int numPages, ReplacementStrategy strategy,
		void *stratData){
	BM_PageFrame* pageFrames = (BM_PageFrame*)malloc(sizeof(BM_PageFrame) * numPages);
	MgmtData* mgmtData = (MgmtData*)malloc(sizeof(MgmtData));

	for(int i=0;i<numPages;i++){
		pageFrames[i].pageNum = NO_PAGE;
		pageFrames[i].content = (SM_PageHandle)malloc(PAGE_SIZE * sizeof(char));
		pageFrames[i].dirtyFlag = 0;
		pageFrames[i].fixCount = 0;
		pageFrames[i].timeStamp = 0;
		pageFrames[i].referBit = 0;
		pageFrames[i].usedTime = 0;
	}
	
	mgmtData->curIdx = -1;
	mgmtData->bufferSize = 0;
	mgmtData->timeNo = 0;
	mgmtData->readNum = 0;
	mgmtData->writeNum = 0;
	mgmtData->clockPointer = 0;
	mgmtData->pageFrames = pageFrames;
	mgmtData->pageNum2Frame = NULL;

	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->mgmtData = mgmtData;

	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
	forceFlushPool(bm);

	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame *pageFrames = (BM_PageFrame *)mgmtData->pageFrames;

	for(int i=0;i<bm->numPages;i++){
		if(pageFrames[i].fixCount > 0){
			return RC_PINNED_PAGE;
		}
	}
	free(mgmtData);
	bm->mgmtData = NULL;
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	for(int i=0;i<bm->numPages;i++){
		if(pageFrames[i].dirtyFlag == 1 && pageFrames[i].fixCount == 0){
			SM_FileHandle fHandle;
			openPageFile(bm->pageFile, &fHandle);
			writeBlock(pageFrames[i].pageNum, &fHandle, pageFrames[i].content);
			pageFrames[i].dirtyFlag = 0;
			closePageFile(&fHandle);

			mgmtData->writeNum++;
		}
	}
	return RC_OK;
}

// Buffer Manager Interface Access Pages
void FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,
		const PageNumber pageNum){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	int replaceIdx = ++(mgmtData->curIdx) % bm->numPages;
	for (int i = 0; i < bm->numPages; i++) {
		if (pageFrames[replaceIdx].fixCount == 0) {
			if (pageFrames[replaceIdx].dirtyFlag == 1) {
				SM_FileHandle fHandle;
				openPageFile(bm->pageFile, &fHandle);
				writeBlock(pageFrames[replaceIdx].pageNum, &fHandle, pageFrames[replaceIdx].content);
				closePageFile(&fHandle);

				mgmtData->writeNum++;
			}

			/* Delete item in Mapping*/
			Mapping* deleteMap;
			HASH_FIND_INT(mgmtData->pageNum2Frame, &pageFrames[replaceIdx].pageNum, deleteMap);
			HASH_DEL(mgmtData->pageNum2Frame, deleteMap);
			free(deleteMap);

			SM_FileHandle fHandle;
			openPageFile(bm->pageFile, &fHandle);
			readBlock(pageNum, &fHandle, pageFrames[replaceIdx].content);
			pageFrames[replaceIdx].dirtyFlag = 0;
			pageFrames[replaceIdx].fixCount = 1;
			pageFrames[replaceIdx].pageNum = pageNum;

			page->data = pageFrames[replaceIdx].content;
			page->pageNum = pageNum;

			closePageFile(&fHandle);
			mgmtData->curIdx = replaceIdx;
			mgmtData->readNum++;

			/* Add new item in Mapping*/
			Mapping* newMap = malloc(sizeof(Mapping));
			newMap->key = pageNum;
			newMap->value = replaceIdx;
			HASH_ADD_INT(mgmtData->pageNum2Frame, key, newMap);

			break;
		}
		else {
			replaceIdx++;
			replaceIdx = replaceIdx % bm->numPages;
		}
	}
	return;
}

void LRU(BM_BufferPool *const bm, BM_PageHandle *const page,
		const PageNumber pageNum){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	int replaceIdx = 0;
	for (int i = 0; i < bm->numPages; i++) {
		if (pageFrames[i].timeStamp <= pageFrames[replaceIdx].timeStamp && pageFrames[replaceIdx].fixCount == 0) {
			replaceIdx = i;
		}
	}

	if (pageFrames[replaceIdx].dirtyFlag == 1) {
		SM_FileHandle fHandle;
		openPageFile(bm->pageFile, &fHandle);
		writeBlock(pageFrames[replaceIdx].pageNum, &fHandle, pageFrames[replaceIdx].content);
		closePageFile(&fHandle);

		mgmtData->writeNum++;
	}

	/* Delete item in Mapping*/
	Mapping* deleteMap;
	HASH_FIND_INT(mgmtData->pageNum2Frame, &pageFrames[replaceIdx].pageNum, deleteMap);
	HASH_DEL(mgmtData->pageNum2Frame, deleteMap);
	free(deleteMap);

	SM_FileHandle fHandle;
	openPageFile(bm->pageFile, &fHandle);
	readBlock(pageNum, &fHandle, pageFrames[replaceIdx].content);
	pageFrames[replaceIdx].dirtyFlag = 0;
	pageFrames[replaceIdx].fixCount = 1;
	pageFrames[replaceIdx].pageNum = pageNum;
	pageFrames[replaceIdx].timeStamp = ++(mgmtData->timeNo);

	page->data = pageFrames[replaceIdx].content;
	page->pageNum = pageNum;

	closePageFile(&fHandle);
	mgmtData->curIdx = replaceIdx;
	mgmtData->readNum++;

	/* Add new item in Mapping*/
	Mapping* newMap;
	newMap = malloc(sizeof(Mapping));
	newMap->key = pageNum;
	newMap->value = replaceIdx;
	HASH_ADD_INT(mgmtData->pageNum2Frame, key, newMap);
	return;
}

void CLOCK(BM_BufferPool *const bm, BM_PageHandle *const page,
		const PageNumber pageNum){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	for (int i = mgmtData->clockPointer; i <= bm->numPages; i++) {
		i = i % bm->numPages;
		if (pageFrames[i].referBit == 1) {
			pageFrames[i].referBit = 0;
		}
		else if(pageFrames[i].referBit == 0 && pageFrames[i].fixCount == 0){
			if (pageFrames[i].dirtyFlag == 1) {
				SM_FileHandle fHandle;
				openPageFile(bm->pageFile, &fHandle);
				writeBlock(pageFrames[i].pageNum, &fHandle, pageFrames[i].content);
				closePageFile(&fHandle);

				mgmtData->writeNum++;
			}

			/* Delete item in Mapping*/
			Mapping* deleteMap;
			HASH_FIND_INT(mgmtData->pageNum2Frame, &pageFrames[i].pageNum, deleteMap);
			HASH_DEL(mgmtData->pageNum2Frame, deleteMap);
			free(deleteMap);

			SM_FileHandle fHandle;
			openPageFile(bm->pageFile, &fHandle);
			readBlock(pageNum, &fHandle, pageFrames[i].content);
			pageFrames[i].dirtyFlag = 0;
			pageFrames[i].fixCount = 1;
			pageFrames[i].pageNum = pageNum;
			pageFrames[i].timeStamp = ++(mgmtData->timeNo);
			pageFrames[i].referBit = 1;

			page->data = pageFrames[i].content;
			page->pageNum = pageNum;
			mgmtData->readNum++;
			mgmtData->clockPointer = (i + 1) % bm->numPages;

			closePageFile(&fHandle);

			/* Add new item in Mapping*/
			Mapping* newMap;
			newMap = malloc(sizeof(Mapping));
			newMap->key = pageNum;
			newMap->value = i;
			HASH_ADD_INT(mgmtData->pageNum2Frame, key, newMap);
			break;
		}
	}
	return;
}

void LFU(BM_BufferPool *const bm, BM_PageHandle *const page,
		const PageNumber pageNum){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	int replaceIdx = 0;
	for (int i = 0; i < bm->numPages; i++) {
		if (pageFrames[i].usedTime < pageFrames[replaceIdx].usedTime && pageFrames[i].fixCount == 0) {
			replaceIdx = i;
		}
	}

	if (pageFrames[replaceIdx].dirtyFlag == 1) {
		SM_FileHandle fHandle;
		openPageFile(bm->pageFile, &fHandle);
		writeBlock(pageFrames[replaceIdx].pageNum, &fHandle, pageFrames[replaceIdx].content);
		closePageFile(&fHandle);

		mgmtData->writeNum++;
	}

	/* Delete item in Mapping*/
	Mapping* deleteMap;
	HASH_FIND_INT(mgmtData->pageNum2Frame, &pageFrames[replaceIdx].pageNum, deleteMap);
	HASH_DEL(mgmtData->pageNum2Frame, deleteMap);
	free(deleteMap);

	SM_FileHandle fHandle;
	openPageFile(bm->pageFile, &fHandle);
	readBlock(pageNum, &fHandle, pageFrames[replaceIdx].content);
	pageFrames[replaceIdx].dirtyFlag = 0;
	pageFrames[replaceIdx].fixCount = 1;
	pageFrames[replaceIdx].pageNum = pageNum;
	pageFrames[replaceIdx].timeStamp = ++(mgmtData->timeNo);
	pageFrames[replaceIdx].usedTime = 0;

	page->data = pageFrames[replaceIdx].content;
	page->pageNum = pageNum;
	mgmtData->readNum++;

	closePageFile(&fHandle);

	/* Add new item in Mapping*/
	Mapping* newMap;
	newMap = malloc(sizeof(Mapping));
	newMap->key = pageNum;
	newMap->value = replaceIdx;
	HASH_ADD_INT(mgmtData->pageNum2Frame, key, newMap);
	return;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	Mapping* res;
	HASH_FIND_INT(mgmtData->pageNum2Frame, &(page->pageNum), res);
	if (res != NULL) {
		int pos = res->value;
		pageFrames[pos].dirtyFlag = 1;
	}
	return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	Mapping* res;
	HASH_FIND_INT(mgmtData->pageNum2Frame, &(page->pageNum), res);
	if (res != NULL) {
		int pos = res->value;
		if (pageFrames[pos].fixCount > 0) {
			pageFrames[pos].fixCount--;
		}
	}
	return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	SM_FileHandle fHandle;
	openPageFile(bm->pageFile, &fHandle);
	writeBlock(page->pageNum, &fHandle, page->data);
	closePageFile(&fHandle);
	mgmtData->writeNum++;

	Mapping* res;
	HASH_FIND_INT(mgmtData->pageNum2Frame, &(page->pageNum), res);
	if (res != NULL) {
		int pos = res->value;
		pageFrames[pos].dirtyFlag = 0;
	}
	return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
		const PageNumber pageNum){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;

	Mapping* res;
	HASH_FIND_INT(mgmtData->pageNum2Frame, &pageNum, res);  /* res: output pointer */
	if (res != NULL) {
		int existIdx = res->value;
		pageFrames[existIdx].fixCount++;
		pageFrames[existIdx].timeStamp = ++(mgmtData->timeNo);
		pageFrames[existIdx].referBit = 1;
		pageFrames[existIdx].usedTime++;

		page->data = pageFrames[existIdx].content;
		page->pageNum = pageNum;
		return RC_OK;
	}

	if(mgmtData->bufferSize < bm->numPages){
		int idx = ++(mgmtData->curIdx);

		SM_FileHandle fHandle;
		openPageFile(bm->pageFile, &fHandle);
		ensureCapacity(pageNum+1, &fHandle);
		readBlock(pageNum, &fHandle, pageFrames[idx].content);
		pageFrames[idx].dirtyFlag = 0;
		pageFrames[idx].fixCount = 1;
		pageFrames[idx].pageNum = pageNum;
		pageFrames[idx].timeStamp = ++(mgmtData->timeNo);
		pageFrames[idx].referBit = 1;
		pageFrames[idx].usedTime++;

		page->data = pageFrames[idx].content;
		page->pageNum = pageNum;
		closePageFile(&fHandle);
		mgmtData->bufferSize++;
		mgmtData->readNum++;

		Mapping *newMap;
		newMap = malloc(sizeof(Mapping));
		newMap->key = pageNum;
		newMap->value = idx;
		HASH_ADD_INT(mgmtData->pageNum2Frame, key, newMap);
	}else{
		switch(bm->strategy){
		case RS_FIFO:
			FIFO(bm, page, pageNum);
			break;
		case RS_LRU:
			LRU(bm, page, pageNum);
			break;
		case RS_CLOCK:
			CLOCK(bm, page, pageNum);
			break;
		case RS_LFU:
			LFU(bm, page, pageNum);
			break;
		}
	}

	return RC_OK;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;
	PageNumber* frameCont = (PageNumber*)malloc(bm->numPages * sizeof(PageNumber));

	for(int i = 0; i < bm->numPages; i++) {
		frameCont[i] = pageFrames[i].pageNum;
	}
	return frameCont;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;
	bool* dirtyFlags = (bool*)malloc(bm->numPages * sizeof(bool));

	for (int i = 0; i < bm->numPages; i++) {
		dirtyFlags[i] = (pageFrames[i].dirtyFlag == 1) ? true : false;
	}
	return dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm){
	MgmtData* mgmtData = bm->mgmtData;
	BM_PageFrame* pageFrames = (BM_PageFrame*)mgmtData->pageFrames;
	int* fixCounts = (int*)malloc(bm->numPages * sizeof(int));

	for (int i = 0; i < bm->numPages; i++) {
		fixCounts[i] = pageFrames[i].fixCount;
	}
	return fixCounts;
}

int getNumReadIO (BM_BufferPool *const bm){
	MgmtData* mgmtData = bm->mgmtData;
	return mgmtData->readNum;
}

int getNumWriteIO (BM_BufferPool *const bm){
	MgmtData* mgmtData = bm->mgmtData;
	return mgmtData->writeNum;
}
