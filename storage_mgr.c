#include "storage_mgr.h"
#include "stdio.h"
#include "sys/stat.h"
#include "stdlib.h"

/* manipulating page files */
extern void initStorageManager (void)
{

}

extern RC createPageFile (char *fileName)
{
	FILE* file = fopen(fileName, "w");
	SM_PageHandle newPage = (SM_PageHandle)malloc(PAGE_SIZE*sizeof(char));
	fwrite(newPage, sizeof(char), PAGE_SIZE, file);
	fclose(file);
	free(newPage);
	return RC_OK;
}

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	FILE* file = fopen(fileName, "r+");
	if(file == NULL){
		return RC_FILE_NOT_FOUND;
	}

	struct stat statbuf;
	stat(fileName,&statbuf);
	fHandle->totalNumPages = statbuf.st_size / PAGE_SIZE;
	if(statbuf.st_size % PAGE_SIZE != 0){
		fHandle->totalNumPages++;
	}
	fHandle->fileName = fileName;
	fHandle->curPagePos = 0;
	fHandle->mgmtInfo = file;

	return RC_OK;
}

extern RC closePageFile (SM_FileHandle *fHandle)
{
	fclose(fHandle->mgmtInfo);
	fHandle->mgmtInfo = NULL;
	return RC_OK;
}

extern RC destroyPageFile (char *fileName)
{
	FILE* file = fopen(fileName, "r");
	if(file == NULL){
		return RC_FILE_NOT_FOUND;
	}
	fclose(file);
	remove(fileName);
	return RC_OK;
}

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(pageNum > fHandle->totalNumPages-1 || pageNum < 0)  // when the pageNum exceeds the total amount of pages
		return RC_READ_NON_EXISTING_PAGE;
	if(!memPage)							// if pageHandler is not assigned memory before
		memPage = (SM_PageHandle)malloc(sizeof(char)*PAGE_SIZE);

	fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET); // put file pointer to the pagenumTh position
	fread(memPage,sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);	// read blocks into buffer(pageHandler)
	fHandle->curPagePos = pageNum;//set current page position to pagenumTh
	return RC_OK;
}

extern int getBlockPos (SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(0, fHandle, memPage);
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int prePos = fHandle->curPagePos - 1;      // get previous position
	return readBlock(prePos, fHandle, memPage);

}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int pos = fHandle->curPagePos;
	return readBlock(pos, fHandle, memPage);
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int nexPagePos = fHandle->curPagePos + 1;
	return readBlock(nexPagePos, fHandle, memPage);
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int pos = fHandle->totalNumPages - 1;
	return readBlock(pos, fHandle, memPage);
}

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;
	}
	long startPosition = pageNum * PAGE_SIZE;
	if(fseek(fHandle->mgmtInfo, PAGE_SIZE*pageNum, SEEK_SET) != 0){
		return RC_WRITE_FAILED;
	}

	fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int pos = fHandle->curPagePos;
	return writeBlock(pos, fHandle, memPage);
}

extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	if(fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;
	}
	if(fseek(fHandle->mgmtInfo, 0, SEEK_END) != 0){
		return RC_WRITE_FAILED;
	}

	SM_PageHandle newBlock = (SM_PageHandle)malloc(PAGE_SIZE*sizeof(char));
	fwrite(newBlock, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
	fHandle->totalNumPages++;
	fHandle->curPagePos = fHandle->totalNumPages-1;
	free(newBlock);
	return RC_OK;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	if(fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;
	}

	if(fHandle->totalNumPages < numberOfPages){
		int addNum = numberOfPages - fHandle->totalNumPages;
		for(int i=0;i<addNum;i++){
			appendEmptyBlock (fHandle);
		}
	}
	return RC_OK;
}
