#include <stdbool.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "zlib.h"
#include "include/com_blackberry_bdp_kaboom_KaBoom.h"
 
JNIEXPORT jbyteArray JNICALL Java_com_blackberry_bdp_kaboom_KaBoom_compress
	(JNIEnv *env, jobject thisObj, jbyteArray bytesIn, jint compressionLevel) 
{
	int len = (*env)->GetArrayLength(env, bytesIn);
	char istream[len];
	(*env)->GetByteArrayRegion(env, bytesIn, 0, len, istream);

	// +1 below is for trailing \0

	ulong srcLen = strlen(istream) + 1;
	ulong destLen = compressBound(srcLen); 
	char* ostream = malloc(destLen);
	int res = compress2(ostream, &destLen, istream, srcLen, compressionLevel);

	// destLen is now the size of actuall buffer needed for compression
	// you don't want to uncompress the whole buffer later, only this

	if (res == Z_BUF_ERROR)
	{
		printf("ERROR: Buffer was too small!\n");
		return;
	}

	if (res ==  Z_MEM_ERROR)
	{
		printf("ERROR: Insufficient memory for compression!\n");
		return;
	}

	/* there's no need to decompress this now, useful for testing....
	
	const char *i2stream = ostream;
	char* o2stream = malloc(srcLen);
	ulong destLen2 = destLen; 
	int des = uncompress(o2stream, &srcLen, i2stream, destLen2);
	printf("%s\n", o2stream);
	*/
	
	// Convert the compressed char array to a jbyteArray and return

	jbyteArray bytesOut = (*env)->NewByteArray(env, destLen);
	(*env)->SetByteArrayRegion(env, bytesOut, 0, destLen, (jbyte*)ostream);
	
	return bytesOut;
} 
