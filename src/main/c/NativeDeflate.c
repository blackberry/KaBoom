#include <stdbool.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "zlib.h"
#include "com_blackberry_bdp_kaboom_FastBoomWriter.h"

#define CHUNK 16384

JNIEXPORT jbyteArray JNICALL Java_com_blackberry_bdp_kaboom_FastBoomWriter_compress
    (JNIEnv *env, jobject thisObj, jbyteArray bytesIn, jint length, jint compressionLevel)
{
    jboolean isCopy;
    unsigned char* istream = (unsigned char*)(*env)->GetByteArrayElements(env, bytesIn, &isCopy);
    unsigned char* ostream = malloc(length);

    //printf("strlen(istream): %lu\n", strlen(istream));

    z_stream defstream;

    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;

    deflateInit(&defstream, compressionLevel);

    unsigned long chunksRead = 0;
    unsigned long bytesRead = 0;
    unsigned char* in; 
    unsigned char* out;

    do  
    {   
        int readAmount = CHUNK;

        if (chunksRead * CHUNK + CHUNK > length)
        {   
            readAmount = length - chunksRead * CHUNK;
        }   
            
        //printf("reading chunk number: %lu, read amount: %i, total_out: %lu\n", chunksRead + 1, readAmount, defstream.total_out);

        in = &istream[CHUNK * chunksRead];
        out = &ostream[defstream.total_out];

        defstream.next_in = (Bytef*)in;
        defstream.avail_in = readAmount;
     
        defstream.avail_out = CHUNK;
        defstream.next_out = out;
                
        deflate(&defstream, Z_NO_FLUSH);

        bytesRead+= readAmount;
        chunksRead++;

    } while (bytesRead < length);

    (void)deflateEnd(&defstream);   

    //printf("strlen(ostream): %lu\n", strlen(ostream));

    (*env)->ReleaseByteArrayElements(env, bytesIn, istream, JNI_ABORT);
    jbyteArray bytesOut = (*env)->NewByteArray(env, defstream.total_out);
    (*env)->SetByteArrayRegion(env, bytesOut, 0, defstream.total_out, (jbyte*)ostream);

    free(ostream);

    return bytesOut;
}
