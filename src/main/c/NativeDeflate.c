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

    printf("strlen(istream): %lu\n", strlen(istream));

    z_stream defstream;

    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;

    deflateInit(&defstream, compressionLevel);

    unsigned long chunksRead = 0;
    unsigned long bytesRead = 0;
    unsigned char* in; 
    unsigned char* out;
    int flush = Z_NO_FLUSH;
    int have;

    do  
    {   
        int readAmount = CHUNK;

        if (bytesRead + CHUNK > length)
        {
            readAmount = length - bytesRead;
            flush = Z_FINISH;
        }
     
        in = &istream[CHUNK * chunksRead];
        defstream.next_in = (Bytef*) in; 
        defstream.avail_in = readAmount;
     
        do
        {
            out = &ostream[defstream.total_out];
            defstream.next_out = (Bytef*) out;
            defstream.avail_out = readAmount;
            deflate(&defstream, flush);
            have = CHUNK - defstream.avail_out;
            printf("have: %i\tavail_out: %i\n", have, defstream.avail_out);
        } while (defstream.avail_out == 0); 

        bytesRead+= readAmount;
        chunksRead++;
		  
        printf("chunk number: %lu\tread amount: %i\tbytes read: %lu\ttotal_in: %lu\t total_out: %lu\n", 
            chunksRead, 
            readAmount, 
            bytesRead, 
            defstream.total_in, 
            defstream.total_out);

    } while (bytesRead < length);

    (void)deflateEnd(&defstream);   

    printf("strlen(ostream): %lu\n", strlen(ostream));

    (*env)->ReleaseByteArrayElements(env, bytesIn, istream, JNI_ABORT);
    jbyteArray bytesOut = (*env)->NewByteArray(env, defstream.total_out);
    (*env)->SetByteArrayRegion(env, bytesOut, 0, defstream.total_out, (jbyte*)ostream);

    free(ostream);

    return bytesOut;}
