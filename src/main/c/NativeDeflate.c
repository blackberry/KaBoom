#include <stdbool.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "zlib.h"
#include "com_blackberry_bdp_kaboom_KaBoom.h"

JNIEXPORT jbyteArray JNICALL Java_ca_ariens_zlib_testing_Main_compress
    (JNIEnv *env, jobject thisObj, jbyteArray bytesIn, jint length, jint compressionLevel)
{
    //length = length + 1;

    //int len = (*env)->GetArrayLength(env, bytesIn);

    jbyte* istream;

    istream = (*env)->GetByteArrayElements(env, bytesIn, JNI_FALSE);


    //(*env)->GetByteArrayRegion(env, bytesIn, 0, length, istream);
    //return bytesIn;


    //ulong srcLen = strlen(istream) + 1;
    ulong destLen = compressBound(length); //used to be srcLen
    char* ostream = malloc(destLen);

    int res = compress2(ostream, &destLen, istream, length, compressionLevel); //position used to be srcLen

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

    (*env)->ReleaseByteArrayElements(env, bytesIn, istream, JNI_ABORT);

    return bytesOut;
}
