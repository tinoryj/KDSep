#include <jni.h>
#include <stdio.h>

JNIEXPORT void JNICALL Java_RocksDBInterface_show(JNIEnv * jenv, jclass jcls) {
    printf("Goodbye World!\n");
}
