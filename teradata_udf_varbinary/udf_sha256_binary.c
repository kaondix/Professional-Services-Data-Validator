/* Copyright 2024 Google LLC
  
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

#define SQL_TEXT Latin_Text
#include "sqltypes_td.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define DIGEST_LEN_SHA256 32
#define UDF_OK "00000"

void sha256(const unsigned char message[], int len, unsigned char result[]);

void sha256_binary(VARBYTE *arg, CHARACTER_LATIN *result,
 char sqlstate[])
{
    int i;
    unsigned char outbuf[DIGEST_LEN_SHA256];

    sha256((unsigned char *)arg->bytes, arg->length, outbuf);

    for (i = 0; i < DIGEST_LEN_SHA256; i++) {
        sprintf(result + i*2, "%02x", outbuf[i]);
    }

    sprintf(sqlstate, UDF_OK);
}
