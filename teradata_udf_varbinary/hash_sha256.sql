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

/* script to install the UDF */

replace function hash_sha256
  (arg varchar(32000) character set latin)
  returns char(64) character set latin
  SPECIFIC sha256_latin
  language c
  no sql
  external name 'cs:sha256:sha256.c:cs:sha256_latin:udf_sha256_latin.c:F:sha256_latin'
  parameter style td_general;

replace function hash_sha256
  (arg varbyte(64000) )
  returns char(64) character set latin
  SPECIFIC sha256_latin
  language c
  no sql
  external name 'cs:sha256_binary:udf_sha256_binary.c:F:sha256_binary'
