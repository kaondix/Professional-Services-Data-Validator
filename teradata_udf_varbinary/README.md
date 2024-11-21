# Update to Teradata UDF to support Accented Characters encoded in Latin1his

The Data Validation Tool uses a [teradata udf](https://github.com/akuroda/teradata-udf-sha2)
written by Akira Kuroda that calculates the SHA-2 checksum that is offered as a builtin in other databases.

For character string comparisons, DVT computes a checksum on the UTF-8 representation of the string across
both databases and compares the checksums. Earlier versions of DVT assumed that the character string in Teradata
was either UTF-8 encoded or ASCII and that worked well. If the character string had Accented characters (e.g. É) encoded
in Latin1, these comparisons failed because the character string was not [converted to UTF-8 encoding prior to computing the checksum](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1170). In Aug 2024, [DVT used the function TransUnicodeToUTF8](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/1226) to convert all character strings to UTF-8 representation. The TransUnicodeToUTF8 function produces a VARBINARY type for which the sha256 checksum needs to be calculated. This directory contains files to modify the udf to support VARBINARY.

The Teradata udf referenced above has most everything needed to calculate a sha256 checksum from
a VARBINARY type. A simple wrapper is needed to support VARBINARY. This directory provides two
files that create the wrapper:
* hash_sha256.sql - this replaces teradata_udf/src/hash_sha256.sql
* udf_sha256_binary.c - this file has the wrapper that defines the function to compute sha256 checksum from VARBINARY

## Installation Instructions

* Download the [teradata udf](https://github.com/akuroda/teradata-udf-sha2)
* Replace the hash_sha256.sql with the file in this directory
* Add the file udf_sha256_binary.c from this directory into the directory that contains hash_sha256.sql
* Install the teradata udf as per instructions provided by Kuroda-san

## Contributions

We are happy to contribute these changes to the original repository. We are contacting Kuroda-san to see if he will accept our contribution
