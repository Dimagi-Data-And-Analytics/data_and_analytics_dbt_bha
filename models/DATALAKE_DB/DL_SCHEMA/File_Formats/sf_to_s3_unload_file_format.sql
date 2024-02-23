-- version control only, will not be compiled or deployed

CREATE OR REPLACE file format sf_to_s3_unload_file_format
	type=JSON TIME_FORMAT=AUTO DATE_FORMAT=AUTO COMPRESSION=NONE FILE_EXTENSION='json';