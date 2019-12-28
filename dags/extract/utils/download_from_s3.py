''' Download files from S3.'''
import threading
import zlib
import pandas as pd

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.handlers import disable_signing

AWS = boto3.session.Session()
#s3 = aws.client('s3', region_name='eu-west-1')
S3 = AWS.resource('s3')
#allow anonymous access
S3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

class ProgressPercentage():
    """Show process percentage
    Adapted from boto3 documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/s3/transfer.html

    Parameters:
    obj: S3 Object
    """
    def __init__(self, obj):
        self._obj = obj
        self._size = self._obj.content_length
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = round((self._seen_so_far / self._size) * 100, 2)
            print(
                '{} is the file name. {} out of {} done. The percentage completed is {} %'.format(
                    str(self._obj.key), str(self._seen_so_far),
                    str(self._size), str(percentage)))

def split_s3_path(url: str):
    """ Splits https url for s3 file
    
    Parameters:
        url (str): https url to s3 location

    Returns:
        domain (str): The domain part of the url
        bucket (str): The S3 bucket name
        key (str): The S3 key
        filename: The last part of the key
    """
    path_parts = url.replace("https://", "").split("/")
    domain = path_parts.pop(0)
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    filename = path_parts.pop()
    return domain, bucket, key, filename

def download(url: str):
    print('Downloading from s3: {}'.format(url))
    domain, bucket, key, filename = split_s3_path(url)
    outfile = 'out_' + filename

    try:
        conf = TransferConfig()
        obj = S3.Object(bucket_name=bucket, key=key)
        print(obj)
        size = obj.content_length
        print(size)
        progress = ProgressPercentage(obj)
        with open(outfile, 'wb') as ofile:
            obj.download_fileobj(ofile, Callback=progress, Config=conf)
        return ofile.name
    except Exception as exception:
        print('Error while downloading: {}'.format(str(exception)))
        return None

def stream_gzip_decompress(stream):
    dec = zlib.decompressobj(32 + zlib.MAX_WBITS)  # offset 32 to skip the header
    try:
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv
    except Exception as exception:
        print('Error while decompressing: {}'.format(str(exception)))

def iterlines(decompressed_stream):
    buf = b''
    # keep reading chunks of bytes into buffer
    for chunk in decompressed_stream:
        buf += chunk
        start = 0
        # process all lines within current buffer
        while True:
            end = buf.find(b'\n', start) + 1
            if end:
                yield buf[start:end]
                start = end
            else:
                # no more newlines => break out to read more data from stream into the buffer
                buf = buf[start:]
                break
    # process last line
    if buf:
        yield buf

def download_stream(url: str):
    domain, bucket, key, filename = split_s3_path(url)
    obj = S3.Object(bucket_name=bucket, key=key)
    dstream = stream_gzip_decompress(obj.get()['Body'])
    return iterlines(dstream)

def file_stream(path: str):
    if path.find('.gz') > 0:
        inputstream = stream_gzip_decompress(open(path, mode='rb'))
    else:
        inputstream = open(path, mode='rb')
    return iterlines(inputstream)

def json_stream(path: str):
    #inputstream = stream_gzip_decompress(open(path, mode='rb'))
    json_reader = pd.read_json(path, lines=True, chunksize=1000)
    return json_reader

def main():
    stream = download_stream(
               'https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz'
               )
#    stream = file_stream('../../Desktop/ta/item_dedup.json.gz')
#    stream = json_stream('../../Desktop/ta/item_dedup.json.gz')
    for line in stream:
        try:
            print(line)
            exit()
        except Exception as error:
            print(line)
            print(str(error))
    

#    download('https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz')

if __name__ == '__main__':
    main()