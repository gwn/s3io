import unittest

import boto
from boto.s3.key import Key

import s3io


CREDENTIALS = dict(
    aws_access_key_id='<ACCESS_KEY>',
    aws_secret_access_key='<SECRET_KEY>',
)
BUCKET = '<TEST_BUCKET>'


def get_bucket():
    s3 = boto.connect_s3(**CREDENTIALS)
    return s3.get_bucket(BUCKET)


def get_contents(key):
    bucket = get_bucket()
    k = Key(bucket)
    k.key = key
    return k.get_contents_as_string()


class ConnectionTests(unittest.TestCase):
    """Used to test that credentials are correct and we can connect to S3."""

    def test_s3_connection(self):
        """Test connection to S3 with supplied credentials."""

        get_bucket()


class ReadingTests(unittest.TestCase):

    TEST_KEY = 'reading_test.txt'
    TEST_CONTENTS = b'Some read test text on S3.\nSecond line.'
    S3_URL = 's3://{0}/{1}'.format(BUCKET, TEST_KEY)

    def setUp(self):
        bucket = get_bucket()
        k = Key(bucket)
        k.key = self.TEST_KEY
        k.set_contents_from_string(self.TEST_CONTENTS)

    def tearDown(self):
        bucket = get_bucket()
        bucket.delete_key(self.TEST_KEY)

    def test_reading_via_predefined_connection(self):
        s3 = boto.connect_s3(**CREDENTIALS)

        with s3io.open(self.S3_URL, s3_connection=s3) as s3_file:
            contents = s3_file.read()
            self.assertEqual(contents, self.TEST_CONTENTS)

    def test_reading_via_credentials(self):
        with s3io.open(self.S3_URL, **CREDENTIALS) as s3_file:
            contents = s3_file.read()
            self.assertEqual(contents, self.TEST_CONTENTS)

    def test_reading_not_existent_file(self):

        def read_not_existent_key():
            S3_URL = 's3://{0}/{1}'.format(BUCKET, 'not_existent_key')
            with s3io.open(S3_URL, **CREDENTIALS):
                pass

        self.assertRaises(s3io.KeyNotFoundError, read_not_existent_key)

    def test_providing_invalid_bucket(self):

        def read_not_existent_bucket():
            S3_URL = 's3://{0}/{1}'.format(
                'not_existent_bucket_hjshewighksfdkjffh', 'not_existent_key')

            with s3io.open(S3_URL, **CREDENTIALS):
                pass

        self.assertRaises(s3io.BucketNotFoundError, read_not_existent_bucket)

    def test_providing_invalid_s3_url(self):

        def open_invalid_url():
            INVALID_S3_URL = 's3://something'

            with s3io.open(INVALID_S3_URL, **CREDENTIALS):
                pass

        self.assertRaises(s3io.UrlParseError, open_invalid_url)


class WritingTests(unittest.TestCase):

    TEST_KEY = 'writing_test.txt'
    TEST_CONTENTS = b'Some write test text on S3.\nSecond line.'
    S3_URL = 's3://{0}/{1}'.format(BUCKET, TEST_KEY)

    def setUp(self):
        bucket = get_bucket()
        bucket.delete_key(self.TEST_KEY)

    def tearDown(self):
        self.setUp()

    def test_writing_via_predefined_connection(self):
        s3 = boto.connect_s3(**CREDENTIALS)

        with s3io.open(self.S3_URL, mode='w', s3_connection=s3) as s3_file:
            s3_file.write(self.TEST_CONTENTS)

        self.assertEqual(get_contents(self.TEST_KEY), self.TEST_CONTENTS)

    def test_writing_via_credentials(self):
        with s3io.open(self.S3_URL, mode='w', **CREDENTIALS) as s3_file:
            s3_file.write(self.TEST_CONTENTS)

        self.assertEqual(get_contents(self.TEST_KEY), self.TEST_CONTENTS)

    def test_writing_twice(self):
        with s3io.open(self.S3_URL, mode='w', **CREDENTIALS) as s3_file:
            s3_file.write(b'Some other data.')

        self.assertEqual(get_contents(self.TEST_KEY), b'Some other data.')

        with s3io.open(self.S3_URL, mode='w', **CREDENTIALS) as s3_file:
            s3_file.write(self.TEST_CONTENTS)

        self.assertEqual(get_contents(self.TEST_KEY), self.TEST_CONTENTS)

if __name__ == '__main__':
    unittest.main()
