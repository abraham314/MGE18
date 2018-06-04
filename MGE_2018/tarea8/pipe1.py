import luigi

from luigi.s3 import S3Target

from somewhere import do_something_with


class MyS3File(luigi.ExternalTask):

    def output(self):
        return luigi.S3Target('s3://my-bucket/path/to/file')

class ProcessS3File(luigi.Task):

    def requires(self):
        return MyS3File()

    def output(self):
        return luigi.S3Target('s3://my-bucket/path/to/output-file')

    def run(self):
        result = None
        # this will return a file stream that reads the file from your aws s3 bucket
        with self.input().open('r') as f:
            result = do_something_with(f)

        # and the you 
        out_file = self.output().open('w')
        # it'd better to serialize this result before writing it to a file, but this is a pretty simple example
        out_file.write(result)
