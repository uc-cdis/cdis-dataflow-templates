from __future__ import absolute_import

import argparse
import logging
import timeit

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json

try:
    unicode  # pylint: disable=unicode-builtin
except NameError:
    unicode = str

FILE_HEADERS = ["id", "file_name", "size", "md5", "acl", "project_id"]

#
class FileCopyingDoFn(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, element):
        """Process each line of the manifest file.
        The element is a line of text.
        Args:
          element: the row being processed
        Returns:
          The outcome of the copying process. True/False means success/failure
        """
        text_line = element.strip()
        words = text_line.split()
        fi = dict(zip(FILE_HEADERS, words))
        fi["size"] = int(fi["size"])

        return [(fi, "processed")]


def format_result(result):
    (fi, datalog) = result
    return "%s\t%s\t%d\t%s\t%s\t%s" % (
        fi.get("id"),
        fi.get("file_name"),
        int(fi.get("size")),
        fi.get("md5"),
        fi.get("acl"),
        fi.get("project_id")
    )


class BucketManifestOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    # Use add_value_provider_argument for arguments to be templatable
    # Use add_argument as usual for non-templatable arguments
    parser.add_value_provider_argument(
        '--input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Path of the file to read from')
    parser.add_argument(
        '--output',
        required=True,
        help='Output file to write results to.')

def run(argv=None):
    """Main entry point; defines and runs the copying pipeline."""
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--input",
    #     dest="input",
    #     default="./scripts/test_data.txt",
    #     help="Input file to process.",
    # )
    # parser.add_argument(
    #     "--output",
    #     dest="output",
    #     required=True,
    #     help="Output file to write results to.",
    # )
    # parser.add_argument(
    #     "--global_config", dest="global_config", help="global configuration"
    # )
    # known_args, pipeline_args = parser.parse_known_args(argv)
    my_options = PipelineOptions().view_as(BucketManifestOptions)
    
    pipeline_options = PipelineOptions()
    
    #pipeline_options = PipelineOptions(pipeline_args)
    #pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    #input_path = known_args.input

    # Read the text file[pattern] into a PCollection.
    lines = p | "read" >> ReadFromText(
        file_pattern=my_options.input, skip_header_lines=1
    )
    result = lines | "copy" >> beam.ParDo(FileCopyingDoFn())
    formated_result = result | "format" >> beam.Map(format_result)
    formated_result | "write" >> WriteToText(my_options.output)
    prog = p.run()
    prog.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    start = timeit.default_timer()
    run()
    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))
