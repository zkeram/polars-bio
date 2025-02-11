from polars_bio.polars_bio import InputFormat

from .context import ctx
from .io import read_bam, read_fasta, read_fastq, read_table, read_vcf
from .polars_ext import PolarsRangesOperations as LazyFrame
from .range_op import FilterOp, nearest, overlap, merge, cluster, coverage, count_overlaps
from .range_viz import visualize_intervals

POLARS_BIO_MAX_THREADS = "datafusion.execution.target_partitions"


__version__ = "0.5.4"
__all__ = [
    "overlap",
    "nearest",
    "coverage",
    "merge",
    "cluster",
    "count_overlaps",
    "ctx",
    "FilterOp",
    "visualize_intervals",
    "read_bam",
    "read_vcf",
    "read_fasta",
    "read_fastq",
    "read_table",
    "InputFormat",
    "LazyFrame",
]
