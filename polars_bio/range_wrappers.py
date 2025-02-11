from .operations import do_range_operation, LEFT_TABLE, RIGHT_TABLE
import datafusion
from polars_bio.polars_bio import (
    BioSessionContext,
    RangeOptions,
    range_operation_frame,
    range_operation_scan,
    stream_range_operation_scan,
    RangeOp
)
from pathlib import Path


def range_operation_frame_wrapper(
    ctx: BioSessionContext,
    df1,
    df2,
    range_options: RangeOptions,
) -> datafusion.dataframe:
    if range_options.range_op != RangeOp.CountOverlaps:
        return range_operation_frame(ctx, df1, df2, range_options)
    py_ctx = datafusion.SessionContext()
    my_df1 = py_ctx.from_arrow(df1, LEFT_TABLE)
    my_df2 = py_ctx.from_arrow(df2, RIGHT_TABLE)
    return do_range_operation(py_ctx, range_options)

def register_file(py_ctx, df, table_name):
    ext = Path(df).suffix
    py_ctx.register_listing_table(table_name, df, file_extension=ext)

def range_operation_scan_wrapper(
    ctx: BioSessionContext,
    df1: str,
    df2: str,
    range_options: RangeOptions
) -> datafusion.dataframe:
    if range_options.range_op != RangeOp.CountOverlaps:
        return range_operation_scan(ctx, df1, df2, range_options)
    py_ctx = datafusion.SessionContext()
    register_file(py_ctx, df1, LEFT_TABLE)
    register_file(py_ctx, df2, RIGHT_TABLE)
    return do_range_operation(py_ctx, range_options)


