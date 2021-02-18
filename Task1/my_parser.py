import pyarrow.parquet
import pandas
import pyarrow

parquet_schema = None


def parquet_to_csv(input_file, output_file, separator, escaped_char):
    if input_file.split(".")[1] != "parquet":
        raise Exception("PARQUET file must have extension .parquet")
    if output_file.split(".")[1] != "csv":
        raise Exception("CSV file must have extension .csv")
    table = pyarrow.parquet.read_table(input_file)
    global parquet_schema
    parquet_schema = table.schema
    table.to_pandas().to_csv(output_file, sep=separator, escapechar=escaped_char)


def csv_to_parquet(input_file, output_file, separator, escaped_char):
    if input_file.split(".")[1] != "csv":
        raise Exception("CSV file must have extension .csv")
    if output_file.split(".")[1] != "parquet":
        raise Exception("PARQUET file must have extension .parquet")
    csv_stream = pandas.read_csv(input_file, sep=separator, escapechar=escaped_char, chunksize=10, index_col=0)
    for i, chunk in enumerate(csv_stream):
        if i == 0:
            global parquet_schema
            parquet_schema = pyarrow.Table.from_pandas(df=chunk).schema
            parquet_writer = pyarrow.parquet.ParquetWriter(output_file, schema=parquet_schema)
        table = pyarrow.Table.from_pandas(df=chunk, schema=parquet_schema)
        parquet_writer.write_table(table)
    parquet_writer.close()


def schema(schema_file):
    with open(schema_file, mode="w") as file:
        file.write(str(parquet_schema.names) + "\n")
        file.write(str(parquet_schema.types))
