import sys
import argparse
import my_parser


def create_parser():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--csv_to_parquet', "--c2p", action="store_true", default=False)
    arg_parser.add_argument('--parquet_to_csv', "--p2c", action="store_true", default=False)
    arg_parser.add_argument("-i", "--input_file", required=True, type=str)
    arg_parser.add_argument("-o", "--output_file", required=True, type=str)
    arg_parser.add_argument("-s", "--separator", type=str, default=";")
    arg_parser.add_argument("-e", "--escaped_char", type=str, default="\"")
    arg_parser.add_argument("--schema", "--sch", type=str, default=None)
    return arg_parser


def main():
    try:
        arg_parser = create_parser()
        namespace = arg_parser.parse_args(sys.argv[1:])

        c2p = namespace.csv_to_parquet
        p2c = namespace.parquet_to_csv
        input_file = namespace.input_file
        output_file = namespace.output_file
        separator = namespace.separator
        escaped_char = namespace.escaped_char
        schema_file = namespace.schema

        if (c2p and p2c) or not (c2p or p2c):
            raise Exception("Choose one converter option")

        if c2p:
            my_parser.csv_to_parquet(input_file, output_file, separator, escaped_char)
        else:
            my_parser.parquet_to_csv(input_file, output_file, separator, escaped_char)

        if schema_file is not None:
            my_parser.schema(schema_file)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
