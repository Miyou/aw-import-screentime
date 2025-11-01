import pathlib
import sys

import blackboxprotobuf
import ccl_segb.ccl_segb1 as ccl_segb1
import ccl_segb.ccl_segb2 as ccl_segb2
from rich import inspect, pretty, print_json
from rich.console import Console
from rich.traceback import install

install(show_locals=True)
pretty.install()

console = Console()


def insp(arg):
    return inspect(arg, all=True, help=True)


def print_d(input_dict):
    return print_json(data=input_dict)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"USAGE: {pathlib.Path(sys.argv[0]).name} <SEGB file>")
        print()
        sys.exit(1)

    input_path = sys.argv[1]
    result = []
    if ccl_segb1.file_matches_segbv1_signature(input_path):
        for record in ccl_segb1.read_segb1_file(input_path):
            offset = record.data_start_offset
            state = record.state
            data = record.data
            ts1 = record.timestamp1
            ts2 = record.timestamp2

            if not any(data):  # null-padded record
                continue

            message, typedef = blackboxprotobuf.decode_message(data)

            result.append(
                {
                    "offset": offset,
                    "state": state,
                    "ts1": ts1.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "ts2": ts2.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "message": message,
                }
            )
        # print_d(result)
        console.print(result)
    elif ccl_segb2.file_matches_segbv2_signature(input_path):
        for record in ccl_segb2.read_segb2_file(input_path):
            offset = record.data_start_offset
            metadata_offset = record.metadata.metadata_offset
            state = record.metadata.state.name
            ts = record.metadata.creation
            data = record.data

            if not any(data):  # null-padded record
                continue
            message, typedef = blackboxprotobuf.decode_message(data)
            result.append(
                {
                    "offset": offset,
                    "metadata_offset": metadata_offset,
                    "state": state,
                    "ts": ts.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "message": message,
                }
            )
        # print_d(result)
        console.print(result)

    else:
        print("File is not a SEGB File")
        sys.exit(1)
    print()
