#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FFLeZCheck Text to SQLite Converter.

Converts the sub par text based FFL list from the ATF's FFLeZCheck website
into a searchable, organizable, SQLite database... it's also 33% smaller in
size than the original text file.

Usage: `fflezcheck2sqlite.py input.txt -o output.db`
"""

import argparse
import os
import sqlite3


def run(input_txt, output_db):
    """
    Actual main function.

    This function will extract and format the input text file and save the
    data to the ourput SQLite database. It will delete any existing file at
    the output location. If there is an error it should delete the output
    database.
    """
    has_entry = False

    if os.path.exists(output_db):
        print(f"Deleting existing \"{output_db}\"...")
        os.remove(output_db)

    print(f"Creating \"{output_db}\"...")
    conn = sqlite3.connect(output_db)
    cur = conn.cursor()

    print("Initializing database...")
    cur.execute(
        "CREATE TABLE entries (\
            uid                 INTEGER UNIQUE,\
            license_number      TEXT NOT NULL UNIQUE,\
            license_name        TEXT NOT NULL,\
            business_name       TEXT,\
            premise_street      TEXT NOT NULL,\
            premise_city        TEXT NOT NULL,\
            premise_state       TEXT NOT NULL,\
            premise_zip         TEXT NOT NULL,\
            mailing_street      TEXT NOT NULL,\
            mailing_city        TEXT NOT NULL,\
            mailing_state       TEXT NOT NULL,\
            mailing_zip         TEXT NOT NULL,\
            voice_telephone     TEXT NOT NULL,\
            loa_issue_date      TEXT,\
            loa_expiration_date TEXT,\
            PRIMARY KEY(uid AUTOINCREMENT)\
        );"
    )

    print(f"Opening \"{input_txt}\"...")
    with open(input_txt, "r", encoding="utf-8") as buf:
        print("Analyzing input data...")
        while True:
            line = buf.readline()

            if not line:
                break

            if line == "\n":
                continue

            has_entry = True

            # Format data from the current line, these locations are hardcoded
            # and padded with spaces if they come up short in character length
            # Positions shown here:
            # https://fflezcheck.atf.gov/FFLEzCheck/downloadFileLayout.action
            license_number = f"{line[0:1]}-{line[1:3]}-{line[3:6]}-{line[6:8]}-{line[8:10]}-{line[10:15]}"
            business_name = line[65:115].strip()
            if len(business_name) == 0:
                business_name = None
            premise_zip = line[197:206].strip()
            if len(premise_zip) > 5:
                premise_zip = f"{premise_zip[:5]}-{premise_zip[5:]}"
            mailing_zip = line[288:297].strip()
            if len(mailing_zip) > 5:
                mailing_zip = f"{mailing_zip[:5]}-{mailing_zip[5:]}"
            phone = f"+1 ({line[297:300]}) {line[300:303]}-{line[303:307]}"
            loa_issue = f"{line[311:315]}-{line[307:309]}-{line[309:311]}"
            if len(loa_issue.strip()) < 10:
                loa_issue = None
            loa_expiration = f"{line[319:323]}-{line[315:317]}-{line[317:319]}"
            if len(loa_expiration.strip()) < 10:
                loa_expiration = None

            cur.execute(
                "INSERT INTO entries (\
                    license_number, \
                    license_name, \
                    business_name, \
                    premise_street, \
                    premise_city, \
                    premise_state, \
                    premise_zip, \
                    mailing_street, \
                    mailing_city, \
                    mailing_state, \
                    mailing_zip, \
                    voice_telephone, \
                    loa_issue_date, \
                    loa_expiration_date\
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    license_number,
                    line[15:65].strip(),  # license_name
                    business_name,
                    line[115:165].strip(),  # premise_street
                    line[165:195].strip(),  # premise_city
                    line[195:197].strip(),  # premise_state
                    premise_zip,
                    line[206:256].strip(),  # mailing_street
                    line[256:286].strip(),  # mailing_city
                    line[286:288].strip(),  # mailing_state
                    mailing_zip,
                    phone,
                    loa_issue,
                    loa_expiration,
                )
            )

    conn.commit()
    conn.execute("VACUUM")
    conn.close()

    if not has_entry and os.path.exists(output_db):
        print(f"No entries found. Deleting \"{output_db}\"...")
        os.remove(output_db)
        return

    print(f"Successfully created \"{output_db}\"!")


def main():
    """
    First function called.

    This function is just used for parsing input args then passing the data to
    the `run()` function.
    """
    parser = argparse.ArgumentParser(
        description="FFLeZCheck Text to SQLite Converter",
        usage="%(prog)s [-h] input [-o OUTPUT]"
    )
    parser.add_argument(
        "input", action="store",
        help="Specify input txt file"
    )
    parser.add_argument(
        "-o", dest="output", action="store",
        default="output.db", required=False,
        help='Specify output file [default: "output.db"]'
    )
    args = parser.parse_args()

    run(args.input, args.output)


if __name__ == "__main__":
    main()
