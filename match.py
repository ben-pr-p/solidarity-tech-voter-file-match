import marimo

__generated_with = "0.11.14"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import re
    return mo, pl, re


@app.cell
def _(mo):
    api_key_input = mo.ui.text(label="Input your solidarity tech api key")
    api_key_input
    return (api_key_input,)


@app.cell
def _(api_key_input, mo):
    mo.stop(api_key_input.value is None)

    import requests


    def get_people_page(limit: int, offset: int):
        url = f"https://api.solidarity.tech/v1/users?_limit={limit}&_offset={offset}"

        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {api_key_input.value}",
        }

        response = requests.get(url, headers=headers)
        return response.json()


    def download_all_people(limit: int, predicate=None):
        page = 0
        result = []

        offset = 0
        total_count = 0

        while len(result) < limit:
            if total_count == 0:
                print(f"Downloading page {page}")
            else:
                print(f"Downloading page {page} out of {total_count / 100}")

            next_page = get_people_page(100, offset)

            if "meta" in next_page:
                total_count = next_page["meta"]["total_count"]

            if "data" not in next_page:
                print(next_page)
                break

            if len(next_page["data"]) == 0:
                break

            if predicate is None:
                filtered_page = next_page["data"]
            else:
                filtered_page = filter(predicate, next_page["data"])

            to_add = list(filtered_page)
            print(f"Adding {len(to_add)} people who have addresses")
            result.extend(to_add)
            offset += 100
            page += 1

        return result


    people = download_all_people(
        6000,
        lambda p: "address1" in p["address"] and p["address"]["address1"] is not None,
    )

    mo.md(f"""
      This cell downloads people from solidarity tech.

      There's a hardcoded limit of 500 people to download for debugging - remove that to run the full thing.
    """)
    return download_all_people, get_people_page, people, requests


@app.cell
def _():
    BANNED = {
        "rd",
        "dr",
        "ave",
        "blvd",
        "st",
        "ln",
        "ct",
        "hwy",
        "pkwy",
        "circle",
        "terrace",
        "road",
        "drive",
        "avenue",
        "boulevard",
        "street",
        "lane",
        "court",
        "highway",
        "parkway",
    }

    _banned_upper_list = map(lambda w: w.upper(), list(BANNED))
    BANNED_REGEX_STRING = "|".join(_banned_upper_list)
    return BANNED, BANNED_REGEX_STRING


@app.cell
def _(people):
    people
    return


@app.cell
def _(BANNED, df, mo, people, pl, re):
    _initial_df = pl.DataFrame(
        map(
            lambda p: (
                {
                    "id": p["id"],
                    "first_name": p["first_name"],
                    "last_name": p["last_name"],
                    "address": p["address"],
                }
            ),
            people,
        )
    )


    # def get_addr_components(address):
    #     # Remove punctuation and split on whitespace
    #     tokens = re.split(r"\s+", re.sub(r"[^\w\s]", "", address))
    #     valid = []
    #     for token in tokens:
    #         t = token.lower()
    #         if re.fullmatch(r"\d+", t):
    #             valid.append(t)
    #         elif re.fullmatch(r"[a-z0-9]+", t) and t not in BANNED:
    #             valid.append(t.upper())
    #     return valid


    def get_addr_components(address):
        # Remove punctuation and split on whitespace
        tokens = re.split(r"\s+", re.sub(r"[^\w\s]", "", address))
        valid = []
        for token in tokens:
            t = token.lower()

            # Pure numeric
            if re.fullmatch(r"\d+", t):
                valid.append(t)

            # Pure alphabetic
            elif re.fullmatch(r"[a-z]+", t) and t not in BANNED:
                valid.append(t.upper())

            # Pure alphanumeric
            elif re.fullmatch(r"[a-z0-9]+", t) and t not in BANNED:
                valid.append(t.upper())

            # Mixed alphanumeric handling
            mixed_match = re.match(r"(\d+)([a-z]+)", t, re.IGNORECASE)
            if mixed_match:
                num, alpha = mixed_match.groups()
                # Add individual components
                valid.extend(
                    [
                        num,  # numeric part
                        alpha.upper(),  # alphabetic part
                        t.upper(),  # full mixed token
                        alpha.upper() + num,  # flipped order
                    ]
                )

            mixed_match_reverse = re.match(r"([a-z]+)(\d+)", t, re.IGNORECASE)
            if mixed_match_reverse:
                alpha, num = mixed_match_reverse.groups()
                # Add individual components
                valid.extend(
                    [
                        num,  # numeric part
                        alpha.upper(),  # alphabetic part
                        t.upper(),  # full mixed token
                        num + alpha.upper(),  # flipped order
                    ]
                )

        # Remove duplicates while preserving order
        return list(dict.fromkeys(valid))


    # Define a function to create the full address string
    def combine_address(addr):
        full_addr = f"{addr['address1']} {addr.get('address2', '')}".strip()
        return get_addr_components(full_addr)

        # Add the new column using map_elements
        return df.with_columns(
            pl.col("address")
            .map_elements(combine_address, return_dtype=pl.List(pl.Utf8))
            .alias("normalized_address_parts")
        )


    people_df = _initial_df.with_columns(
        pl.col("address")
        .map_elements(combine_address, return_dtype=pl.List(pl.Utf8))
        .alias("normalized_address_parts")
    )

    mo.md(
        f"""
        This cell adds a column to people that contains the components of their address as normalized strings.

        Here, normalized means:

        - Uppercased

        - Trimmed

        - With street types (AVENUE, AVE, AV,. etc.) removed, as reconciling those can be difficult 
        """
    )
    return combine_address, get_addr_components, people_df


@app.cell
def _(mo, people_df):
    normalized_people = mo.sql(
        f"""
        SELECT 
            id as solidarity_tech_id,
            upper(first_name) as first_name, 
            upper(last_name) as last_name, 
            address['zip_code'] as zip5, 
            normalized_address_parts
        FROM people_df
        """
    )
    return (normalized_people,)


@app.cell
def _(mo):
    mo.md(
        r"""
        First pass of matching - a match requires:

        - Complete last name match
        - Complete zip match
        - Each component of the voter file address is in the normalized Solidarity Tech Address string 

        After that, each match is *either* a direct match or a household match.
        """
    )
    return


@app.cell
def _(BANNED_REGEX_STRING, mo):
    match = mo.sql(
        f"""
        SELECT 
            distinct on (normalized_people.solidarity_tech_id, voters.sboeid)
            normalized_people.solidarity_tech_id, 
            normalized_people.first_name as solidarity_tech_first, 
            normalized_people.last_name as solidarity_tech_last,
            voters.first_name as voter_first,
            voters.last_name as voter_last,
            normalized_people.normalized_address_parts,
            voters.clean_raddrnumber,
            voters.original_street_name,
            voters.apt_num,
            voters.sboeid as boe_id,
            voters.status as registration_status,
            voters.enrollment as party_enrollment,
            case 
                when voters.first_name <> normalized_people.first_name then 'Household/Review'
                else 'Direct'
            end as match_type,
            case 
              when (
                voters.apt_num is null or voters.apt_num = '' 
                or voters.apt_num = ANY(normalized_people.normalized_address_parts)
              ) then 'Apt Number Matches'
              else 'Apt Number NO Match - Needs Review'
            end as apt_num_match
        FROM normalized_people
        LEFT JOIN (
            select 
                sboeid,
                enrollment,
                status,
                upper(firstname) as first_name, 
                upper(lastname) as last_name, 
                regexp_replace(upper(rstreetname), '(^| )({BANNED_REGEX_STRING})( |$)', '', 'g') as street_name_without_addr, 
                rstreetname as original_street_name,
                upper(rapartment) as apt_num, 
                regexp_replace(raddnumber, '[^a-zA-Z0-9]', '', 'g') as clean_raddrnumber, 
                rzip5
            from './voterfile.parquet' voterfile
            where rzip5 in (
                select zip5 from normalized_people 
            )
        ) voters on voters.last_name = normalized_people.last_name
            and voters.rzip5 = normalized_people.zip5
        where true
          and voters.clean_raddrnumber = ANY(normalized_people.normalized_address_parts)
          and voters.street_name_without_addr = ANY(normalized_people.normalized_address_parts)
        """
    )
    return (match,)


@app.cell
def _(mo):
    mo.md(r"""The cell below is just to have a sample of the voterfile present for column discovery.""")
    return


@app.cell
def _(mo):
    voterfile_sample = mo.sql(
        f"""
        SELECT * FROM './voterfile.parquet' limit 5
        """
    )
    return (voterfile_sample,)


if __name__ == "__main__":
    app.run()
