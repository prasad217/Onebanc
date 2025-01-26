import pandas as pd
import re
import os


os.makedirs("InputFiles", exist_ok=True)
os.makedirs("OutputFiles", exist_ok=True)




def parse_idfc_amount(amount_str):
    if pd.isna(amount_str):
        return (None, None)
    amt = str(amount_str).strip()
    is_credit = bool(re.search(r'\bcr\b', amt, flags=re.IGNORECASE))
    amt_cleaned = re.sub(r'(?i)\bcr\b', '', amt).replace(',', '').strip()
    try:
        numeric_amt = float(amt_cleaned)
    except ValueError:
        numeric_amt = None
    txn_type = 'Credit' if is_credit else 'Debit'
    return (numeric_amt, txn_type)

def extract_idfc_location_and_currency(description):
    if not description or pd.isna(description):
        return (None, None, description)
    parts = str(description).strip().split()
    if len(parts) < 2:
        return (None, None, description)
    last_word = parts[-1].upper()
    if last_word in {"USD", "EUR", "INR"}:
        currency = last_word
        location = parts[-2]
        updated_description = " ".join(parts[:-1])
    else:
        currency = "INR"
        location = last_word
        updated_description = description
    return location, currency, updated_description

def find_idfc_transaction_type(df, i):
    for offset in [-2, -1]:
        row_idx = i + offset
        if row_idx >= 0:
            for col_idx in range(min(20, df.shape[1])):
                cell_val = df.iat[row_idx, col_idx]
                if not pd.isna(cell_val):
                    cell_str = str(cell_val).strip().lower()
                    if "domestic transactions" in cell_str:
                        return "Domestic"
                    elif "international transactions" in cell_str:
                        return "International"
    return None

def parse_idfc_transaction_data(df):
    n_rows = len(df)
    parsed_records = []
    i = 0
    while i < n_rows:
        row_i_first = str(df.iat[i, 0]) if not pd.isna(df.iat[i, 0]) else ""
        row_i_first = row_i_first.strip().lower()
        if "transaction details" in row_i_first:
            txn_type = find_idfc_transaction_type(df, i)
            cardholder = None
            if (i + 1) < n_rows:
                for col_idx in range(min(20, df.shape[1])):
                    cell_val = df.iat[i + 1, col_idx]
                    if not pd.isna(cell_val):
                        cell_str = str(cell_val).strip()
                        if cell_str and "transactions" not in cell_str.lower():
                            cardholder = cell_str
                            break
            i += 2
            while i < n_rows:
                row_i2_first = str(df.iat[i, 0]) if not pd.isna(df.iat[i, 0]) else ""
                if "transaction details" in row_i2_first.lower():
                    break
                desc_cell = str(df.iat[i, 0]) if not pd.isna(df.iat[i, 0]) else ""
                date_cell = str(df.iat[i, 1]) if (df.shape[1] > 1 and not pd.isna(df.iat[i, 1])) else ""
                amt_cell  = str(df.iat[i, 2]) if (df.shape[1] > 2 and not pd.isna(df.iat[i, 2])) else ""
                desc_cell = desc_cell.strip()
                if desc_cell:
                    location, currency, desc_cell = extract_idfc_location_and_currency(desc_cell)
                    numeric_amt, debit_credit = parse_idfc_amount(amt_cell)
                    try:
                        parsed_date = pd.to_datetime(date_cell, errors='coerce')
                    except:
                        parsed_date = None
                    inferred_txn_type = txn_type
                    if inferred_txn_type is None:
                        if currency == "INR":
                            inferred_txn_type = "Domestic"
                        elif currency:
                            inferred_txn_type = "International"
                    
                    if cardholder == "Rahul":
                        record = {
                            "Date": parsed_date,
                            "TransactionDesc": desc_cell,
                            "Debit": numeric_amt if (debit_credit == "Debit") else None,
                            "Credit": numeric_amt if (debit_credit == "Credit") else None,
                            "Currency": currency,
                            "Cardholder": cardholder,
                            "TransactionType": inferred_txn_type,
                            "Location": location
                        }
                        parsed_records.append(record)
                i += 1
            continue
        else:
            i += 1
    return pd.DataFrame(parsed_records)

def process_idfc(input_file, output_file):
    df = pd.read_csv(input_file, header=None)
    result_df = parse_idfc_transaction_data(df)
    result_df.to_csv(output_file, index=False)


process_idfc("InputFiles/IDFC-Input-Case4.csv", "OutputFiles/IDFC-Output-Case4.csv")
print("IDFC file processed and saved to OutputFiles/IDFC-Output-Case4.csv")

def extract_icici_location_and_currency(description, is_international=False):
    if not description or pd.isna(description):
        return (None, None, None)
    parts = str(description).strip().split()
    if len(parts) == 0:
        return (None, None, None)
    if is_international:
        if len(parts) >= 2:
            currency = parts[-1]
            location = parts[-2]
            if currency in ["USD", "EUR"]:
                description = " ".join(parts[:-1])
            else:
                currency = None
                location = None
        else:
            currency = None
            location = None
    else:
        currency = "INR"
        location = parts[-1]
    return (description, location, currency)

def parse_icici(df):
    current_txn_type = None
    current_cardholder = None
    parsed_records = []
    consecutive_empty_rows = 0

    def check_for_txn_type(row_values):
        for val in row_values:
            if pd.isna(val):
                continue
            text = str(val).lower()
            if "domestic transactions" in text:
                return "Domestic"
            if "international transaction" in text:
                return "International"
        return None

    for i in range(len(df)):
        if df.iloc[i].dropna().empty:
            consecutive_empty_rows += 1
            if consecutive_empty_rows > 5:
                break
            continue
        else:
            consecutive_empty_rows = 0

        row = df.iloc[i]
        row_values = list(row.values)

        found_type = check_for_txn_type(row_values)
        if found_type:
            current_txn_type = found_type
            continue

        row_non_empty = [str(x).strip() for x in row_values if not pd.isna(x)]
        if len(row_non_empty) == 1:
            val = row_non_empty[0]
            if re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', val):
                pass
            elif "transaction" in val.lower():
                pass
            else:
                current_cardholder = val
            continue

        date_str = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ""
        try:
            parsed_date = pd.to_datetime(date_str, format='%d-%m-%Y', errors='raise')
        except ValueError:
            continue

        desc_cell = str(row.iloc[1]).strip() if (df.shape[1] > 1 and not pd.isna(row.iloc[1])) else ""
        debit_cell = row.iloc[2] if (df.shape[1] > 2) else None
        credit_cell = row.iloc[3] if (df.shape[1] > 3) else None

        def to_float(x):
            try:
                return float(str(x).replace(',', ''))
            except:
                return None

        debit_val = to_float(debit_cell)
        credit_val = to_float(credit_cell)

        is_international = (current_txn_type == "International")
        desc_cell, location, currency = extract_icici_location_and_currency(desc_cell, is_international)

        inferred_type = current_txn_type
        if inferred_type is None:
            if currency == "INR":
                inferred_type = "Domestic"
            elif currency:
                inferred_type = "International"

        if current_cardholder == "Rahul":
            record = {
                "Date": parsed_date,
                "TransactionDesc": desc_cell,
                "Debit": debit_val,
                "Credit": credit_val,
                "Currency": currency,
                "Cardholder": current_cardholder,
                "TransactionType": inferred_type,
                "Location": location
            }
            parsed_records.append(record)

    return pd.DataFrame(parsed_records)

def process_icici(input_file, output_file):
    df_raw = pd.read_csv(input_file, header=None)
    icici_cleaned = parse_icici(df_raw)
    icici_cleaned.to_csv(output_file, index=False)
    print("ICICI file processed and saved to:", output_file)

process_icici("InputFiles/ICICI-Input-Case2.csv", "OutputFiles/ICICI-Output-Case2.csv")



def extract_axis_location_and_currency(description, is_international=False):
    if pd.isna(description):
        return (description, None, None)
    parts = str(description).strip().split()
    if not parts:
        return (description, None, None)
    if is_international:
        if len(parts) >= 2:
            currency = parts[-1]
            location = parts[-2]
            if currency in ["USD", "EUR"]:
                description = " ".join(parts[:-1])
            else:
                currency = None
                location = None
        else:
            currency = None
            location = None
    else:
        currency = "INR"
        location = parts[-1]
    return (description, location, currency)

def parse_axis(df):
    df.columns = ["Col0", "Col1", "Col2", "Col3"]
    df = df.dropna(how='all').reset_index(drop=True)
    current_txn_type = None
    current_cardholder = None
    parsed_records = []

    def looks_like_column_header(row_vals):
        row_strs = [str(x).strip().lower() for x in row_vals if pd.notna(x)]
        keywords = ["debit", "credit", "transaction details"]
        match_count = sum(1 for kw in keywords if any(kw in cell for cell in row_strs))
        return match_count >= 2

    def to_float(x):
        try:
            return float(str(x).replace(",", "").strip())
        except:
            return None

    for i in range(len(df)):
        row = df.iloc[i]
        row_vals = row.values
        row_combined_lower = " ".join(str(x).lower() for x in row_vals if pd.notna(x))

        if "domestic transactions" in row_combined_lower:
            current_txn_type = "Domestic"
            continue
        elif "international transactions" in row_combined_lower:
            current_txn_type = "International"
            continue

        row_non_empty = [str(x).strip() for x in row_vals if not pd.isna(x)]
        if len(row_non_empty) == 1:
            val = row_non_empty[0]
            if "transactions" not in val.lower():
                current_cardholder = val
            continue

        if looks_like_column_header(row_vals):
            continue

        date_str = str(row["Col0"]).strip()
        try:
            parsed_date = pd.to_datetime(date_str, format='%d-%m-%Y', errors='raise')
        except ValueError:
            continue

        debit_val = to_float(row["Col1"])
        credit_val = to_float(row["Col2"])
        txn_desc = str(row["Col3"]).strip() if pd.notna(row["Col3"]) else ""
        is_international = (current_txn_type == "International")
        txn_desc, location, currency = extract_axis_location_and_currency(txn_desc, is_international)

        if current_cardholder == "Rahul":
            record = {
                "Date": parsed_date,
                "TransactionDesc": txn_desc,
                "Debit": debit_val,
                "Credit": credit_val,
                "Currency": currency,
                "Cardholder": current_cardholder,
                "TransactionType": current_txn_type,
                "Location": location
            }
            parsed_records.append(record)

    final_df = pd.DataFrame(parsed_records)
    final_df.dropna(subset=['Date'], inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    final_df = final_df[["Date", "TransactionDesc", "Debit", "Credit", "Currency", "Cardholder", "TransactionType", "Location"]]
    return final_df


def process_axis(input_file, output_file):
    df_raw = pd.read_csv(input_file, header=None)
    cleaned_axis = parse_axis(df_raw)
    cleaned_axis.to_csv(output_file, index=False)
    print(f"Axis file processed and saved to {output_file}")

process_axis("InputFiles/Axis-Input-Case3.csv", "OutputFiles/Axis-Output-Case3.csv")



def parse_hdfc_amount(amount_str):
    if not amount_str or pd.isna(amount_str):
        return (None, None)
    amt_str = str(amount_str).strip().lower()
    is_credit = ('cr' in amt_str)
    amt_str_clean = re.sub(r'cr', '', amt_str, flags=re.IGNORECASE)
    amt_str_clean = amt_str_clean.replace(',', '').strip()
    try:
        numeric_val = float(amt_str_clean)
    except ValueError:
        numeric_val = None
    return (numeric_val, 'Credit' if is_credit else 'Debit')

def extract_hdfc_location_and_currency(desc, is_international):
    if not desc or pd.isna(desc):
        return (None, None, desc)
    parts = str(desc).strip().split()
    if is_international:
        if len(parts) >= 2:
            currency = parts[-1].upper()
            location = parts[-2]
            if currency in ["USD", "EUR"]:
                updated_desc = " ".join(parts[:-1])
                return (location, currency, updated_desc)
            else:
                return (None, None, desc)
        else:
            return (None, None, desc)
    else:
        currency = 'INR'
        location = parts[-1]
        return (location, currency, desc)


def parse_hdfc_data(df):
    df.columns = ['Col0', 'Col1', 'Col2']
    df.dropna(how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)
    current_cardholder = None
    current_txn_type = "Domestic"
    parsed_records = []

    def looks_like_heading(*cells):
        combined = " ".join(str(c).lower() for c in cells if pd.notna(c))
        possible_headings = [
            "transaction description", "date", "amount",
            "domestic transactions", "international transactions"
        ]
        return any(h in combined for h in possible_headings)

    for i in range(len(df)):
        row = df.iloc[i]
        col0, col1, col2 = row['Col0'], row['Col1'], row['Col2']
        combined_lower = " ".join(str(x).lower() for x in [col0, col1, col2] if pd.notna(x))
        if "domestic transactions" in combined_lower:
            current_txn_type = "Domestic"
            continue
        elif "international transactions" in combined_lower:
            current_txn_type = "International"
            continue
        if looks_like_heading(col0, col1, col2):
            continue
        non_empty_vals = [x for x in [col0, col1, col2] if pd.notna(x) and str(x).strip()]
        if len(non_empty_vals) == 1:
            val = str(non_empty_vals[0]).strip()
            if not re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', val):
                current_cardholder = val
                continue
        date_str = str(col0).strip() if pd.notna(col0) else ""
        try:
            parsed_date = pd.to_datetime(date_str, format="%d-%m-%Y", errors='raise')
        except ValueError:
            continue
        txn_desc = str(col1).strip() if pd.notna(col1) else ""
        amount_val, debit_credit = parse_hdfc_amount(col2)
        debit_amt = None
        credit_amt = None
        if debit_credit == "Credit":
            credit_amt = amount_val
        else:
            debit_amt = amount_val
        is_international = (current_txn_type == "International")
        location, currency, txn_desc = extract_hdfc_location_and_currency(txn_desc, is_international)
        
        if current_cardholder == "Rahul":
            record = {
                "Date": parsed_date,
                "TransactionDesc": txn_desc,
                "Debit": debit_amt,
                "Credit": credit_amt,
                "Cardholder": current_cardholder,
                "TransactionType": current_txn_type,
                "Currency": currency,
                "Location": location
            }
            parsed_records.append(record)

    final_df = pd.DataFrame(parsed_records)
    final_df = final_df[["Date", "TransactionDesc", "Debit", "Credit", "Cardholder", "TransactionType", "Currency", "Location"]]
    return final_df


def process_hdfc(input_file, output_file):
    df_raw = pd.read_csv(input_file)
    cleaned_hdfc = parse_hdfc_data(df_raw)
    cleaned_hdfc.to_csv(output_file, index=False)
    print(f"HDFC file processed and saved to {output_file}")

process_hdfc("InputFiles/HDFC-Input-Case1.csv", "OutputFiles/HDFC-Output-Case1.csv")
