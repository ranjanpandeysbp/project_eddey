from dateutil.parser import parse, ParserError
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType, StringType
from pyspark.sql import functions as F, SparkSession
import re
from typing import List, Dict, Tuple, Optional, Union
from pyspark.sql.functions import col, trim
from difflib import SequenceMatcher

spark = SparkSession.builder.appName("ProjectEddy").getOrCreate()
class normalisation:
    df = spark.read.format("delta").table("data/Accounting_Sample_Data_2.xlsx - Journal Entries.csv")
    df.show(5)

    #For date normalisation
    def try_parse_date(self, date_str):
        try:
            parsed_date = parse(date_str, fuzzy=True)
            return parsed_date
        except (ParserError, ValueError):
            return None

    def format_date(self, df):
        date_cols = ["Period", "Effective Date", "Entry Date"]
        parse_date_udf = udf(self.try_parse_date, DateType())
        for col_name in date_cols:
            df = df.withColumn(col_name, parse_date_udf(col(col_name)))
        return df
    def format_numeric_fields(self,df,
            float_cols=None,
            int_cols=None,
            float_precision=2,
            use_sep=True,
            int_padding=0
    ):
        """
        Format numeric fields in a PySpark DataFrame consistently.

        Parameters:
        - df (pyspark.sql.DataFrame): Input DataFrame
        - float_cols (list): Columns with float values to format
        - int_cols (list): Columns with integer values to format
        - float_precision (int): Number of decimal places for floats
        - use_sep (bool): Whether to use thousands separator
        - int_padding (int): Zero-padding width for integers (e.g., 5 => 00042)

        Returns:
        - pyspark.sql.DataFrame: Formatted DataFrame (as string values)
        """

        if float_cols:
            for col in float_cols:
                if col in df.columns:
                    if use_sep:
                        # Format with comma separator and specified precision
                        df = df.withColumn(
                            col,
                            F.when(
                                F.col(col).isNull(),
                                F.lit(None).cast(StringType())
                            ).otherwise(
                                F.format_number(F.col(col), float_precision)
                            )
                        )
                    else:
                        # Format without comma separator
                        df = df.withColumn(
                            col,
                            F.when(
                                F.col(col).isNull(),
                                F.lit(None).cast(StringType())
                            ).otherwise(
                                F.format_string(f"%.{float_precision}f", F.col(col))
                            )
                        )

        if int_cols:
            for col in int_cols:
                if col in df.columns:
                    if int_padding > 0:
                        # Format with zero padding
                        df = df.withColumn(
                            col,
                            F.when(
                                F.col(col).isNull(),
                                F.lit(None).cast(StringType())
                            ).otherwise(
                                F.format_string(f"%0{int_padding}d", F.col(col).cast("int"))
                            )
                        )
                    else:
                        # Format without padding
                        df = df.withColumn(
                            col,
                            F.when(
                                F.col(col).isNull(),
                                F.lit(None).cast(StringType())
                            ).otherwise(
                                F.col(col).cast("string")
                            )
                        )

        return df

    """
    # Example usage:

    # Sample usage with PySpark DataFrame

    spark = SparkSession.builder.appName("NumericFormatting").getOrCreate()

    # Create sample data
    data = [(1, 1234.5678, 42), (2, 9876.1234, 7), (3, None, None)]
    df = spark.createDataFrame(data, ["id", "amount", "count"])

    # Format the DataFrame
    formatted_df = format_numeric_fields(
        df,
        float_cols=["amount"],
        int_cols=["count"],
        float_precision=2,
        use_sep=True,
        int_padding=5
    )

    display(formatted_df)
    """

    def handling_missing_values(self, df):
        """
        Handle missing values in the DataFrame according to defined business rules.
        """

        # --- Critical Fields ---
        # 1. Journal Entry Number (Must not be missing)
        if "Journal Entry Number" in df.columns:
            missing_je = df.filter(F.col("Journal Entry Number").isNull())
            if missing_je.count() > 0:
                print("Missing Journal Entry Numbers detected — further investigation required.")

        # 2. Fiscal Year (Can be hardcoded if not present)
        if "Fiscal Year" not in df.columns:
            print("Fiscal Year column missing — consider deriving from analysis period / filename / effective date.")

        # 3. Business Unit (Mandatory)
        if "Business Unit" in df.columns:
            missing_bu = df.filter(F.col("Business Unit").isNull())
            if missing_bu.count() > 0:
                print("Missing Business Unit values detected — further investigation required.")

        # 4. JE Line Number (Preferred, not mandatory — allow missing)
        # No special handling required

        # 5. GL Account Number (Must not be missing)
        if "GL Account Number" in df.columns:
            missing_gl = df.filter(F.col("GL Account Number").isNull())
            if missing_gl.count() > 0:
                print("Missing GL Account Numbers detected — further investigation required.")

        # --- Date Fields ---
        # 6. Period (Generate from Effective Date if missing)
        if "Period" in df.columns and "Effective Date" in df.columns:
            df = df.withColumn(
                "Period",
                F.when(
                    F.col("Period").isNull() & F.col("Effective Date").isNotNull(),
                    F.date_format(F.col("Effective Date"), "yyyyMM")
                ).otherwise(F.col("Period"))
            )

        # 7. Effective Date (Must not be missing)
        if "Effective Date" in df.columns:
            missing_ed = df.filter(F.col("Effective Date").isNull())
            if missing_ed.count() > 0:
                print("Missing Effective Dates detected — investigation required. Consider dummy dates if justified.")

        # 8. Entry Date (Optional)
        if "Entry Date" in df.columns:
            df = df.withColumn("Entry Date", F.to_date(F.col("Entry Date")))

        # --- Amount & Currency ---
        # 9. Functional Amount (Set to 0 if null)
        if "Functional Amount" in df.columns:
            df = df.withColumn(
                "Functional Amount",
                F.when(F.col("Functional Amount").isNull(), F.lit(0)).otherwise(F.col("Functional Amount"))
            )

        # 10. Functional Currency Code (Single currency → fill, else check Business Unit)
        if "Functional Currency Code" in df.columns:
            unique_currencies = [row[0] for row in df.select("Functional Currency Code").distinct().collect() if
                                 row[0] is not None]
            if len(unique_currencies) == 1:
                df = df.withColumn("Functional Currency Code", F.lit(unique_currencies[0]))
            else:
                if "Business Unit" in df.columns:
                    window = F.when(F.col("Functional Currency Code").isNull(), F.lit("CHECK_BU_MAPPING")).otherwise(
                        F.col("Functional Currency Code"))
                    df = df.withColumn("Functional Currency Code", window)
                    print("Multiple currencies detected — fill missing based on Business Unit mapping.")

        # --- Descriptions & Text Fields ---
        # 11. Descriptions/User Defined → keep missing values as is

        # 12. User/Source (Fill with Unknown, flag for investigation)
        for col_name in ["Source", "Preparer ID"]:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), F.lit("Unknown")).otherwise(F.col(col_name))
                )

        # --- Check Remaining Missing Values ---
        print("\nMissing values after handling:")
        for column in df.columns:
            null_count = df.filter(F.col(column).isNull()).count()
            if null_count > 0:
                print(f"{column}: {null_count}")

        # --- Balance Check ---
        if "Journal Entry Number" in df.columns and "Functional Amount" in df.columns:
            journal_totals = df.groupBy("Journal Entry Number") \
                .agg(F.sum("Functional Amount").alias("total_amount"))

            imbalanced_entries = journal_totals.filter(F.col("total_amount") != 0)
            if imbalanced_entries.count() > 0:
                print("\nFound journal entries that do not balance:")
                imbalanced_entries.show()

        return df

    def skip_empty_rows(self, df, required_columns: List[str] = None,
                        skip_mode: str = 'auto'):
        """
        Skip empty rows or flag them based on requirements.

        Parameters:
        - df: Input DataFrame
        - required_columns: List of columns that must have values
        - skip_mode: 'auto' (skip empty), 'flag' (add flag column), 'strict' (require all non-null)

        Returns:
        - Processed DataFrame
        """
        print(f"Original row count: {df.count()}")

        if skip_mode == 'auto':
            # Skip rows where all columns are null or empty
            non_empty_condition = None
            for column in df.columns:
                col_condition = (col(column).isNotNull()) & (trim(col(column)) != "")
                if non_empty_condition is None:
                    non_empty_condition = col_condition
                else:
                    non_empty_condition = non_empty_condition | col_condition

            filtered_df = df.filter(non_empty_condition)
            print(f"Rows after removing empty: {filtered_df.count()}")
            return filtered_df

        elif skip_mode == 'flag':
            # Add flag column for empty rows
            empty_condition = None
            for column in df.columns:
                col_condition = (col(column).isNull()) | (trim(col(column)) == "")
                if empty_condition is None:
                    empty_condition = col_condition
                else:
                    empty_condition = empty_condition & col_condition

            flagged_df = df.withColumn("is_empty_row", empty_condition)
            empty_count = flagged_df.filter(col("is_empty_row") == True).count()
            print(f"Empty rows flagged: {empty_count}")
            return flagged_df

        elif skip_mode == 'strict' and required_columns:
            # Skip rows where required columns are null/empty
            required_condition = None
            for column in required_columns:
                if column in df.columns:
                    col_condition = (col(column).isNotNull()) & (trim(col(column)) != "")
                    if required_condition is None:
                        required_condition = col_condition
                    else:
                        required_condition = required_condition & col_condition

            if required_condition is not None:
                filtered_df = df.filter(required_condition)
                print(f"Rows after strict filtering: {filtered_df.count()}")
                return filtered_df

        return df

    def similarity_score(self, a: str, b: str) -> float:
        """Calculate similarity between two strings using SequenceMatcher"""
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

    def fuzzy_match_column(self, df, column_name: str, threshold: float = 0.7) -> Optional[str]:
        expected_columns = [
            "Fiscal Year", "Business Unit", "Journal Entry Number", "Period",
            "Effective Date", "Source", "GL Account Number", "GL Account Name",
            "Functional Amount", "Functional Currency Code", "JE Description",
            "Preparer ID", "Entry Date", "JE Line Number", "JE Line Description",
            "User Defined Field 1", "User Defined Field 2", "User Defined Field 3"
        ]

        # Required columns that should not be empty
        required_columns = [
            "Fiscal Year", "Business Unit", "Journal Entry Number",
            "JE Line Number", "GL Account Number"
        ]

        # Column variations and abbreviations for fuzzy matching
        column_variations = {
            "Fiscal Year": ["FY", "fiscal_year", "fiscal yr", "year", "fy"],
            "Business Unit": ["BU", "business_unit", "dept", "department", "bu"],
            "Journal Entry Number": ["JE_Number", "journal_no", "je_no", "entry_number", "journal_entry_no"],
            "Period": ["period", "accounting_period", "acct_period", "pd"],
            "Effective Date": ["eff_date", "effective_dt", "date", "transaction_date"],
            "Source": ["source", "src", "journal_source", "je_source"],
            "GL Account Number": ["account_no", "gl_account", "account_number", "acct_no", "gl_acct"],
            "GL Account Name": ["account_name", "gl_account_name", "acct_name", "gl_acct_name"],
            "Functional Amount": ["amount", "func_amount", "functional_amt", "amt", "value"],
            "Functional Currency Code": ["currency", "curr_code", "func_currency", "currency_code"],
            "JE Description": ["description", "je_desc", "journal_description", "desc"],
            "Preparer ID": ["preparer", "prepared_by", "user_id", "prep_id"],
            "Entry Date": ["entry_dt", "created_date", "input_date", "entry_date"],
            "JE Line Number": ["line_no", "je_line_no", "line_number", "seq_no"],
            "JE Line Description": ["line_desc", "line_description", "je_line_desc", "detail_desc"],
            "User Defined Field 1": ["udf1", "custom1", "field1", "user_field1"],
            "User Defined Field 2": ["udf2", "custom2", "field2", "user_field2"],
            "User Defined Field 3": ["udf3", "custom3", "field3", "user_field3"]
        }
        """
        Find the best matching expected column name using fuzzy matching
        """
        best_match = None
        best_score = 0

        cleaned_column = re.sub(r'[^\w\s]', '', column_name.lower().strip())
        cleaned_column = re.sub(r'\s+', ' ', cleaned_column)

        for expected_col in expected_columns:
            # Check exact match first
            if cleaned_column == expected_col.lower():
                return expected_col

            # Check variations
            variations = column_variations.get(expected_col, [])
            for variation in variations:
                score = self.similarity_score(cleaned_column, variation)
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = expected_col

            # Check similarity with main column name
            score = self.similarity_score(cleaned_column, expected_col)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = expected_col

        return best_match
