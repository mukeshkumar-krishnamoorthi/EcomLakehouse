import yaml
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class DataQualityValidator:
    def __init__(self, table_name: str, config_path: str = "config/quality_rules.yml"):
        self.table_name = table_name
        self.config_path = config_path
        self.rules: list[tuple[str, F.Column]] = []
        self._load_rules()

    def _load_rules(self):
        with open(self.config_path) as f:
            config = yaml.safe_load(f)

        table_rules = config.get("tables", {}).get(self.table_name, {})

        # 1. Completeness Check
        for rule in table_rules.get("completeness", []):
            for col in rule.get("columns", []):
                self.rules.append((f"{col}_not_null", F.col(col).isNotNull()))

        # 2. Allowed Values Check
        for rule in table_rules.get("allowed_values", []):
            col = rule["column"]
            values = rule["values"]
            self.rules.append(
                (f"{col}_allowed_values", F.col(col).isin(values)))

        # 3. Range Check
        for rule in table_rules.get("range_check", []):
            col = rule["column"]
            if "min" in rule:
                self.rules.append(
                    (f"{col}_min_{rule['min']}", F.col(col) >= rule["min"]))
            if "min_exclusive" in rule:
                self.rules.append(
                    (f"{col}_gt_{rule['min_exclusive']}", F.col(col) > rule["min_exclusive"]))

        # 4. Temporal Logic Check (cannot be future)
        for rule in table_rules.get("temporal_logic", []):
            col = rule["column"]
            if rule.get("cannot_be_future"):
                self.rules.append(
                    (f"{col}_not_future", F.col(col) <= F.current_timestamp()))

        # 5. Temporal Order Check (updated_at >= created_at)
        for rule in table_rules.get("temporal_order", []):
            before = rule["before_column"]
            after = rule["after_column"]
            self.rules.append((f"{after}_after_{before}",
                              F.col(after) >= F.col(before)))

        # 6. Regex Check
        for rule in table_rules.get("regex_check", []):
            col = rule["column"]
            pattern = rule["pattern"]
            self.rules.append((f"{col}_regex", F.col(col).rlike(pattern)))

        # 7. Conditional Rule
        for rule in table_rules.get("conditional_rule", []):
            cond = rule["condition"]
            assertion = rule["assertion"]
            # Logic: NOT (condition) OR assertion
            expr = F.expr(f"NOT ({cond}) OR ({assertion})")
            self.rules.append((f"cond_{cond}", expr))

    def validate(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Splits DataFrame into valid rows and quarantined rows.
        """
        # Collect failed rule names into an array
        failed_conditions = [
            F.when(~condition, F.lit(rule_name))
            for rule_name, condition in self.rules
        ]

        annotated_df = df.withColumn(
            "validation_errors",
            F.array_compact(F.array(*failed_conditions))
        )

        valid_df = annotated_df.filter(
            F.size("validation_errors") == 0).drop("validation_errors")
        quarantine_df = annotated_df.filter(F.size("validation_errors") > 0)

        return valid_df, quarantine_df
