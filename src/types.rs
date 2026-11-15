//! HiveServer2's results are columns already (`TRowSet` from protocol 6 on): each one becomes
//! the Arrow array of its SpelaionDB logical type (the plugin converts, never the core).
//!
//! - `BOOLEAN` is `Bool`; `TINYINT` to `BIGINT` are `Int`; `FLOAT` and `DOUBLE` are `Float`;
//! - `DECIMAL` is `Decimal`, as the server writes it;
//! - `STRING`, `VARCHAR` and `CHAR` are `Text`; `BINARY` is `Bytes`;
//! - `DATE` is `Date` and `TIMESTAMP` is `Timestamp` (microseconds: finer digits are cut), read
//!   from the text the server sends for them;
//! - `ARRAY` is `Array` and the intervals are `Interval`, as the server writes them;
//! - everything else (`MAP`, `STRUCT`, `UNIONTYPE`, `TIMESTAMP WITH LOCAL TIME ZONE`, a type
//!   this file does not know) is `Native`, as the server writes it.
//!
//! The native type is written as Hive writes it in `DESCRIBE`: `decimal(10,2)`, `varchar(20)`,
//! `array<map<string,int>>`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{Field, Schema};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

use crate::tcliservice::{
    TColumn, TColumnDesc, TRowSet, TTableSchema, TTypeDesc, TTypeEntry, TTypeId,
    TTypeQualifierValue,
};

/// The logical type of a column whose (first) type is `id`.
pub fn logical(id: TTypeId) -> LogicalType {
    match id {
        TTypeId::BOOLEAN_TYPE => LogicalType::Bool,
        TTypeId::TINYINT_TYPE
        | TTypeId::SMALLINT_TYPE
        | TTypeId::INT_TYPE
        | TTypeId::BIGINT_TYPE => LogicalType::Int,
        TTypeId::FLOAT_TYPE | TTypeId::DOUBLE_TYPE => LogicalType::Float,
        TTypeId::DECIMAL_TYPE => LogicalType::Decimal,
        TTypeId::STRING_TYPE | TTypeId::VARCHAR_TYPE | TTypeId::CHAR_TYPE => LogicalType::Text,
        TTypeId::BINARY_TYPE => LogicalType::Bytes,
        TTypeId::DATE_TYPE => LogicalType::Date,
        TTypeId::TIMESTAMP_TYPE => LogicalType::Timestamp,
        TTypeId::ARRAY_TYPE => LogicalType::Array,
        TTypeId::INTERVAL_YEAR_MONTH_TYPE | TTypeId::INTERVAL_DAY_TIME_TYPE => {
            LogicalType::Interval
        }
        _ => LogicalType::Native,
    }
}

/// Hive's name of a type id.
fn type_name(id: TTypeId) -> &'static str {
    match id {
        TTypeId::BOOLEAN_TYPE => "boolean",
        TTypeId::TINYINT_TYPE => "tinyint",
        TTypeId::SMALLINT_TYPE => "smallint",
        TTypeId::INT_TYPE => "int",
        TTypeId::BIGINT_TYPE => "bigint",
        TTypeId::FLOAT_TYPE => "float",
        TTypeId::DOUBLE_TYPE => "double",
        TTypeId::STRING_TYPE => "string",
        TTypeId::TIMESTAMP_TYPE => "timestamp",
        TTypeId::BINARY_TYPE => "binary",
        TTypeId::ARRAY_TYPE => "array",
        TTypeId::MAP_TYPE => "map",
        TTypeId::STRUCT_TYPE => "struct",
        TTypeId::UNION_TYPE => "uniontype",
        TTypeId::USER_DEFINED_TYPE => "user_defined",
        TTypeId::DECIMAL_TYPE => "decimal",
        TTypeId::NULL_TYPE => "void",
        TTypeId::DATE_TYPE => "date",
        TTypeId::VARCHAR_TYPE => "varchar",
        TTypeId::CHAR_TYPE => "char",
        TTypeId::INTERVAL_YEAR_MONTH_TYPE => "interval_year_month",
        TTypeId::INTERVAL_DAY_TIME_TYPE => "interval_day_time",
        TTypeId::TIMESTAMPLOCALTZ_TYPE => "timestamp with local time zone",
        _ => "unknown",
    }
}

/// The type id a column's description starts with.
fn first_id(desc: &TTypeDesc) -> TTypeId {
    match desc.types.first() {
        Some(TTypeEntry::PrimitiveEntry(entry)) => entry.type_,
        Some(TTypeEntry::ArrayEntry(_)) => TTypeId::ARRAY_TYPE,
        Some(TTypeEntry::MapEntry(_)) => TTypeId::MAP_TYPE,
        Some(TTypeEntry::StructEntry(_)) => TTypeId::STRUCT_TYPE,
        Some(TTypeEntry::UnionEntry(_)) => TTypeId::UNION_TYPE,
        Some(TTypeEntry::UserDefinedTypeEntry(_)) => TTypeId::USER_DEFINED_TYPE,
        None => TTypeId::NULL_TYPE,
    }
}

/// The name of entry `index` of a description, nested types written in full. Entries point to
/// each other by index; `depth` stops a description that points in circles.
fn entry_name(desc: &TTypeDesc, index: usize, depth: usize) -> String {
    if depth > 32 {
        return "...".into();
    }
    let at = |ptr: i32| -> String {
        usize::try_from(ptr)
            .ok()
            .filter(|&i| i < desc.types.len())
            .map(|i| entry_name(desc, i, depth + 1))
            .unwrap_or_else(|| "unknown".into())
    };
    match desc.types.get(index) {
        Some(TTypeEntry::PrimitiveEntry(entry)) => {
            let name = type_name(entry.type_);
            let qualifier = |key: &str| {
                entry
                    .type_qualifiers
                    .as_ref()
                    .and_then(|q| q.qualifiers.get(key))
                    .map(|value| match value {
                        TTypeQualifierValue::I32Value(n) => n.to_string(),
                        TTypeQualifierValue::StringValue(s) => s.clone(),
                    })
            };
            match entry.type_ {
                TTypeId::DECIMAL_TYPE => match (qualifier("precision"), qualifier("scale")) {
                    (Some(p), Some(s)) => format!("{name}({p},{s})"),
                    _ => name.to_string(),
                },
                TTypeId::VARCHAR_TYPE | TTypeId::CHAR_TYPE => {
                    match qualifier("characterMaximumLength") {
                        Some(n) => format!("{name}({n})"),
                        None => name.to_string(),
                    }
                }
                _ => name.to_string(),
            }
        }
        Some(TTypeEntry::ArrayEntry(entry)) => format!("array<{}>", at(entry.object_type_ptr)),
        Some(TTypeEntry::MapEntry(entry)) => format!(
            "map<{},{}>",
            at(entry.key_type_ptr),
            at(entry.value_type_ptr)
        ),
        Some(TTypeEntry::StructEntry(entry)) => format!(
            "struct<{}>",
            entry
                .name_to_type_ptr
                .iter()
                .map(|(name, ptr)| format!("{name}:{}", at(*ptr)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Some(TTypeEntry::UnionEntry(entry)) => format!(
            "uniontype<{}>",
            entry
                .name_to_type_ptr
                .values()
                .map(|ptr| at(*ptr))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Some(TTypeEntry::UserDefinedTypeEntry(entry)) => entry.type_class_name.clone(),
        None => "unknown".into(),
    }
}

/// The native type of a column, as Hive writes it.
pub fn native_type(desc: &TTypeDesc) -> String {
    entry_name(desc, 0, 0)
}

fn column_meta(column: &TColumnDesc) -> ColumnMeta {
    ColumnMeta {
        name: column.column_name.clone(),
        logical: logical(first_id(&column.type_desc)),
        native_type: native_type(&column.type_desc),
        native_id: None,
        origin: None,
    }
}

/// SpelaionDB's schema for a result HiveServer2 described as `schema`.
pub fn result_schema(schema: &TTableSchema) -> Arc<ResultSchema> {
    Arc::new(ResultSchema::new(
        schema.columns.iter().map(column_meta).collect(),
    ))
}

/// Whether value `i` of a column is null: its bit in `nulls`, lowest first. The bitmap may stop
/// before the last values, which are then not null.
fn is_null(nulls: &[u8], i: usize) -> bool {
    nulls
        .get(i / 8)
        .is_some_and(|byte| byte >> (i % 8) & 1 == 1)
}

/// How many rows a set of columns holds.
pub fn row_count(rows: &TRowSet) -> usize {
    rows.columns
        .as_ref()
        .and_then(|columns| columns.first())
        .map(column_len)
        .unwrap_or(0)
}

fn column_len(column: &TColumn) -> usize {
    match column {
        TColumn::BoolVal(c) => c.values.len(),
        TColumn::ByteVal(c) => c.values.len(),
        TColumn::I16Val(c) => c.values.len(),
        TColumn::I32Val(c) => c.values.len(),
        TColumn::I64Val(c) => c.values.len(),
        TColumn::DoubleVal(c) => c.values.len(),
        TColumn::StringVal(c) => c.values.len(),
        TColumn::BinaryVal(c) => c.values.len(),
    }
}

/// Value `i` of a column as text, or `None` when it is null.
fn text_at(column: &TColumn, i: usize) -> Option<String> {
    match column {
        TColumn::BoolVal(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].to_string()),
        TColumn::ByteVal(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].to_string()),
        TColumn::I16Val(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].to_string()),
        TColumn::I32Val(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].to_string()),
        TColumn::I64Val(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].to_string()),
        TColumn::DoubleVal(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].0.to_string()),
        TColumn::StringVal(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].clone()),
        TColumn::BinaryVal(c) => {
            (!is_null(&c.nulls, i)).then(|| String::from_utf8_lossy(&c.values[i]).into_owned())
        }
    }
}

fn int_at(column: &TColumn, i: usize) -> Result<Option<i64>> {
    Ok(match column {
        TColumn::ByteVal(c) => (!is_null(&c.nulls, i)).then(|| i64::from(c.values[i])),
        TColumn::I16Val(c) => (!is_null(&c.nulls, i)).then(|| i64::from(c.values[i])),
        TColumn::I32Val(c) => (!is_null(&c.nulls, i)).then(|| i64::from(c.values[i])),
        TColumn::I64Val(c) => (!is_null(&c.nulls, i)).then_some(c.values[i]),
        other => match text_at(other, i) {
            None => None,
            Some(text) => Some(
                text.trim()
                    .parse()
                    .map_err(|_| unreadable("an integer", &text))?,
            ),
        },
    })
}

fn float_at(column: &TColumn, i: usize) -> Result<Option<f64>> {
    Ok(match column {
        TColumn::DoubleVal(c) => (!is_null(&c.nulls, i)).then(|| c.values[i].0),
        other => match text_at(other, i) {
            None => None,
            Some(text) => Some(match text.trim() {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                t => t.parse().map_err(|_| unreadable("a number", &text))?,
            }),
        },
    })
}

fn unreadable(what: &str, text: &str) -> DriverError {
    let shown: String = text.chars().take(64).collect();
    DriverError::Other(format!(
        "the server sent {shown:?} where {what} was expected"
    ))
}

/// Days from 1970-01-01 to a date of the proleptic Gregorian calendar (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// `YYYY-MM-DD`, as days since 1970-01-01.
pub fn parse_date(text: &str) -> Option<i32> {
    let text = text.trim();
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let mut parts = text.splitn(3, '-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: i64 = parts.next()?.parse().ok()?;
    let day: i64 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = if negative { -year } else { year };
    i32::try_from(days_from_civil(year, month, day)).ok()
}

/// `YYYY-MM-DD HH:MM:SS[.fffffffff]`, as microseconds since 1970-01-01 (digits past the sixth
/// are cut, as SpelaionDB keeps microseconds).
pub fn parse_timestamp(text: &str) -> Option<i64> {
    let text = text.trim();
    let (date, time) = text.split_once([' ', 'T']).unwrap_or((text, "00:00:00"));
    let days = i64::from(parse_date(date)?);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = clock.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next().unwrap_or("0").parse().ok()?;
    let seconds: i64 = parts.next().unwrap_or("0").parse().ok()?;
    if parts.next().is_some() || hours > 23 || minutes > 59 || seconds > 60 {
        return None;
    }
    if !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut micros: i64 = 0;
    for position in 0..6 {
        let digit = fraction
            .as_bytes()
            .get(position)
            .map_or(0, |b| i64::from(b - b'0'));
        micros = micros * 10 + digit;
    }
    Some(((days * 24 + hours) * 60 + minutes) * 60_000_000 + seconds * 1_000_000 + micros)
}

/// One column of a fetch, as the Arrow array of its logical type.
fn convert_column(column: &TColumn, logical: LogicalType, rows: usize) -> Result<ArrayRef> {
    if column_len(column) < rows {
        return Err(DriverError::Other(
            "the server sent columns of different lengths".into(),
        ));
    }
    Ok(match logical {
        LogicalType::Bool => {
            let mut builder = BooleanBuilder::with_capacity(rows);
            for i in 0..rows {
                match column {
                    TColumn::BoolVal(c) => {
                        builder.append_option((!is_null(&c.nulls, i)).then(|| c.values[i]))
                    }
                    other => builder.append_option(match text_at(other, i) {
                        None => None,
                        Some(text) => Some(match text.trim() {
                            "true" | "TRUE" | "1" => true,
                            "false" | "FALSE" | "0" => false,
                            _ => return Err(unreadable("a boolean", &text)),
                        }),
                    }),
                }
            }
            Arc::new(builder.finish())
        }
        LogicalType::Int => {
            let mut builder = Int64Builder::with_capacity(rows);
            for i in 0..rows {
                builder.append_option(int_at(column, i)?);
            }
            Arc::new(builder.finish())
        }
        LogicalType::Float => {
            let mut builder = Float64Builder::with_capacity(rows);
            for i in 0..rows {
                builder.append_option(float_at(column, i)?);
            }
            Arc::new(builder.finish())
        }
        LogicalType::Bytes => {
            let mut builder = BinaryBuilder::with_capacity(rows, 0);
            for i in 0..rows {
                match column {
                    TColumn::BinaryVal(c) => builder
                        .append_option((!is_null(&c.nulls, i)).then(|| c.values[i].as_slice())),
                    other => builder.append_option(text_at(other, i)),
                }
            }
            Arc::new(builder.finish())
        }
        LogicalType::Date => {
            let mut builder = Date32Builder::with_capacity(rows);
            for i in 0..rows {
                builder.append_option(match text_at(column, i) {
                    None => None,
                    Some(text) => {
                        Some(parse_date(&text).ok_or_else(|| unreadable("a date", &text))?)
                    }
                });
            }
            Arc::new(builder.finish())
        }
        LogicalType::Timestamp => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows);
            for i in 0..rows {
                builder.append_option(match text_at(column, i) {
                    None => None,
                    Some(text) => Some(
                        parse_timestamp(&text).ok_or_else(|| unreadable("a timestamp", &text))?,
                    ),
                });
            }
            Arc::new(builder.finish())
        }
        LogicalType::Decimal
        | LogicalType::Text
        | LogicalType::Array
        | LogicalType::Interval
        | LogicalType::Json
        | LogicalType::Uuid
        | LogicalType::Native
        | LogicalType::Time
        | LogicalType::TimestampTz => {
            let mut builder = StringBuilder::with_capacity(rows, rows * 8);
            for i in 0..rows {
                builder.append_option(text_at(column, i));
            }
            Arc::new(builder.finish())
        }
    })
}

/// A fetch's columns as one batch of SpelaionDB's schema.
pub fn convert(rows: &TRowSet, schema: &ResultSchema) -> Result<RecordBatch> {
    let count = row_count(rows);
    let columns = rows.columns.as_deref().unwrap_or(&[]);
    if columns.len() != schema.columns.len() {
        return Err(DriverError::Other(format!(
            "the server sent {} columns for a result of {}",
            columns.len(),
            schema.columns.len()
        )));
    }
    let arrays = columns
        .iter()
        .zip(&schema.columns)
        .map(|(column, meta)| convert_column(column, meta.logical, count))
        .collect::<Result<Vec<_>>>()?;
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|meta| Field::new(&meta.name, meta.logical.arrow_type(), true))
        .collect();
    let arrow_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| DriverError::Other(format!("the result cannot be built: {e}")))
}

/// A fetch's columns as text, row by row: what the catalog calls read.
pub fn text_rows(rows: &TRowSet) -> Vec<Vec<Option<String>>> {
    let count = row_count(rows);
    let columns = rows.columns.as_deref().unwrap_or(&[]);
    (0..count)
        .map(|i| {
            columns
                .iter()
                .map(|c| {
                    if i < column_len(c) {
                        text_at(c, i)
                    } else {
                        None
                    }
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_and_timestamps_are_read_from_hive_text() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2000-03-01"), Some(11_017));
        assert_eq!(parse_date("0001-01-01"), Some(-719_162));
        assert_eq!(parse_date("9999-12-31"), Some(2_932_896));
        assert_eq!(parse_date("2020-13-01"), None);
        assert_eq!(parse_date("nonsense"), None);
        assert_eq!(parse_timestamp("1970-01-01 00:00:01"), Some(1_000_000));
        assert_eq!(
            parse_timestamp("2026-11-15 14:36:04.494"),
            Some(1_794_753_364_494_000)
        );
        // Nanoseconds lose their last three digits.
        assert_eq!(
            parse_timestamp("1969-12-31 23:59:59.123456789"),
            Some(-876_544)
        );
        assert_eq!(parse_timestamp("2026-11-15 25:00:00"), None);
        assert_eq!(parse_timestamp("2026-11-15 10:00:00.1x"), None);
    }

    #[test]
    fn a_short_null_bitmap_leaves_the_last_values_set() {
        assert!(is_null(&[0b0000_0101], 0));
        assert!(!is_null(&[0b0000_0101], 1));
        assert!(is_null(&[0b0000_0101], 2));
        assert!(!is_null(&[0b0000_0101], 9));
        assert!(!is_null(&[], 0));
    }
}
