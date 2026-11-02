//! Foundation JSONDecoder compatibility at the wire boundary: UTF detection and
//! first-occurrence object keys. Canonical comparison still uses original bytes.
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::borrow::Cow;
struct FirstValue(Value);
impl<'de> Deserialize<'de> for FirstValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = FirstValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("JSON")
            }
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(FirstValue(Value::Bool(v)))
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(FirstValue(Value::Null))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(FirstValue(Value::String(v.into())))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(FirstValue(Value::Number(v.into())))
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(FirstValue(Value::Number(v.into())))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(FirstValue(Value::Number(
                    Number::from_f64(v).ok_or_else(|| E::custom("nonfinite"))?,
                )))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(FirstValue(v)) = seq.next_element()? {
                    out.push(v)
                }
                Ok(FirstValue(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = Map::new();
                while let Some((k, FirstValue(v))) = map.next_entry::<String, FirstValue>()? {
                    out.entry(k).or_insert(v);
                }
                Ok(FirstValue(Value::Object(out)))
            }
        }
        d.deserialize_any(V)
    }
}
fn utf8(bytes: &[u8]) -> Result<Cow<'_, [u8]>, ()> {
    let (width, little, skip) = if bytes.starts_with(&[0, 0, 0xfe, 0xff]) {
        (4, false, 4)
    } else if bytes.starts_with(&[0xff, 0xfe, 0, 0]) {
        (4, true, 4)
    } else if bytes.starts_with(&[0xfe, 0xff]) {
        (2, false, 2)
    } else if bytes.starts_with(&[0xff, 0xfe]) {
        (2, true, 2)
    } else if bytes.len() >= 4 && bytes[..3] == [0, 0, 0] {
        (4, false, 0)
    } else if bytes.len() >= 4 && bytes[1..4] == [0, 0, 0] {
        (4, true, 0)
    } else if bytes.len() >= 2 && bytes[0] == 0 {
        (2, false, 0)
    } else if bytes.len() >= 2 && bytes[1] == 0 {
        (2, true, 0)
    } else {
        return Ok(Cow::Borrowed(
            bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes),
        ));
    };
    let bytes = &bytes[skip..];
    if !bytes.len().is_multiple_of(width) {
        return Err(());
    }
    let text = if width == 2 {
        let words: Vec<_> = bytes
            .chunks_exact(2)
            .map(|b| {
                if little {
                    u16::from_le_bytes([b[0], b[1]])
                } else {
                    u16::from_be_bytes([b[0], b[1]])
                }
            })
            .collect();
        String::from_utf16(&words).map_err(|_| ())?
    } else {
        bytes
            .chunks_exact(4)
            .map(|b| {
                let raw: [u8; 4] = b.try_into().unwrap();
                char::from_u32(if little {
                    u32::from_le_bytes(raw)
                } else {
                    u32::from_be_bytes(raw)
                })
                .ok_or(())
            })
            .collect::<Result<String, _>>()?
    };
    Ok(Cow::Owned(text.into_bytes()))
}
pub(super) fn parse(bytes: &[u8]) -> Result<Value, ()> {
    let bytes = utf8(bytes)?;
    serde_json::from_slice::<FirstValue>(&bytes)
        .map(|v| v.0)
        .map_err(|_| ())
}
