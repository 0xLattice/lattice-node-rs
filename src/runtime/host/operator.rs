use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
impl Host {
    pub(super) fn operator(
        &mut self,
        route: &str,
        body: &Value,
    ) -> anyhow::Result<Option<(u16, Value)>> {
        match route {
            // The executable currently hosts only Nexus. This endpoint lists
            // child levels; consumers add Nexus separately.
            "/hosted-chains" => Ok(Some((
                200,
                json!({"chains":self.levels.ordered().iter().map(|p|p.join("/")).collect::<Vec<_>>()}),
            ))),
            "/pins" => Ok(Some((200, json!({"pins":self.storage.operator_pins()?})))),
            "POST /pins" => {
                let result = (|| -> Result<String, String> {
                    let cid = body
                        .get("cid")
                        .and_then(Value::as_str)
                        .ok_or("invalidCID")?;
                    lattice_cashew::canonical_cid(cid).map_err(|_| "invalidCID")?;
                    let entries = match body.get("entries").filter(|v| !v.is_null()) {
                        Some(value) => {
                            let entries: BTreeMap<String, String> =
                                serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
                            Some(
                                entries
                                    .into_iter()
                                    .map(|(cid, data)| {
                                        STANDARD
                                            .decode(data)
                                            .map(|data| (cid, data))
                                            .map_err(|e| e.to_string())
                                    })
                                    .collect::<Result<BTreeMap<_, _>, _>>()?,
                            )
                        }
                        None => None,
                    };
                    self.storage
                        .pin_volume(cid, entries)
                        .map_err(|e| e.to_string())?;
                    Ok(cid.into())
                })();
                Ok(Some(match result {
                    Ok(_) => {
                        self.discovery.changed();
                        (200, json!({"pins":self.storage.operator_pins()?}))
                    }
                    Err(e) => error(e),
                }))
            }
            route if route.starts_with("DELETE /pins/") => {
                let cid = route.trim_start_matches("DELETE /pins/");
                if lattice_cashew::canonical_cid(cid).is_err() {
                    return Ok(Some(error("invalidCID")));
                }
                self.storage.unpin_volume(cid)?;
                self.discovery.changed();
                Ok(Some((200, json!({"pins":self.storage.operator_pins()?}))))
            }
            _ => Ok(None),
        }
    }
}

pub(super) fn archive(volume: lattice_cashew::SerializedVolume) -> Option<Vec<u8>> {
    if volume.entries.is_empty()
        || volume.entries.len() > u16::MAX as usize
        || !volume.entries.contains_key(&volume.root)
        || volume.validate().is_err()
    {
        return None;
    }
    let mut size = 2usize;
    for (cid, bytes) in &volume.entries {
        if cid.is_empty() || cid.len() > 8192 || !cid.is_ascii() || bytes.len() > u32::MAX as usize
        {
            return None;
        }
        size = size.checked_add(6 + cid.len())?.checked_add(bytes.len())?;
        if size > 64 * 1024 * 1024 {
            return None;
        }
    }
    let mut archive = Vec::with_capacity(size);
    archive.extend_from_slice(&(volume.entries.len() as u16).to_be_bytes());
    for (cid, bytes) in volume.entries {
        archive.extend_from_slice(&(cid.len() as u16).to_be_bytes());
        archive.extend_from_slice(cid.as_bytes());
        archive.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        archive.extend_from_slice(&bytes);
    }
    Some(archive)
}
